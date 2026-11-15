/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
**
** Subject to the condition set forth below, permission is hereby granted to any
** person obtaining a copy of this software, associated documentation and/or data
** (collectively, the "Software"), free of charge and under any and all
** copyright and patent rights owned or licensable by each licensor hereunder
** covering the Software, to deal in the Software without restriction, including
** without limitation the rights to use, copy, modify, merge, publish,
** distribute, sublicense, and/or sell copies of the Software, and to permit
** persons to whom the Software is furnished to do so, subject to the following
** conditions:
**
** The above copyright notice and this permission notice shall be included in all
** copies or substantial portions of the Software.
**
** THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
** IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
** FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
** AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
** LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
** OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
** SOFTWARE.
 */

package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	datatypeLOB "github.com/oracle/go-oracledb/v26/oracle/datatype/lob"
)

// runTemporaryLOBExample owns a dedicated connection for the temporary
// LOB lifecycle demonstration.
func runTemporaryLOBExample(ctx context.Context, db *sql.DB) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("open dedicated connection: %w", err)
	}
	defer conn.Close()
	return runTemporaryLOBScenario(ctx, conn)
}

// runTemporaryLOBScenario demonstrates temporary BLOB and CLOB handles.
// It covers writing, metadata, open state, trimming, and freeing temporary
// LOB storage.
func runTemporaryLOBScenario(ctx context.Context, conn *sql.Conn) (retErr error) {
	for _, test := range []struct {
		name    string
		kind    datatypeLOB.Kind
		payload []byte
	}{
		{name: "BLOB", kind: datatypeLOB.KindBLOB, payload: []byte("temporary BLOB payload")},
		{name: "CLOB", kind: datatypeLOB.KindCLOB, payload: []byte("temporary CLOB payload")},
	} {
		// Create a typed temporary LOB of the requested kind.
		var value exampleLOB
		var err error
		if test.kind == datatypeLOB.KindBLOB {
			value, err = datatypeLOB.CreateTemporaryBlob(ctx, conn)
		} else {
			value, err = datatypeLOB.CreateTemporaryClob(ctx, conn)
		}
		if err != nil {
			return fmt.Errorf("create temporary %s: %w", test.name, err)
		}
		// Keep a fallback cleanup until the explicit Free below succeeds.
		freed := false
		defer func() {
			if !freed {
				if err := value.FreeContext(ctx); err != nil {
					retErr = errors.Join(retErr, fmt.Errorf("free temporary %s during cleanup: %w", test.name, err))
				}
			}
		}()

		// Check the new handle's declared kind and temporary status.
		if value.Kind() != test.kind || !value.IsTemporary() {
			return fmt.Errorf("temporary %s metadata is incorrect", test.name)
		}

		// Write the payload. Use the context-free convenience method for BLOB and
		// the context-aware string method for CLOB to show both public forms.
		var written int
		if test.kind == datatypeLOB.KindBLOB {
			written, err = value.Write(test.payload)
		} else {
			clob, ok := value.(*datatypeLOB.Clob)
			if !ok {
				return fmt.Errorf("temporary %s has type %T, want *datatypeLOB.Clob", test.name, value)
			}
			written, err = clob.WriteStringContext(ctx, string(test.payload))
		}
		if err != nil || written != len(test.payload) {
			return fmt.Errorf("write temporary %s = (%d, %v), want (%d, nil)", test.name, written, err, len(test.payload))
		}

		// Size reports logical length; ChunkSize reports Oracle's storage chunk
		// size. Neither operation changes the sequential cursor.
		wantSize := int64(len(test.payload))
		if size, err := value.SizeContext(ctx); err != nil || size != wantSize {
			return fmt.Errorf("temporary %s size = (%d, %v), want (%d, nil)", test.name, size, err, wantSize)
		}
		if chunkSize, err := value.ChunkSizeContext(ctx); err != nil || chunkSize <= 0 {
			return fmt.Errorf("temporary %s chunk size = (%d, %v), want positive size", test.name, chunkSize, err)
		}

		// Open the temporary LOB for read/write access and verify the open state.
		if err := value.OpenContext(ctx); err != nil {
			return fmt.Errorf("open temporary %s: %w", test.name, err)
		}
		if open, err := value.IsOpenContext(ctx); err != nil || !open {
			return fmt.Errorf("temporary %s IsOpen = (%t, %v), want (true, nil)", test.name, open, err)
		}

		// CloseServer ends the explicit open state while leaving the Go handle
		// usable. For temporary LOBs, this updates local state without a server call.
		if err := value.CloseServerContext(ctx); err != nil {
			return fmt.Errorf("close temporary %s open state: %w", test.name, err)
		}
		if open, err := value.IsOpenContext(ctx); err != nil || open {
			return fmt.Errorf("temporary %s IsOpen after CloseServer = (%t, %v), want (false, nil)", test.name, open, err)
		}

		// Trim one logical unit and confirm the resulting logical length.
		if err := value.TrimContext(ctx, wantSize-1); err != nil {
			return fmt.Errorf("trim temporary %s: %w", test.name, err)
		}
		if size, err := value.SizeContext(ctx); err != nil || size != wantSize-1 {
			return fmt.Errorf("temporary %s size after Trim = (%d, %v), want (%d, nil)", test.name, size, err, wantSize-1)
		}

		// Free is the terminal cleanup operation for a temporary LOB. It releases
		// local handle state and queues Oracle storage cleanup for a later TTC
		// request. Keep conn open so a later request can send the cleanup; Oracle
		// also reclaims session-duration temporary storage when the session ends.
		if err := value.FreeContext(ctx); err != nil {
			return fmt.Errorf("free temporary %s: %w", test.name, err)
		}
		freed = true
	}
	return nil
}
