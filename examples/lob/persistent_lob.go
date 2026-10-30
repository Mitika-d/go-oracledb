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
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"

	datatypeLOB "github.com/oracle/go-oracledb/v26/oracle/datatype/lob"

	"github.com/oracle/go-oracledb/v26/oracle"
)

// runPersistentLOBExample owns a dedicated connection while persistent
// query locators are scanned, read, and released.
func runPersistentLOBExample(ctx context.Context, db *sql.DB) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("open dedicated connection: %w", err)
	}
	defer conn.Close()
	return runPersistentLOBScenario(ctx, conn)
}

// runPersistentLOBScenario demonstrates persistent BLOB and CLOB handles. It queries
// one row, scans both query LOBs into connection-scoped values, closes the
// source Rows, reads the values, and cleans them up.
func runPersistentLOBScenario(ctx context.Context, conn *sql.Conn) (retErr error) {
	// Create a persistent table and insert one BLOB and one CLOB.
	table, err := createTableConn(ctx, conn, "id NUMBER PRIMARY KEY, blob_value BLOB, clob_value CLOB")
	if err != nil {
		return err
	}
	defer cleanupTableConn(conn, table)

	blobPayload := []byte("persistent BLOB payload")
	clobPayload := "persistent CLOB payload 🙂"
	insert := fmt.Sprintf("INSERT INTO %s (id, blob_value, clob_value) VALUES (:1, :2, :3)", table)
	// BindBlob and BindClob select the matching Oracle LOB bind type.
	if _, err := conn.ExecContext(ctx, insert, int64(1), oracle.BindBlob(blobPayload), oracle.BindClob(clobPayload)); err != nil {
		return fmt.Errorf("insert persistent LOBs: %w", err)
	}

	// Typed zero values are scan targets. The query's row locator adopts the
	// physical connection lease during Scan.
	var blobValue datatypeLOB.Blob
	defer func() {
		if err := blobValue.FreeContext(ctx); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("free persistent BLOB during cleanup: %w", err))
		}
	}()
	var clobValue datatypeLOB.Clob
	defer func() {
		if err := clobValue.FreeContext(ctx); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("free persistent CLOB during cleanup: %w", err))
		}
	}()

	// Query and scan the persistent values; Scan adopts their locators.
	query := fmt.Sprintf("SELECT blob_value, clob_value FROM %s WHERE id = :1", table)
	rows, err := conn.QueryContext(ctx, query, int64(1))
	if err != nil {
		return fmt.Errorf("query persistent LOBs: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		iterationErr := rows.Err()
		_ = rows.Close()
		if iterationErr != nil {
			return fmt.Errorf("iterate persistent LOBs: %w", iterationErr)
		}
		return fmt.Errorf("query persistent LOBs returned no row")
	}
	if err := rows.Scan(&blobValue, &clobValue); err != nil {
		_ = rows.Close()
		return fmt.Errorf("scan persistent LOBs: %w", err)
	}
	if rows.Next() {
		_ = rows.Close()
		return fmt.Errorf("query persistent LOBs returned more than one row")
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("iterate persistent LOBs: %w", err)
	}
	// Rows.Close releases the query result. Because this query used a dedicated
	// *sql.Conn, its physical session stays checked out and the adopted typed
	// values remain usable until conn.Close.
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close source LOB rows: %w", err)
	}
	persistentValues := []exampleLOB{&blobValue, &clobValue}

	values := []struct {
		name      string
		kind      datatypeLOB.Kind
		value     exampleLOB
		wantBytes []byte
		wantText  string
		readTo    bool
	}{
		{name: "BLOB", kind: datatypeLOB.KindBLOB, value: &blobValue, wantBytes: blobPayload},
		{name: "CLOB", kind: datatypeLOB.KindCLOB, value: &clobValue, wantText: clobPayload, readTo: true},
	}

	// Check each typed value's kind, lifetime, logical size, and content while
	// the owning connection remains open.
	for _, test := range values {
		// Kind and IsTemporary confirm that the query value became the expected
		// persistent LOB family rather than a temporary handle.
		if test.value.Kind() != test.kind || !test.value.Valid() || test.value.IsTemporary() {
			return fmt.Errorf("persistent %s metadata is incorrect", test.name)
		}
		wantSize := int64(len(test.wantBytes))
		if test.kind != datatypeLOB.KindBLOB {
			wantSize = logicalTextSize(test.wantText)
		}
		// Size uses bytes for BLOBs and UTF-16 code units for CLOBs.
		if size, err := test.value.SizeContext(ctx); err != nil || size != wantSize {
			return fmt.Errorf("persistent %s size = (%d, %v), want (%d, nil)", test.name, size, err, wantSize)
		}

		var got []byte
		if test.readTo {
			// WriteToContext reads the remaining CLOB into this in-memory writer
			// while honoring the scenario context.
			var output bytes.Buffer
			if _, err := test.value.WriteToContext(ctx, &output); err != nil {
				return fmt.Errorf("read persistent %s with WriteTo: %w", test.name, err)
			}
			got = output.Bytes()
		} else {
			var err error
			got, err = readTypedLOB(ctx, test.value)
			if err != nil {
				return fmt.Errorf("read persistent %s: %w", test.name, err)
			}
		}
		if test.kind == datatypeLOB.KindBLOB {
			if !bytes.Equal(got, test.wantBytes) {
				return fmt.Errorf("persistent %s value mismatch: got %d bytes, want %d", test.name, len(got), len(test.wantBytes))
			}
		} else if string(got) != test.wantText {
			return fmt.Errorf("persistent %s value mismatch: got %d UTF-8 bytes, want %d", test.name, len(got), len(test.wantText))
		}
	}

	// Free releases each local persistent handle without deleting its column.
	for _, value := range persistentValues {
		if err := value.Close(); err != nil {
			return fmt.Errorf("close persistent LOB: %w", err)
		}
		if err := value.FreeContext(ctx); err != nil {
			return fmt.Errorf("free persistent LOB: %w", err)
		}
	}
	return nil
}

// readTypedLOB reads a typed LOB to completion with a small buffer and honors
// the caller's context for every database read.
func readTypedLOB(ctx context.Context, value exampleLOB) ([]byte, error) {
	var output bytes.Buffer
	buffer := make([]byte, 7)
	for {
		count, err := value.ReadContext(ctx, buffer)
		if count != 0 {
			_, _ = output.Write(buffer[:count])
		}
		if err == io.EOF {
			return output.Bytes(), nil
		}
		if err != nil {
			return nil, err
		}
		if count == 0 {
			return nil, io.ErrNoProgress
		}
	}
}
