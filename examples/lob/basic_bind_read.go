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

	lob "github.com/oracle/go-oracledb/v26/oracle/datatype/lob"
)

// runBasicBindReadExample demonstrates the public insert flow: create
// temporary LOBs, write their contents, bind the typed values, then scan them
// into materialized []byte and string-compatible public LOB values.
func runBasicBindReadExample(ctx context.Context, db *sql.DB) (retErr error) {
	table, err := createTable(ctx, db, "id NUMBER PRIMARY KEY, blob_value BLOB, clob_value CLOB")
	if err != nil {
		return err
	}
	defer cleanupTable(db, table)

	blobPayload := binaryPayload(basicPayloadSize)
	clobPayload := "A small CLOB: café, 中, and 🙂"
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("get connection: %w", err)
	}
	defer conn.Close()
	temporaryBlob, err := createAndWriteTemporaryBlob(ctx, conn, blobPayload)
	if err != nil {
		return err
	}
	defer func() {
		if temporaryBlob.Valid() {
			if err := temporaryBlob.FreeContext(ctx); err != nil {
				retErr = errors.Join(retErr, fmt.Errorf("free temporary BLOB: %w", err))
			}
		}
	}()
	temporaryClob, err := createAndWriteTemporaryClob(ctx, conn, clobPayload)
	if err != nil {
		return err
	}
	defer func() {
		if temporaryClob.Valid() {
			if err := temporaryClob.FreeContext(ctx); err != nil {
				retErr = errors.Join(retErr, fmt.Errorf("free temporary CLOB: %w", err))
			}
		}
	}()
	// Bind the caller-created temporary LOB objects to the INSERT.
	insert := fmt.Sprintf("INSERT INTO %s (id, blob_value, clob_value) VALUES (:1, :2, :3)", table)
	if _, err := conn.ExecContext(ctx, insert, int64(1), temporaryBlob, temporaryClob); err != nil {
		return fmt.Errorf("insert BLOB and CLOB: %w", err)
	}
	if err := temporaryBlob.FreeContext(ctx); err != nil {
		return fmt.Errorf("free temporary BLOB: %w", err)
	}
	if err := temporaryClob.FreeContext(ctx); err != nil {
		return fmt.Errorf("free temporary CLOB: %w", err)
	}

	query := fmt.Sprintf("SELECT blob_value, clob_value FROM %s WHERE id = :1", table)
	var gotBlob lob.Bytes
	var gotClob lob.Text
	if err := conn.QueryRowContext(ctx, query, int64(1)).Scan(&gotBlob, &gotClob); err != nil {
		return fmt.Errorf("scan BLOB and CLOB: %w", err)
	}
	if !bytes.Equal(gotBlob, blobPayload) {
		return fmt.Errorf("BLOB mismatch: got %d bytes, want %d", len(gotBlob), len(blobPayload))
	}
	if string(gotClob) != clobPayload {
		return fmt.Errorf("CLOB mismatch: got %q, want %q", gotClob, clobPayload)
	}
	fmt.Println("materialized BLOB and CLOB read: ok")
	return nil
}
