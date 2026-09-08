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
	"fmt"
	"io"

	"github.com/oracle/go-oracledb/v26/oracle"
	"github.com/oracle/go-oracledb/v26/oracle/lob"
)

// runMultipleLOBTest keeps six independent lob.LOB values from one row alive at
// the same time. Reading each value to EOF verifies that every column can be
// consumed independently.
func runMultipleLOBTest(ctx context.Context, db *sql.DB, prefetchSize int) error {
	// Step 1: Create one table containing two columns from each LOB family.
	table, err := createTable(ctx, db, "id NUMBER PRIMARY KEY, blob_one BLOB, blob_two BLOB, clob_one CLOB, clob_two CLOB, nclob_one NCLOB, nclob_two NCLOB")
	if err != nil {
		return err
	}
	defer cleanupTable(db, table)

	// Step 2: Prepare different values that cross the configured prefetch
	// boundary. Different values make it possible to detect column mix-ups.
	blobOne := binaryPayload(prefetchSize + boundaryPayloadExtra64KiB)
	blobTwo := binaryPayload(prefetchSize + boundaryPayloadExtra64KiB + 17)
	clobOne := textPayload(prefetchSize + boundaryPayloadExtra64KiB)
	clobTwo := "second CLOB: " + textPayload(prefetchSize+boundaryPayloadExtra64KiB)
	nclobOne := "first NCLOB: " + textPayload(prefetchSize+boundaryPayloadExtra64KiB)
	nclobTwo := "second NCLOB: " + textPayload(prefetchSize+boundaryPayloadExtra64KiB+19)

	// Step 3: Insert all six values using the public LOB bind markers.
	insert := fmt.Sprintf("INSERT INTO %s (id, blob_one, blob_two, clob_one, clob_two, nclob_one, nclob_two) VALUES (:1, :2, :3, :4, :5, :6, :7)", table)
	if _, err := db.ExecContext(ctx, insert,
		int64(1),
		oracle.BindBlob(blobOne), oracle.BindBlob(blobTwo),
		oracle.BindClob(clobOne), oracle.BindClob(clobTwo),
		oracle.BindNClob(nclobOne), oracle.BindNClob(nclobTwo),
	); err != nil {
		return fmt.Errorf("insert multiple LOBs: %w", err)
	}

	// Step 4: Query one row and scan every LOB column into its own streaming
	// value. Keep Rows open while the values are being read.
	query := fmt.Sprintf("SELECT blob_one, blob_two, clob_one, clob_two, nclob_one, nclob_two FROM %s WHERE id = :1", table)
	rows, err := db.QueryContext(ctx, query, int64(1))
	if err != nil {
		return fmt.Errorf("query multiple LOBs: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterate multiple LOBs: %w", err)
		}
		return fmt.Errorf("query multiple LOBs returned no row")
	}

	var values [6]lob.LOB
	destinations := make([]any, len(values))
	for index := range values {
		destinations[index] = &values[index]
	}
	if err := rows.Scan(destinations...); err != nil {
		return fmt.Errorf("scan multiple LOBs: %w", err)
	}

	checks := []struct {
		name      string
		value     *lob.LOB
		kind      lob.Kind
		wantBytes []byte
		wantText  string
	}{
		{name: "BLOB one", value: &values[0], kind: lob.BLOB, wantBytes: blobOne},
		{name: "BLOB two", value: &values[1], kind: lob.BLOB, wantBytes: blobTwo},
		{name: "CLOB one", value: &values[2], kind: lob.CLOB, wantText: clobOne},
		{name: "CLOB two", value: &values[3], kind: lob.CLOB, wantText: clobTwo},
		{name: "NCLOB one", value: &values[4], kind: lob.NCLOB, wantText: nclobOne},
		{name: "NCLOB two", value: &values[5], kind: lob.NCLOB, wantText: nclobTwo},
	}

	// Step 5: Read and compare the actual content for each column. BLOBs are
	// compared as bytes; CLOBs and NCLOBs are compared as UTF-8 strings. Reading
	// the values one at a time also verifies that each LOB has an independent
	// cursor and buffer, even though all six came from the same row.
	for _, check := range checks {
		var output bytes.Buffer
		buffer := make([]byte, 32*1024)
		for {
			count, err := check.value.Read(buffer)
			if count > 0 {
				_, _ = output.Write(buffer[:count])
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("read %s: %w", check.name, err)
			}
			if count == 0 {
				return fmt.Errorf("read %s: %w", check.name, io.ErrNoProgress)
			}
		}
		got := output.Bytes()
		if check.kind == lob.BLOB {
			if !bytes.Equal(got, check.wantBytes) {
				return fmt.Errorf("%s value mismatch: got %d bytes, want %d", check.name, len(got), len(check.wantBytes))
			}
			continue
		}
		if string(got) != check.wantText {
			return fmt.Errorf("%s value mismatch: got %d UTF-8 bytes, want %d", check.name, len(got), len(check.wantText))
		}
	}

	// Step 6: Confirm that the query returned exactly one row.
	if rows.Next() {
		return fmt.Errorf("query multiple LOBs returned more than one row")
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate multiple LOBs: %w", err)
	}

	fmt.Println("six independent streaming values (2 BLOB, 2 CLOB, 2 NCLOB): ok")
	return nil
}
