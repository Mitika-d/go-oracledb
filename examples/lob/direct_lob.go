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

// runDirectLOBTest demonstrates the connection-bound DirectLOB API in one
// test: temporary LOB creation and mutation, followed by promotion of
// persistent query LOBs and reads after the producing Rows have closed.
func runDirectLOBTest(ctx context.Context, db *sql.DB) error {
	// Step 1: Reserve one dedicated connection. A DirectLOB remains tied to the
	// *sql.Conn used to create or open it.
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("open dedicated LOB connection: %w", err)
	}
	defer conn.Close()

	// Step 2: Create and modify temporary BLOB and CLOB values.
	if err := runTemporaryDirectLOBs(ctx, conn); err != nil {
		return err
	}
	// Step 3: Promote persistent query LOBs and use them after Rows closes.
	if err := runPersistentDirectLOBs(ctx, conn); err != nil {
		return err
	}

	fmt.Println("direct LOB API: temporary and persistent usage: ok")
	return nil
}

// runTemporaryDirectLOBs tests temporary BLOB and CLOB handles.
// It covers writing, metadata, open state, trimming, closing, and
// freeing temporary LOB storage.
func runTemporaryDirectLOBs(ctx context.Context, conn *sql.Conn) error {
	for _, test := range []struct {
		name    string
		kind    lob.Kind
		payload []byte
	}{
		{name: "BLOB", kind: lob.BLOB, payload: []byte("temporary BLOB payload")},
		{name: "CLOB", kind: lob.CLOB, payload: []byte("temporary CLOB payload")},
	} {
		// Step 1: Create a temporary LOB of the requested kind.
		value, err := lob.CreateTemporary(ctx, conn, test.kind)
		if err != nil {
			return fmt.Errorf("create temporary %s: %w", test.name, err)
		}
		// Keep a fallback cleanup until the explicit Free below succeeds.
		freed := false
		defer func() {
			if !freed {
				_ = value.Free(ctx)
			}
		}()

		// Step 2: Use Kind and IsTemporary to verify the new handle's local
		// metadata before using the handle.
		if value.Kind() != test.kind || !value.IsTemporary() {
			return fmt.Errorf("temporary %s metadata is incorrect", test.name)
		}

		// Step 3: Write the payload. Write is the context.Background wrapper;
		// WriteContext is used for the text LOB to demonstrate the cancellable
		// form. Both methods support the BLOB and CLOB handles used here.
		var written int
		if test.kind == lob.BLOB {
			written, err = value.Write(test.payload)
		} else {
			written, err = value.WriteContext(ctx, test.payload)
		}
		if err != nil || written != len(test.payload) {
			return fmt.Errorf("write temporary %s = (%d, %v), want (%d, nil)", test.name, written, err, len(test.payload))
		}

		// Step 4: Size reports the current logical LOB length. ChunkSize reports
		// Oracle's storage chunk size; neither operation changes the cursor.
		wantSize := int64(len(test.payload))
		if size, err := value.Size(ctx); err != nil || size != wantSize {
			return fmt.Errorf("temporary %s size = (%d, %v), want (%d, nil)", test.name, size, err, wantSize)
		}
		if chunkSize, err := value.ChunkSize(ctx); err != nil || chunkSize <= 0 {
			return fmt.Errorf("temporary %s chunk size = (%d, %v), want positive size", test.name, chunkSize, err)
		}

		// Step 5: Open the temporary LOB for read/write access and verify its open
		// state with IsOpen.
		if opened, err := value.Open(ctx, lob.ReadWrite); err != nil || !opened {
			return fmt.Errorf("open temporary %s = (%t, %v), want (true, nil)", test.name, opened, err)
		}
		if open, err := value.IsOpen(ctx); err != nil || !open {
			return fmt.Errorf("temporary %s IsOpen = (%t, %v), want (true, nil)", test.name, open, err)
		}

		// Step 6: End the explicit open state. CloseServer is separate from Close:
		// it leaves the Go handle usable for later operations.
		if err := value.CloseServer(ctx); err != nil {
			return fmt.Errorf("close temporary %s on server: %w", test.name, err)
		}
		if open, err := value.IsOpen(ctx); err != nil || open {
			return fmt.Errorf("temporary %s IsOpen after CloseServer = (%t, %v), want (false, nil)", test.name, open, err)
		}

		// Step 7: Trim one logical unit and confirm the new logical length.
		if err := value.Trim(ctx, wantSize-1); err != nil {
			return fmt.Errorf("trim temporary %s: %w", test.name, err)
		}
		if size, err := value.Size(ctx); err != nil || size != wantSize-1 {
			return fmt.Errorf("temporary %s size after Trim = (%d, %v), want (%d, nil)", test.name, size, err, wantSize-1)
		}

		// Step 8: Close the local handle, then Free the Oracle temporary storage.
		// Free is the required cleanup operation before the owning connection closes.
		if err := value.Close(); err != nil {
			return fmt.Errorf("close temporary %s locally: %w", test.name, err)
		}
		if err := value.Free(ctx); err != nil {
			return fmt.Errorf("free temporary %s: %w", test.name, err)
		}
		freed = true
	}
	return nil
}

// runPersistentDirectLOBs tests persistent BLOB and CLOB handles. It queries
// one row, promotes both unread query LOBs, closes the source Rows,
// reads the promoted handles, and cleans them up.
func runPersistentDirectLOBs(ctx context.Context, conn *sql.Conn) error {
	// Step 1: Create a persistent table and insert one BLOB and one CLOB.
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

	// Step 2: Query the persistent values and keep Rows open until they are
	// transferred to DirectLOB handles.
	query := fmt.Sprintf("SELECT blob_value, clob_value FROM %s WHERE id = :1", table)
	rows, err := conn.QueryContext(ctx, query, int64(1))
	if err != nil {
		return fmt.Errorf("query persistent LOBs: %w", err)
	}
	if !rows.Next() {
		_ = rows.Close()
		return fmt.Errorf("query persistent LOBs returned no row: %v", rows.Err())
	}
	var queryBlob, queryClob lob.LOB
	if err := rows.Scan(&queryBlob, &queryClob); err != nil {
		_ = rows.Close()
		return fmt.Errorf("scan persistent LOBs: %w", err)
	}

	// Step 3: OpenPersistent transfers each unread query value to a DirectLOB on
	// the same connection. All transfers must happen before Rows is closed.
	blobValue, err := lob.OpenPersistent(ctx, conn, &queryBlob)
	if err != nil {
		_ = rows.Close()
		return fmt.Errorf("promote persistent BLOB: %w", err)
	}
	clobValue, err := lob.OpenPersistent(ctx, conn, &queryClob)
	if err != nil {
		_ = rows.Close()
		_ = blobValue.Free(ctx)
		return fmt.Errorf("promote persistent CLOB: %w", err)
	}
	// Rows.Close releases the query result. The DirectLOB handles remain usable
	// because they are now independent persistent handles on conn.
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close source LOB rows: %w", err)
	}
	directValues := []*lob.DirectLOB{blobValue, clobValue}
	defer func() {
		for _, value := range directValues {
			_ = value.Free(ctx)
		}
	}()

	values := []struct {
		name      string
		kind      lob.Kind
		value     *lob.DirectLOB
		wantBytes []byte
		wantText  string
		readTo    bool
	}{
		{name: "BLOB", kind: lob.BLOB, value: blobValue, wantBytes: blobPayload},
		{name: "CLOB", kind: lob.CLOB, value: clobValue, wantText: clobPayload, readTo: true},
	}

	// Step 4: Check each promoted handle's kind, lifetime, logical size, and
	// actual content while the owning connection remains open.
	for _, test := range values {
		// Kind and IsTemporary confirm that the query value became the expected
		// persistent LOB family rather than a temporary handle.
		if test.value.Kind() == lob.Unknown || test.value.IsTemporary() {
			return fmt.Errorf("persistent %s metadata is incorrect", test.name)
		}
		wantSize := int64(len(test.wantBytes))
		if test.kind != lob.BLOB {
			wantSize = logicalTextSize(test.wantText)
		}
		// Size uses bytes for BLOBs and UTF-16 code units for CLOBs.
		if size, err := test.value.Size(ctx); err != nil || size != wantSize {
			return fmt.Errorf("persistent %s size = (%d, %v), want (%d, nil)", test.name, size, err, wantSize)
		}

		var got []byte
		if test.readTo {
			// WriteTo is a read convenience API: it reads the remaining CLOB into
			// this in-memory writer and returns the number of bytes copied.
			var output bytes.Buffer
			if _, err := test.value.WriteTo(&output); err != nil {
				return fmt.Errorf("read persistent %s with WriteTo: %w", test.name, err)
			}
			got = output.Bytes()
		} else {
			var err error
			got, err = readDirectLOB(ctx, test.value)
			if err != nil {
				return fmt.Errorf("read persistent %s: %w", test.name, err)
			}
		}
		if test.kind == lob.BLOB {
			if !bytes.Equal(got, test.wantBytes) {
				return fmt.Errorf("persistent %s value mismatch: got %d bytes, want %d", test.name, len(got), len(test.wantBytes))
			}
		} else if string(got) != test.wantText {
			return fmt.Errorf("persistent %s value mismatch: got %d UTF-8 bytes, want %d", test.name, len(got), len(test.wantText))
		}
	}

	// Step 5: Close and free every promoted handle. Free releases the direct
	// handle; it does not delete the persistent column value.
	for _, value := range directValues {
		if err := value.Close(); err != nil {
			return fmt.Errorf("close persistent LOB: %w", err)
		}
		if err := value.Free(ctx); err != nil {
			return fmt.Errorf("free persistent LOB: %w", err)
		}
	}
	return nil
}

// readDirectLOB reads a DirectLOB to completion with a small buffer.
func readDirectLOB(ctx context.Context, value *lob.DirectLOB) ([]byte, error) {
	var output bytes.Buffer
	buffer := make([]byte, 7)
	firstRead := true
	for {
		var count int
		var err error
		if firstRead {
			// Demonstrate Read, which uses context.Background internally, for the
			// first chunk.
			count, err = value.Read(buffer)
			firstRead = false
		} else {
			// Use ReadContext for the remaining chunks so the caller's deadline and
			// cancellation are honored during the remaining DirectLOB reads.
			count, err = value.ReadContext(ctx, buffer)
		}
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
