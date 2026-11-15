/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
**
** Subject to the condition set forth below, permission is hereby granted to any
** person obtaining a copy of this software, associated documentation and/or data
** (collectively the "Software"), free of charge and under any and all copyright
** rights in the Software, and any and all patent rights owned or freely
** licensable by each licensor hereunder covering either (i) the unmodified
** Software as contributed to or provided by such licensor, or (ii) the Larger
** Works (as defined below), to deal in both
**
** (a) the Software, and
** (b) any piece of software and/or hardware listed in the lrgrwrks.txt file if
** one is included with the Software (each a "Larger Work" to which the Software
** is contributed by such licensors),
**
** without restriction, including without limitation the rights to copy, create
** derivative works of, display, perform, and distribute the Software and make,
** use, sell, offer for sale, import, export, have made, and have sold the
** Software and the Larger Work(s), and to sublicense the foregoing rights on
** either these or other terms.
**
** This license is subject to the following condition:
** The above copyright notice and either this complete permission notice or at
** a minimum a reference to the UPL must be included in all copies or
** substantial portions of the Software.
**
** THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
** IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
** FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
** AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
** LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
** OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
** SOFTWARE.
 */

package oracle

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	lobpkg "github.com/oracle/go-oracledb/v26/oracle/datatype/lob"
)

// TestDriver_Table_Select_CLOB verifies materialized CLOB scans for small,
// large, and SQL NULL values.
func TestDriver_Table_Select_CLOB(t *testing.T) {
	t.Parallel()
	if TestingConfig == nil {
		t.Skip("No configuration available")
	}

	db, err := openTestDBWithConfig(TestingConfig)
	if err != nil {
		t.Fatalf("failed to open test DB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	table := createObjectName("t_select_clob")

	cols := map[string]string{
		"id":  "NUMBER PRIMARY KEY",
		"txt": "CLOB",
	}

	_ = dropTable(ctx, db, table)
	if err := createTable(ctx, db, table, cols); err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	t.Cleanup(func() { _ = dropTable(ctx, db, table) })

	clobSmall := "hello clob"
	clobLarge := strings.Repeat("The quick brown fox jumps over the lazy dog. ", 5000) // ~220KB

	insSQL := fmt.Sprintf("INSERT INTO %s (id, txt) VALUES (:id, :txt)", table)
	if _, err := db.ExecContext(ctx, insSQL, sql.Named("id", int64(1)), sql.Named("txt", clobSmall)); err != nil {
		t.Fatalf("insert small clob failed: %v", err)
	}
	if _, err := db.ExecContext(ctx, insSQL, sql.Named("id", int64(2)), sql.Named("txt", clobLarge)); err != nil {
		t.Fatalf("insert large clob failed: %v", err)
	}
	// NULL case
	insNull := fmt.Sprintf("INSERT INTO %s (id, txt) VALUES (:id, NULL)", table)
	if _, err := db.ExecContext(ctx, insNull, sql.Named("id", int64(3))); err != nil {
		t.Fatalf("insert null clob failed: %v", err)
	}

	// Select back (small)
	var outSmall lobpkg.Text
	selSQL := fmt.Sprintf("SELECT txt FROM %s WHERE id = :id", table)
	if err := db.QueryRowContext(ctx, selSQL, sql.Named("id", int64(1))).Scan(&outSmall); err != nil {
		t.Fatalf("select/scan small clob failed: %v", err)
	}
	if string(outSmall) != clobSmall {
		t.Fatalf("small clob mismatch:\n got: %q\nwant: %q", outSmall, clobSmall)
	}

	// Select back (large) - compare hash to keep failure messages manageable
	var outLarge lobpkg.Text
	if err := db.QueryRowContext(ctx, selSQL, sql.Named("id", int64(2))).Scan(&outLarge); err != nil {
		t.Fatalf("select/scan large clob failed: %v", err)
	}
	if sha256.Sum256([]byte(outLarge)) != sha256.Sum256([]byte(clobLarge)) {
		t.Fatalf("large clob mismatch (sha256 differs):\n got:  %x\nwant: %x",
			sha256.Sum256([]byte(outLarge)),
			sha256.Sum256([]byte(clobLarge)),
		)
	}

	// Select back (NULL) - use sql.NullString to validate NULL handling
	var outNull sql.NullString
	if err := db.QueryRowContext(ctx, selSQL, sql.Named("id", int64(3))).Scan(&outNull); err != nil {
		t.Fatalf("select/scan null clob failed: %v", err)
	}
	if outNull.Valid {
		t.Fatalf("expected NULL clob, got valid string: %q", outNull.String)
	}
}

// TestDriver_Table_Select_BLOB verifies materialized scans after inserting a
// small inline BLOB, a large streamed temporary BLOB, and SQL NULL.
func TestDriver_Table_Select_BLOB(t *testing.T) {
	t.Parallel()
	if TestingConfig == nil {
		t.Skip("No configuration available")
	}

	db, err := openTestDBWithConfig(TestingConfig)
	if err != nil {
		t.Fatalf("failed to open test DB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	table := createObjectName("t_select_blob")

	cols := map[string]string{
		"id":  "NUMBER PRIMARY KEY",
		"bin": "BLOB",
	}

	_ = dropTable(ctx, db, table)
	if err := createTable(ctx, db, table, cols); err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	t.Cleanup(func() { _ = dropTable(ctx, db, table) })

	blobSmall := []byte("hello blob")
	blobLarge := bytes.Repeat([]byte{0x00, 0x01, 0x02, 0x03, 0xFE, 0xFF}, 100_000) // 600KB

	// Case 1: Insert the small BLOB from its ordinary []byte value.
	insSQL := fmt.Sprintf("INSERT INTO %s (id, bin) VALUES (:1, :2)", table)
	if _, err := db.ExecContext(ctx, insSQL, int64(1), blobSmall); err != nil {
		t.Fatalf("insert small BLOB: %v", err)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("get dedicated LOB connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	// Case 2: Stream the large BLOB into a temporary LOB and bind its object.
	temporaryBlob := writeTemporaryBlob(t, ctx, conn, blobLarge)
	if _, err := conn.ExecContext(ctx, insSQL, int64(2), temporaryBlob); err != nil {
		t.Fatalf("insert large BLOB: %v", err)
	}
	if err := temporaryBlob.FreeContext(ctx); err != nil {
		t.Fatalf("free temporary BLOB: %v", err)
	}

	// Case 3: Insert SQL NULL for the materialized-scan validation.
	insNull := fmt.Sprintf("INSERT INTO %s (id, bin) VALUES (:id, NULL)", table)
	if _, err := conn.ExecContext(ctx, insNull, sql.Named("id", int64(3))); err != nil {
		t.Fatalf("insert null blob failed: %v", err)
	}

	// Case 4: Read the small inline BLOB and the large temporary BLOB.
	selSQL := fmt.Sprintf("SELECT bin FROM %s WHERE id = :id", table)
	var outSmall lobpkg.Bytes
	if err := conn.QueryRowContext(ctx, selSQL, sql.Named("id", int64(1))).Scan(&outSmall); err != nil {
		t.Fatalf("select/scan small blob failed: %v", err)
	}
	if !bytes.Equal(outSmall, blobSmall) {
		t.Fatalf("small blob mismatch:\n got:  %x\nwant: %x", outSmall, blobSmall)
	}

	var outLarge lobpkg.Bytes
	if err := conn.QueryRowContext(ctx, selSQL, sql.Named("id", int64(2))).Scan(&outLarge); err != nil {
		t.Fatalf("select/scan large blob failed: %v", err)
	}
	if sha256.Sum256(outLarge) != sha256.Sum256(blobLarge) {
		t.Fatalf("large blob mismatch (sha256 differs):\n got:  %x\nwant: %x",
			sha256.Sum256(outLarge),
			sha256.Sum256(blobLarge),
		)
	}

	// Case 5: Materializing SQL NULL into lob.Bytes is rejected.
	var outNull lobpkg.Bytes
	if err := conn.QueryRowContext(ctx, selSQL, sql.Named("id", int64(3))).Scan(&outNull); err == nil {
		t.Fatal("select/scan NULL BLOB into lob.Bytes succeeded")
	}
}

// TestDriver_Table_Insert_Select_CLOB_BLOB_MultiRows inserts 15 short CLOB and
// BLOB values, then checks every row returned by SELECT *.
func TestDriver_Table_Insert_Select_CLOB_BLOB_MultiRows(t *testing.T) {
	t.Parallel()
	if TestingConfig == nil {
		t.Skip("No configuration available")
	}

	db, err := openTestDBWithConfig(TestingConfig)
	if err != nil {
		t.Fatalf("failed to open test DB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	table := createObjectName("t_ins_sel_clob_blob_15")
	cols := map[string]string{
		"id":  "NUMBER PRIMARY KEY",
		"txt": "CLOB",
		"bin": "BLOB",
	}

	// Recreate table to ensure a clean state for this test run.
	_ = dropTable(ctx, db, table)
	if err := createTable(ctx, db, table, cols); err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	t.Cleanup(func() { _ = dropTable(ctx, db, table) })

	// Build expected row data container for later validation.
	type expectedRow struct {
		clob string
		blob []byte
	}
	expected := make(map[int64]expectedRow, 15)

	// Insert 15 rows of deterministic CLOB/BLOB payloads.
	insSQL := fmt.Sprintf("INSERT INTO %s (id, txt, bin) VALUES (:id, :txt, :bin)", table)
	for i := 1; i <= 15; i++ {
		id := int64(i)
		clobLen := i + 5 // 6..20
		blobLen := i + 5 // 6..20

		clobVal := strings.Repeat(string(rune('a'+(i%26))), clobLen)
		blobVal := bytes.Repeat([]byte{byte('A' + (i % 26))}, blobLen)
		expected[id] = expectedRow{clob: clobVal, blob: blobVal}

		if _, err := db.ExecContext(ctx, insSQL,
			sql.Named("id", id),
			sql.Named("txt", clobVal),
			sql.Named("bin", blobVal),
		); err != nil {
			t.Fatalf("insert failed for id=%d: %v", id, err)
		}
	}

	// Select all rows using SELECT * syntax.
	selSQL := fmt.Sprintf("SELECT * FROM %s", table)
	rows, err := db.QueryContext(ctx, selSQL)
	if err != nil {
		t.Fatalf("select rows failed: %v", err)
	}
	t.Cleanup(func() { _ = rows.Close() })

	// Validate selected column count before scanning rows.
	colsReturned, err := rows.Columns()
	if err != nil {
		t.Fatalf("failed to read selected columns: %v", err)
	}
	if len(colsReturned) != 3 {
		t.Fatalf("unexpected selected column count: got=%d want=3", len(colsReturned))
	}

	// Iterate through selected rows and verify each value matches expected.
	seen := make(map[int64]bool, 15)
	for rows.Next() {
		var (
			id  int64
			txt lobpkg.Text
			bin lobpkg.Bytes
		)

		// Keep SELECT * while making scan resilient to table column order.
		scanTargets := make([]any, len(colsReturned))
		for i, c := range colsReturned {
			switch strings.ToUpper(c) {
			case "ID":
				scanTargets[i] = &id
			case "TXT":
				scanTargets[i] = &txt
			case "BIN":
				scanTargets[i] = &bin
			default:
				var discard any
				scanTargets[i] = &discard
			}
		}

		if err := rows.Scan(scanTargets...); err != nil {
			t.Fatalf("row scan failed: %v", err)
		}

		exp, ok := expected[id]
		if !ok {
			t.Fatalf("unexpected id returned from select: %d", id)
		}

		if string(txt) != exp.clob {
			t.Fatalf("clob mismatch for id=%d: got=%q want=%q", id, txt, exp.clob)
		}

		if !bytes.Equal(bin, exp.blob) {
			t.Fatalf("blob mismatch for id=%d: got=%x want=%x", id, bin, exp.blob)
		}

		seen[id] = true
	}

	// Ensure row iteration completed without driver/database errors.
	if err := rows.Err(); err != nil {
		t.Fatalf("rows iteration failed: %v", err)
	}

	// Confirm all 15 inserted rows were returned and validated.
	if len(seen) != 15 {
		t.Fatalf("row count mismatch: got=%d want=15", len(seen))
	}
}
