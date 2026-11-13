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
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/oracle/go-oracledb/v26/oracle/datatype/lob"
)

type textLOBRowData struct {
	id int64
	n  string
	c  string
}

type blobRowData struct {
	id int64
	n  string
	b  []byte
}

// TestDriver_Prepared_Insert_Clob_Small ensures the driver can bind and transmit
// CLOB payloads up to 32KB via prepared statements. It exercises INSERT
// execution and verifies the RowsAffected metadata returned by the driver.
func TestDriver_Prepared_Insert_Clob_Small(t *testing.T) {
	t.Parallel()
	rows := []textLOBRowData{
		{
			id: 1,
			n:  "aaa",
			c:  "hello prepared",
		},
		{
			id: 2,
			n:  "bbb",
			c:  strings.Repeat("a", 4000),
		},
		{
			id: 3,
			n:  "ccc",
			c:  strings.Repeat("b", 20000),
		},
		{
			id: 4,
			n:  "ddd",
			c:  strings.Repeat("c", 32768),
		},
	}

	runPreparedInsertTextLOB(t, createObjectName("clob1_small"), rows, lob.KindCLOB)
}

// TestDriver_Prepared_Insert_Clob_Large covers CLOB payloads larger than 32KB to
// ensure the driver can stream large data through prepared statements.
func TestDriver_Prepared_Insert_Clob_Large(t *testing.T) {
	t.Parallel()
	rows := []textLOBRowData{
		{
			id: 5,
			n:  "eee",
			c:  strings.Repeat("o", 11534336), // 11MB
		},
		{
			id: 6,
			n:  "fff",
			c:  strings.Repeat("m", 33554432), // 32MB
		},
		{
			id: 7,
			n:  "ggg",
			c:  strings.Repeat("t", 41943040), // 40MB
		},
	}

	runPreparedInsertTextLOB(t, createObjectName("clob1_large"), rows, lob.KindCLOB)
}

// TestDriver_Prepared_Insert_Blob_Small mirrors the CLOB small insertion test
// but uses BLOB columns bound from Go []byte values (RAW bind type).
func TestDriver_Prepared_Insert_Blob_Small(t *testing.T) {
	t.Parallel()
	rows := []blobRowData{
		{
			id: 1,
			n:  "aaa",
			b:  []byte("hello prepared"),
		},
		{
			id: 2,
			n:  "bbb",
			b:  bytes.Repeat([]byte{0xAB}, 4000),
		},
		{
			id: 3,
			n:  "ccc",
			b:  bytes.Repeat([]byte{0xBC}, 20000),
		},
		{
			id: 4,
			n:  "ddd",
			b:  bytes.Repeat([]byte{0xCD}, 32768),
		},
	}

	runPreparedInsertBlob(t, createObjectName("blob1_small"), rows)
}

// TestDriver_Prepared_Insert_Blob_Large covers large BLOB payloads to verify
// inserts via prepared statements continues to work for >32KB data.
func TestDriver_Prepared_Insert_Blob_Large(t *testing.T) {
	t.Parallel()
	rows := []blobRowData{
		{
			id: 5,
			n:  "eee",
			b:  bytes.Repeat([]byte{0xDE}, 11534336), // 11MB
		},
		{
			id: 6,
			n:  "fff",
			b:  bytes.Repeat([]byte{0xEF}, 33554432), // 32MB
		},
		{
			id: 7,
			n:  "ggg",
			b:  bytes.Repeat([]byte{0xFA}, 41943040), // 40MB
		},
	}

	runPreparedInsertBlob(t, createObjectName("blob1_large"), rows)
}

// runPreparedInsertTextLOB inserts CLOB or NCLOB rows and verifies their
// affected-row counts and readback.
func runPreparedInsertTextLOB(t *testing.T, table string, rows []textLOBRowData, kind lob.Kind) {
	t.Helper()

	column, sqlType := "", ""
	switch kind {
	case lob.KindCLOB:
		column, sqlType = "clob", "CLOB"
	case lob.KindNCLOB:
		column, sqlType = "nclob", "NCLOB"
	default:
		t.Fatalf("unsupported text LOB kind: %v", kind)
	}

	if TestingConfig == nil {
		t.Skip("No configuration available")
	}
	db, err := openTestDBWithConfig(TestingConfig)
	if err != nil {
		t.Fatalf("failed to open test DB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	if kind == lob.KindNCLOB {
		dbCharset, err := fetchNLSCharacterSet(ctx, db)
		if err != nil {
			t.Fatalf("fetch NLS_CHARACTERSET: %v", err)
		}
		ncharCharset, err := fetchNLSNcharCharacterSet(ctx, db)
		if err != nil {
			t.Fatalf("fetch NLS_NCHAR_CHARACTERSET: %v", err)
		}
		if dbCharset != "AL32UTF8" {
			t.Skipf("skipping prepared insert NCLOB test: expected NLS_CHARACTERSET AL32UTF8, got %s", dbCharset)
		}
		if ncharCharset != "AL16UTF16" {
			t.Skipf("skipping prepared insert NCLOB test: expected NLS_NCHAR_CHARACTERSET AL16UTF16, got %s", ncharCharset)
		}
	}

	cols := map[string]string{
		"id":   "NUMBER PRIMARY KEY",
		"name": "VARCHAR2(20)",
	}
	cols[column] = sqlType

	if err := createTable(ctx, db, table, cols); err != nil {
		t.Skipf("Skipping prepared insert %s test (create failed): %v", sqlType, err)
		return
	}
	t.Cleanup(func() {
		if e := dropTable(ctx, db, table); e != nil {
			t.Errorf("cleanup drop table %s failed: %v", table, e)
		}
	})

	insSQL := fmt.Sprintf("INSERT INTO %s (id, name, %s) ", table, column) +
		"VALUES (:id, :n, :c)"
	insStmt, err := db.PrepareContext(ctx, insSQL)
	if err != nil {
		t.Fatalf("prepare insert failed: %v", err)
	}
	defer func() { _ = insStmt.Close() }()

	for _, rr := range rows {
		result, err := insStmt.ExecContext(ctx,
			sql.Named("id", rr.id),
			sql.Named("n", rr.n),
			sql.Named("c", rr.c),
		)

		if err != nil {
			t.Fatalf("exec insert failed for id=%d: %v", rr.id, err)
		}

		affected, err := result.RowsAffected()
		if err != nil {
			t.Fatalf("fetch RowsAffected failed for id=%d: %v", rr.id, err)
		}
		if affected != 1 {
			t.Fatalf("unexpected rows affected for id=%d: got %d want 1", rr.id, affected)
		}

		var gotText lob.Text
		err = db.QueryRowContext(ctx,
			fmt.Sprintf("SELECT %s FROM %s WHERE id = %d", column, table, rr.id),
		).Scan(&gotText)
		if err != nil {
			t.Fatalf("select inserted row failed for id=%d: %v", rr.id, err)
		}
		if string(gotText) != rr.c {
			t.Fatalf("%s mismatch for id=%d: got %d bytes, want %d", sqlType, rr.id, len(gotText), len(rr.c))
		}
	}
}

// runPreparedInsertBlob executes the prepared BLOB inserts and verifies their
// affected-row counts and readback for payloads within the materialization cap.
func runPreparedInsertBlob(t *testing.T, table string, rows []blobRowData) {
	t.Helper()

	if TestingConfig == nil {
		t.Skip("No configuration available")
	}

	db, err := openTestDBWithConfig(TestingConfig)
	if err != nil {
		t.Fatalf("failed to open test DB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	cols := map[string]string{
		"id":   "NUMBER PRIMARY KEY",
		"name": "VARCHAR2(20)",
		"blob": "BLOB",
	}

	if err := createTable(ctx, db, table, cols); err != nil {
		t.Skipf("Skipping prepared insert BLOB test (create failed): %v", err)
		return
	}
	t.Cleanup(func() {
		if e := dropTable(ctx, db, table); e != nil {
			t.Errorf("cleanup drop table %s failed: %v", table, e)
		}
	})

	insSQL := "INSERT INTO " + table + " (id, name, blob) " +
		"VALUES (:id, :n, :b)"
	insStmt, err := db.PrepareContext(ctx, insSQL)
	if err != nil {
		t.Fatalf("prepare insert failed: %v", err)
	}
	defer func() { _ = insStmt.Close() }()

	for _, rr := range rows {
		result, err := insStmt.ExecContext(ctx,
			sql.Named("id", rr.id),
			sql.Named("n", rr.n),
			sql.Named("b", rr.b),
		)

		if err != nil {
			t.Fatalf("exec insert failed for id=%d: %v", rr.id, err)
		}

		affected, err := result.RowsAffected()
		if err != nil {
			t.Fatalf("fetch RowsAffected failed for id=%d: %v", rr.id, err)
		}
		if affected != 1 {
			t.Fatalf("unexpected rows affected for id=%d: got %d want 1", rr.id, affected)
		}

		var gotBlob lob.Bytes
		err = db.QueryRowContext(ctx,
			fmt.Sprintf("SELECT blob FROM %s WHERE id = %d", table, rr.id),
		).Scan(&gotBlob)
		if err != nil {
			t.Fatalf("select inserted row failed for id=%d: %v", rr.id, err)
		}
		if !bytes.Equal(gotBlob, rr.b) {
			t.Fatalf("BLOB mismatch for id=%d: got %d bytes, want %d", rr.id, len(gotBlob), len(rr.b))
		}
	}
}

// TestDriver_Prepared_Insert_Nclob_Small verifies prepared NCLOB inserts with
// accented, non-Latin, and supplementary Unicode text.
func TestDriver_Prepared_Insert_Nclob_Small(t *testing.T) {
	t.Parallel()
	rows := []textLOBRowData{
		{
			id: 1,
			n:  "aaa",
			c:  "hello nclob 山田太郎",
		},
		{
			id: 2,
			n:  "bbb",
			c:  strings.Repeat("李小龙", 2000),
		},
		{
			id: 3,
			n:  "ccc",
			c:  strings.Repeat("مرحبا", 4000),
		},
		{
			id: 4,
			n:  "ddd",
			c:  strings.Repeat("👨‍💻🖥️", 1500),
		},
	}

	runPreparedInsertTextLOB(t, createObjectName("nclob1_small"), rows, lob.KindNCLOB)
}

// TestDriver_LOBBindLargeAndEmptyValues checks large temporary LOB inserts and
// empty marker binds for BLOB, CLOB, and NCLOB.
//
// Setup and readback:
//   - Create a table with BLOB, CLOB, and NCLOB columns.
//   - Write each large value to a temporary LOB, bind it in INSERT, and verify
//     the persistent table contents.
//   - Update the row with empty marker values and verify their SQL NULL status
//     and DBMS_LOB.GETLENGTH.
//
// Cases:
//   - 128 KiB BLOB, 256 KiB UTF-8 CLOB, and 272 KiB UTF-8 NCLOB round-trip.
//   - Empty marker values remain non-NULL, zero-length LOBs after UPDATE.
func TestDriver_LOBBindLargeAndEmptyValues(t *testing.T) {
	t.Parallel()
	if TestingConfig == nil {
		t.Skip("No configuration available")
	}
	db, err := openTestDBWithConfig(TestingConfig)
	if err != nil {
		t.Fatalf("open test DB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	table := createObjectName("lob_bind")
	if err := createTable(ctx, db, table, map[string]string{
		"id": "NUMBER PRIMARY KEY", "bin": "BLOB", "txt": "CLOB", "ntxt": "NCLOB",
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	deferTableCleanup(t, db, table)

	blobWant := bytes.Repeat([]byte{0x00, 0x41, 0x80, 0xff}, 32*1024+1) // 128 KiB + 4 bytes
	clobWant := strings.Repeat("CLOB Aé中🙂 ", 16*1024)                   // 256 KiB UTF-8
	nclobWant := strings.Repeat("NCLOB Aé中🙂 ", 16*1024)                 // 272 KiB UTF-8
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("get dedicated LOB connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	temporaryBlob := writeTemporaryBlob(t, ctx, conn, blobWant)
	temporaryClob := writeTemporaryClob(t, ctx, conn, clobWant)
	temporaryNclob := writeTemporaryNClob(t, ctx, conn, nclobWant)

	// Case 1: Insert large temporary LOB objects and verify their contents.
	insert := fmt.Sprintf("INSERT INTO %s (id, bin, txt, ntxt) VALUES (:1, :2, :3, :4)", table)
	if _, err := conn.ExecContext(ctx, insert, int64(1), temporaryBlob, temporaryClob, temporaryNclob); err != nil {
		t.Fatalf("insert large temporary LOBs: %v", err)
	}
	if err := temporaryBlob.FreeContext(ctx); err != nil {
		t.Fatalf("free temporary BLOB: %v", err)
	}
	if err := temporaryClob.FreeContext(ctx); err != nil {
		t.Fatalf("free temporary CLOB: %v", err)
	}
	if err := temporaryNclob.FreeContext(ctx); err != nil {
		t.Fatalf("free temporary NCLOB: %v", err)
	}

	var blobGot lob.Bytes
	var clobGot, nclobGot lob.Text
	selectValues := fmt.Sprintf("SELECT bin, txt, ntxt FROM %s WHERE id = :1", table)
	if err := conn.QueryRowContext(ctx, selectValues, int64(1)).Scan(&blobGot, &clobGot, &nclobGot); err != nil {
		t.Fatalf("scan inserted values: %v", err)
	}
	if !bytes.Equal(blobGot, blobWant) {
		t.Fatalf("large BLOB round trip mismatch: got %d bytes, want %d", len(blobGot), len(blobWant))
	}
	if got := string(clobGot); got != clobWant {
		t.Fatalf("large CLOB round trip mismatch: got %d bytes, want %d", len(got), len(clobWant))
	}
	if got := string(nclobGot); got != nclobWant {
		t.Fatalf("large NCLOB round trip mismatch: got %d bytes, want %d", len(got), len(nclobWant))
	}

	// Case 2: Update each column with an empty marker bind.
	update := fmt.Sprintf("UPDATE %s SET bin = :1, txt = :2, ntxt = :3 WHERE id = :4", table)
	if _, err := conn.ExecContext(ctx, update, BindBlob([]byte{}), BindClob(""), BindNClob(""), int64(1)); err != nil {
		t.Fatalf("update empty marker values: %v", err)
	}
	var binNull, txtNull, nclobNull int
	var binLength, txtLength, nclobLength int64
	emptyState := fmt.Sprintf(`SELECT
		CASE WHEN bin IS NULL THEN 1 ELSE 0 END, DBMS_LOB.GETLENGTH(bin),
		CASE WHEN txt IS NULL THEN 1 ELSE 0 END, DBMS_LOB.GETLENGTH(txt),
		CASE WHEN ntxt IS NULL THEN 1 ELSE 0 END, DBMS_LOB.GETLENGTH(ntxt)
		FROM %s WHERE id = :1`, table)
	if err := conn.QueryRowContext(ctx, emptyState, int64(1)).Scan(
		&binNull, &binLength, &txtNull, &txtLength, &nclobNull, &nclobLength,
	); err != nil {
		t.Fatalf("query empty LOB state: %v", err)
	}
	// The SQL values must be non-NULL and have length zero.
	testCases := []struct {
		name     string
		nullFlag int
		length   int64
	}{
		{"BLOB", binNull, binLength},
		{"CLOB", txtNull, txtLength},
		{"NCLOB", nclobNull, nclobLength},
	}
	for _, testCase := range testCases {
		if testCase.nullFlag != 0 || testCase.length != 0 {
			t.Fatalf("empty %s state = (NULL %d, length %d), want (NULL 0, length 0)",
				testCase.name, testCase.nullFlag, testCase.length)
		}
	}

	// Scanning the empty LOBs should return empty public values.
	if err := conn.QueryRowContext(ctx, selectValues, int64(1)).Scan(&blobGot, &clobGot, &nclobGot); err != nil {
		t.Fatalf("scan empty marker values: %v", err)
	}
	emptyReadbackCases := []struct {
		name      string
		byteCount int
	}{
		{"BLOB", len(blobGot)},
		{"CLOB", len(clobGot)},
		{"NCLOB", len(nclobGot)},
	}
	for _, testCase := range emptyReadbackCases {
		if testCase.byteCount != 0 {
			t.Fatalf("empty %s readback has %d bytes, want 0", testCase.name, testCase.byteCount)
		}
	}
}

// TestDriver_TemporaryLOBObjectBindInsert verifies that caller-created
// temporary LOBs can be written in chunks and bound directly to an INSERT.
//
// Setup and APIs:
//   - Create temporary LOBs on a dedicated *sql.Conn.
//   - Write payloads larger than one write chunk with the public Write APIs.
//   - Pass the typed LOB objects to ExecContext and scan the inserted values.
//
// Cases:
//   - Large binary data in a temporary BLOB.
//   - Multibyte UTF-8 text in temporary CLOB and NCLOB values.
func TestDriver_TemporaryLOBObjectBindInsert(t *testing.T) {
	t.Parallel()
	if TestingConfig == nil {
		t.Skip("No configuration available")
	}
	db, err := openTestDBWithConfig(TestingConfig)
	if err != nil {
		t.Fatalf("open test DB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("get dedicated connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	dbCharset, err := fetchNLSCharacterSet(ctx, db)
	if err != nil {
		t.Fatalf("fetch NLS_CHARACTERSET: %v", err)
	}
	ncharCharset, err := fetchNLSNcharCharacterSet(ctx, db)
	if err != nil {
		t.Fatalf("fetch NLS_NCHAR_CHARACTERSET: %v", err)
	}
	if dbCharset != "AL32UTF8" {
		t.Skipf("expected NLS_CHARACTERSET AL32UTF8, got %s", dbCharset)
	}
	if ncharCharset != "AL16UTF16" {
		t.Skipf("expected NLS_NCHAR_CHARACTERSET AL16UTF16, got %s", ncharCharset)
	}

	table := createObjectName("lob_obj_bind")
	if err := createTable(ctx, db, table, map[string]string{
		"id": "NUMBER PRIMARY KEY", "bin": "BLOB", "txt": "CLOB", "ntxt": "NCLOB",
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	deferTableCleanup(t, db, table)

	// Case 1: Stream multiple binary chunks into a temporary BLOB.
	blobWant := bytes.Repeat([]byte{0x00, 0x01, 0x7f, 0x80, 0xff}, 24*1024)
	temporaryBlob, err := lob.CreateTemporaryBlob(ctx, conn)
	if err != nil {
		t.Fatalf("create temporary BLOB: %v", err)
	}
	t.Cleanup(func() {
		if err := temporaryBlob.FreeContext(ctx); err != nil {
			t.Errorf("free temporary BLOB: %v", err)
		}
	})
	if n, err := temporaryBlob.WriteContext(ctx, blobWant); err != nil || n != len(blobWant) {
		t.Fatalf("write temporary BLOB = (%d, %v), want (%d, nil)", n, err, len(blobWant))
	}

	// Case 2: Stream multiple chunks of multibyte UTF-8 text into a
	// temporary CLOB.
	clobWant := strings.Repeat("CLOB café 中 🙂 ", 8*1024)
	temporaryClob, err := lob.CreateTemporaryClob(ctx, conn)
	if err != nil {
		t.Fatalf("create temporary CLOB: %v", err)
	}
	t.Cleanup(func() {
		if err := temporaryClob.FreeContext(ctx); err != nil {
			t.Errorf("free temporary CLOB: %v", err)
		}
	})
	if n, err := temporaryClob.WriteStringContext(ctx, clobWant); err != nil || n != len(clobWant) {
		t.Fatalf("write temporary CLOB = (%d, %v), want (%d, nil)", n, err, len(clobWant))
	}

	// Case 3: Stream multiple chunks of supplementary Unicode text into a
	// temporary NCLOB.
	nclobWant := strings.Repeat("NCLOB नमस्ते 🌏🙂 ", 8*1024)
	temporaryNclob, err := lob.CreateTemporaryNClob(ctx, conn)
	if err != nil {
		t.Fatalf("create temporary NCLOB: %v", err)
	}
	t.Cleanup(func() {
		if err := temporaryNclob.FreeContext(ctx); err != nil {
			t.Errorf("free temporary NCLOB: %v", err)
		}
	})
	if n, err := temporaryNclob.WriteStringContext(ctx, nclobWant); err != nil || n != len(nclobWant) {
		t.Fatalf("write temporary NCLOB = (%d, %v), want (%d, nil)", n, err, len(nclobWant))
	}

	// Case 4: Bind the typed temporary LOB objects directly, then verify the
	// permanent table values and confirm the source locators remain caller-owned.
	insert := fmt.Sprintf("INSERT INTO %s (id, bin, txt, ntxt) VALUES (:1, :2, :3, :4)", table)
	if _, err := conn.ExecContext(ctx, insert, int64(1), temporaryBlob, temporaryClob, temporaryNclob); err != nil {
		t.Fatalf("insert temporary LOB objects: %v", err)
	}
	if !temporaryBlob.Valid() || !temporaryBlob.IsTemporary() ||
		!temporaryClob.Valid() || !temporaryClob.IsTemporary() ||
		!temporaryNclob.Valid() || !temporaryNclob.IsTemporary() {
		t.Fatal("binding changed caller-owned temporary LOB state")
	}

	var blobGot lob.Bytes
	var clobGot, nclobGot lob.Text
	query := fmt.Sprintf("SELECT bin, txt, ntxt FROM %s WHERE id = :1", table)
	if err := conn.QueryRowContext(ctx, query, int64(1)).Scan(&blobGot, &clobGot, &nclobGot); err != nil {
		t.Fatalf("scan inserted LOB values: %v", err)
	}
	if !bytes.Equal(blobGot, blobWant) {
		t.Fatalf("inserted BLOB = %d bytes, want %d", len(blobGot), len(blobWant))
	}
	if got := string(clobGot); got != clobWant {
		t.Fatalf("inserted CLOB = %d bytes, want %d", len(got), len(clobWant))
	}
	if got := string(nclobGot); got != nclobWant {
		t.Fatalf("inserted NCLOB = %d bytes, want %d", len(got), len(nclobWant))
	}
}
