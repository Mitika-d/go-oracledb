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
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/oracle/go-oracledb/v26/oracle/datatype/lob"
)

// TestDriver_TypedLOBStreamingPastPrefetch verifies ReadContext continues from
// each LOB locator after the prefetched prefix is exhausted.
//
// Setup and APIs:
//   - Set the LOB prefetch size to 8 bytes.
//   - Insert values with ordinary []byte and string bind arguments.
//   - Scan into the public Blob, Clob, and NClob types, close Rows, then read
//     each value with ReadContext.
//
// Cases:
//   - 120 KiB BLOB payload containing zero and high-bit bytes.
//   - 28 KiB UTF-8 CLOB and 56 KiB UTF-8 NCLOB payloads containing multibyte
//     and supplementary characters.
func TestDriver_TypedLOBStreamingPastPrefetch(t *testing.T) {
	t.Parallel()
	db := openLOBFunctionalDBWithPrefetch(t, 8)
	ctx := context.Background()

	table := createObjectName("lob_stream")
	if err := createTable(ctx, db, table, map[string]string{
		"id": "NUMBER PRIMARY KEY", "bin": "BLOB", "txt": "CLOB", "ntxt": "NCLOB",
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	deferTableCleanup(t, db, table)

	blobWant := bytes.Repeat([]byte{0, 1, 127, 128, 254, 255}, 20*1024) // 120 KiB
	clobWant := strings.Repeat("CLOB é🙂中", 2*1024)                      // 28 KiB in UTF-8
	nclobWant := strings.Repeat("NCLOB नमस्ते🌏", 2*1024)                // 56 KiB in UTF-8
	// Case 1: Seed the table using ordinary Go values, without LOB bind markers.
	insert := fmt.Sprintf("INSERT INTO %s (id, bin, txt, ntxt) VALUES (:1, :2, :3, :4)", table)
	if _, err := db.ExecContext(ctx, insert, int64(1), blobWant, clobWant, nclobWant); err != nil {
		t.Fatalf("insert LOB values: %v", err)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("get dedicated connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	var blob lob.Blob
	var clob lob.Clob
	var nclob lob.NClob
	t.Cleanup(func() { _ = blob.Close() })
	t.Cleanup(func() { _ = clob.Close() })
	t.Cleanup(func() { _ = nclob.Close() })

	rows, err := conn.QueryContext(ctx, fmt.Sprintf("SELECT bin, txt, ntxt FROM %s WHERE id = :1", table), int64(1))
	if err != nil {
		t.Fatalf("query LOBs: %v", err)
	}
	if !rows.Next() {
		_ = rows.Close()
		t.Fatalf("query returned no row: %v", rows.Err())
	}
	if err := rows.Scan(&blob, &clob, &nclob); err != nil {
		_ = rows.Close()
		t.Fatalf("scan LOBs: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close source rows: %v", err)
	}

	// Case 2: Read each typed LOB after Rows.Close; the 8-byte prefetch is only
	// a prefix, so the remaining data must be fetched from its locator.
	if got := readTypedLOBContents(t, ctx, blob.ReadContext); !bytes.Equal(got, blobWant) {
		t.Fatalf("streamed BLOB length = %d, want %d", len(got), len(blobWant))
	}
	if got := string(readTypedLOBContents(t, ctx, clob.ReadContext)); got != clobWant {
		t.Fatalf("streamed CLOB length = %d UTF-8 bytes, want %d", len(got), len(clobWant))
	}
	if got := string(readTypedLOBContents(t, ctx, nclob.ReadContext)); got != nclobWant {
		t.Fatalf("streamed NCLOB length = %d UTF-8 bytes, want %d", len(got), len(nclobWant))
	}
}

// TestDriver_LOBPrefetchBoundaryBelowDefault verifies materialized and typed
// streaming reads with a 32 KiB LOB prefetch setting.
//
// Setup and APIs:
//   - Write BLOB and CLOB values that extend at least 64 KiB beyond prefetch
//     into temporary LOBs, then bind the LOB objects in INSERT and UPDATE.
//   - Compare materialized scans with typed Blob and Clob ReadContext results.
//
// Cases:
//   - 96 KiB BLOB with a 32 KiB prefetch.
//   - CLOB with a 96 KiB ASCII prefix, a multibyte suffix, and a 32 KiB prefetch.
func TestDriver_LOBPrefetchBoundaryBelowDefault(t *testing.T) {
	runDriverLOBPrefetchBoundaryTest(t, 32*1024)
}

// TestDriver_LOBPrefetchBoundaryAboveDefault verifies the same read paths with
// a 40 MiB prefetch setting, above the driver's default.
//
// Setup and APIs:
//   - Write BLOB and CLOB values that extend at least 64 KiB beyond prefetch
//     into temporary LOBs, then bind the LOB objects in INSERT and UPDATE.
//   - Compare materialized scans with typed Blob and Clob ReadContext results.
//
// Cases:
//   - 40 MiB + 64 KiB BLOB with a 40 MiB prefetch.
//   - CLOB with a 40 MiB + 64 KiB ASCII prefix, a multibyte suffix, and a
//     40 MiB prefetch.
func TestDriver_LOBPrefetchBoundaryAboveDefault(t *testing.T) {
	runDriverLOBPrefetchBoundaryTest(t, 40*1024*1024)
}

// runDriverLOBPrefetchBoundaryTest exercises materialized and locator reads
// for BLOB and CLOB values larger than prefetchSize.
//
// Steps:
//   - Open a database with prefetchSize and create a table with BLOB and CLOB columns.
//   - Write a BLOB of prefetchSize + 64 KiB and a CLOB with that many ASCII
//     characters plus a multibyte suffix into temporary LOBs, then bind them.
//   - Scan into lob.Bytes and lob.Text and compare with the inserted values.
//   - Query on a dedicated connection into lob.Blob and lob.Clob, close Rows,
//     then compare ReadContext output with the expected values.
func runDriverLOBPrefetchBoundaryTest(t *testing.T, prefetchSize int) {
	t.Helper()
	db := openLOBFunctionalDBWithPrefetch(t, prefetchSize)
	ctx := context.Background()
	table := createObjectName("lob_prefetch")
	if err := createTable(ctx, db, table, map[string]string{
		"id": "NUMBER PRIMARY KEY", "bin": "BLOB", "txt": "CLOB",
	}); err != nil {
		t.Fatalf("create boundary table: %v", err)
	}
	deferTableCleanup(t, db, table)

	blobWant := functionalLOBBytes(prefetchSize + 64*1024)
	clobWant := functionalLOBText(prefetchSize + 64*1024)
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("get dedicated LOB connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	temporaryBlob := writeTemporaryBlob(t, ctx, conn, blobWant)
	temporaryClob := writeTemporaryClob(t, ctx, conn, clobWant)
	// Case 1: Store values larger than prefetch through streamed temporary LOBs.
	insertBlob := fmt.Sprintf("INSERT INTO %s (id, bin) VALUES (:1, :2)", table)
	if _, err := conn.ExecContext(ctx, insertBlob, int64(1), temporaryBlob); err != nil {
		t.Fatalf("insert boundary BLOB: %v", err)
	}
	updateClob := fmt.Sprintf("UPDATE %s SET txt = :1 WHERE id = :2", table)
	if _, err := conn.ExecContext(ctx, updateClob, temporaryClob, int64(1)); err != nil {
		t.Fatalf("update boundary CLOB: %v", err)
	}
	if err := temporaryBlob.FreeContext(ctx); err != nil {
		t.Fatalf("free temporary BLOB: %v", err)
	}
	if err := temporaryClob.FreeContext(ctx); err != nil {
		t.Fatalf("free temporary CLOB: %v", err)
	}

	// Case 2: Verify materialized scans return the complete values.
	var materializedBlob lob.Bytes
	var materializedClob lob.Text
	query := fmt.Sprintf("SELECT bin, txt FROM %s WHERE id = :1", table)
	if err := conn.QueryRowContext(ctx, query, int64(1)).Scan(&materializedBlob, &materializedClob); err != nil {
		t.Fatalf("materialize boundary LOBs: %v", err)
	}
	if !bytes.Equal(materializedBlob, blobWant) || string(materializedClob) != clobWant {
		t.Fatal("materialized boundary LOB values did not match the inserted payloads")
	}

	var blob lob.Blob
	var clob lob.Clob
	t.Cleanup(func() {
		if blob.Valid() {
			if err := blob.FreeContext(ctx); err != nil {
				t.Errorf("free query BLOB: %v", err)
			}
		}
	})
	t.Cleanup(func() {
		if clob.Valid() {
			if err := clob.FreeContext(ctx); err != nil {
				t.Errorf("free query CLOB: %v", err)
			}
		}
	})

	// Case 3: Fetch typed locators and close Rows before reading their contents.
	rows, err := conn.QueryContext(ctx, query, int64(1))
	if err != nil {
		t.Fatalf("query boundary LOBs for streaming: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("boundary query returned no row: %v", rows.Err())
	}
	if err := rows.Scan(&blob, &clob); err != nil {
		t.Fatalf("scan typed boundary LOBs: %v", err)
	}
	if rows.Next() {
		t.Fatal("boundary query returned more than one row")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate boundary rows: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close boundary rows: %v", err)
	}

	if !blob.Valid() || blob.Kind() != lob.KindBLOB {
		t.Fatalf("BLOB locator metadata = (valid %t, kind %v)", blob.Valid(), blob.Kind())
	}
	if !clob.Valid() || clob.Kind() != lob.KindCLOB {
		t.Fatalf("CLOB locator metadata = (valid %t, kind %v)", clob.Valid(), clob.Kind())
	}
	// Case 4: Compare each locator stream with its full payload.
	readCases := []struct {
		name     string
		read     func(context.Context, []byte) (int, error)
		expected io.Reader
	}{
		{"BLOB", blob.ReadContext, bytes.NewReader(blobWant)},
		{"CLOB", clob.ReadContext, strings.NewReader(clobWant)},
	}
	for _, testCase := range readCases {
		if err := compareTypedLOBContents(t, ctx, testCase.read, testCase.expected); err != nil {
			t.Fatalf("read %s: %v", testCase.name, err)
		}
	}
}

// TestDriver_LOBMultipleColumnsLowPrefetch verifies six independent typed
// LOB locators with a 32 KiB prefetch setting.
//
// Setup and APIs:
//   - Stream two values each into temporary BLOB, CLOB, and NCLOB objects and
//     insert all six LOB objects in one statement.
//   - Scan into the public typed LOB values, close Rows, and read with ReadContext.
//
// Cases:
//   - Two BLOBs, two CLOBs, and two NCLOBs each exceed the 32 KiB prefetch by
//     at least 64 KiB.
//   - Each locator returns only its own column's value after Rows.Close.
func TestDriver_LOBMultipleColumnsLowPrefetch(t *testing.T) {
	runDriverLOBMultipleColumnsTest(t, 32*1024)
}

// TestDriver_LOBMultipleColumnsConfiguredPrefetch verifies six independent
// typed LOB locators with an 8 MiB prefetch setting.
//
// Setup and APIs:
//   - Stream two values each into temporary BLOB, CLOB, and NCLOB objects and
//     insert all six LOB objects in one statement.
//   - Scan into the public typed LOB values, close Rows, and read with ReadContext.
//
// Cases:
//   - Two BLOBs, two CLOBs, and two NCLOBs each exceed the 8 MiB prefetch by
//     at least 64 KiB.
//   - Each locator returns only its own column's value after Rows.Close.
func TestDriver_LOBMultipleColumnsConfiguredPrefetch(t *testing.T) {
	runDriverLOBMultipleColumnsTest(t, 8*1024*1024)
}

// runDriverLOBMultipleColumnsTest checks that multiple locators of each LOB
// kind remain independent when one row is fetched with prefetchSize.
//
// Steps:
//   - Create a row with two BLOB, two CLOB, and two NCLOB columns.
//   - Build distinct payloads larger than prefetchSize, stream each into a
//     temporary LOB, and bind the six LOB objects in one INSERT.
//   - Keep each column's bind value, scan destination, reader, and expected
//     payload together in one test case.
//   - Scan all six columns into typed LOB values and close Rows.
//   - Read each locator with ReadContext and compare it with its matching payload.
func runDriverLOBMultipleColumnsTest(t *testing.T, prefetchSize int) {
	t.Helper()
	db := openLOBFunctionalDBWithPrefetch(t, prefetchSize)
	ctx := context.Background()
	table := createObjectName("lob_multi")
	if err := createTable(ctx, db, table, map[string]string{
		"id": "NUMBER PRIMARY KEY", "bin_one": "BLOB", "bin_two": "BLOB",
		"txt_one": "CLOB", "txt_two": "CLOB", "ntxt_one": "NCLOB", "ntxt_two": "NCLOB",
	}); err != nil {
		t.Fatalf("create multiple-LOB table: %v", err)
	}
	deferTableCleanup(t, db, table)

	// Case 1: Build distinct payloads that extend beyond the configured prefetch.
	blobOne := functionalLOBBytes(prefetchSize + 64*1024)
	blobTwo := functionalLOBBytes(prefetchSize + 64*1024 + 17)
	clobOne := functionalLOBText(prefetchSize + 64*1024)
	clobTwo := "second CLOB: " + functionalLOBText(prefetchSize+64*1024)
	nclobOne := "first NCLOB: " + functionalLOBText(prefetchSize+64*1024)
	nclobTwo := "second NCLOB: " + functionalLOBText(prefetchSize+64*1024+19)
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("get dedicated LOB connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	temporaryBlobOne := writeTemporaryBlob(t, ctx, conn, blobOne)
	temporaryBlobTwo := writeTemporaryBlob(t, ctx, conn, blobTwo)
	temporaryClobOne := writeTemporaryClob(t, ctx, conn, clobOne)
	temporaryClobTwo := writeTemporaryClob(t, ctx, conn, clobTwo)
	temporaryNclobOne := writeTemporaryNClob(t, ctx, conn, nclobOne)
	temporaryNclobTwo := writeTemporaryNClob(t, ctx, conn, nclobTwo)
	var blobOneValue, blobTwoValue lob.Blob
	var clobOneValue, clobTwoValue lob.Clob
	var nclobOneValue, nclobTwoValue lob.NClob
	// Keep case order aligned with the INSERT and SELECT column order below.
	testCases := []struct {
		name        string
		bindValue   any
		destination any
		read        func(context.Context, []byte) (int, error)
		expected    io.Reader
	}{
		{
			name: "BLOB one", bindValue: temporaryBlobOne, destination: &blobOneValue,
			read: blobOneValue.ReadContext, expected: bytes.NewReader(blobOne),
		},
		{
			name: "BLOB two", bindValue: temporaryBlobTwo, destination: &blobTwoValue,
			read: blobTwoValue.ReadContext, expected: bytes.NewReader(blobTwo),
		},
		{
			name: "CLOB one", bindValue: temporaryClobOne, destination: &clobOneValue,
			read: clobOneValue.ReadContext, expected: strings.NewReader(clobOne),
		},
		{
			name: "CLOB two", bindValue: temporaryClobTwo, destination: &clobTwoValue,
			read: clobTwoValue.ReadContext, expected: strings.NewReader(clobTwo),
		},
		{
			name: "NCLOB one", bindValue: temporaryNclobOne, destination: &nclobOneValue,
			read: nclobOneValue.ReadContext, expected: strings.NewReader(nclobOne),
		},
		{
			name: "NCLOB two", bindValue: temporaryNclobTwo, destination: &nclobTwoValue,
			read: nclobTwoValue.ReadContext, expected: strings.NewReader(nclobTwo),
		},
	}
	insert := fmt.Sprintf(
		"INSERT INTO %s (id, bin_one, bin_two, txt_one, txt_two, ntxt_one, ntxt_two) "+
			"VALUES (:1, :2, :3, :4, :5, :6, :7)",
		table,
	)
	// Case 2: Insert all six caller-created temporary LOB objects together.
	bindValues := []any{int64(1)}
	for _, testCase := range testCases {
		bindValues = append(bindValues, testCase.bindValue)
	}
	if _, err := conn.ExecContext(ctx, insert, bindValues...); err != nil {
		t.Fatalf("insert multiple LOBs: %v", err)
	}
	if err := temporaryBlobOne.FreeContext(ctx); err != nil {
		t.Fatalf("free temporary BLOB one: %v", err)
	}
	if err := temporaryBlobTwo.FreeContext(ctx); err != nil {
		t.Fatalf("free temporary BLOB two: %v", err)
	}
	if err := temporaryClobOne.FreeContext(ctx); err != nil {
		t.Fatalf("free temporary CLOB one: %v", err)
	}
	if err := temporaryClobTwo.FreeContext(ctx); err != nil {
		t.Fatalf("free temporary CLOB two: %v", err)
	}
	if err := temporaryNclobOne.FreeContext(ctx); err != nil {
		t.Fatalf("free temporary NCLOB one: %v", err)
	}
	if err := temporaryNclobTwo.FreeContext(ctx); err != nil {
		t.Fatalf("free temporary NCLOB two: %v", err)
	}
	t.Cleanup(func() { _ = blobOneValue.Close() })
	t.Cleanup(func() { _ = blobTwoValue.Close() })
	t.Cleanup(func() { _ = clobOneValue.Close() })
	t.Cleanup(func() { _ = clobTwoValue.Close() })
	t.Cleanup(func() { _ = nclobOneValue.Close() })
	t.Cleanup(func() { _ = nclobTwoValue.Close() })

	query := fmt.Sprintf(
		"SELECT bin_one, bin_two, txt_one, txt_two, ntxt_one, ntxt_two FROM %s WHERE id = :1",
		table,
	)
	// Case 3: Scan all six locators on one dedicated connection and close Rows.
	rows, err := conn.QueryContext(ctx, query, int64(1))
	if err != nil {
		t.Fatalf("query multiple LOBs: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("multiple-LOB query returned no row: %v", rows.Err())
	}
	destinations := make([]any, 0, len(testCases))
	for _, testCase := range testCases {
		destinations = append(destinations, testCase.destination)
	}
	if err := rows.Scan(destinations...); err != nil {
		t.Fatalf("scan multiple typed LOBs: %v", err)
	}
	if rows.Next() {
		t.Fatal("multiple-LOB query returned more than one row")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate multiple-LOB rows: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close multiple-LOB rows: %v", err)
	}

	// Case 4: Read each locator and verify it matches its corresponding column.
	for _, testCase := range testCases {
		if err := compareTypedLOBContents(t, ctx, testCase.read, testCase.expected); err != nil {
			t.Fatalf("read %s: %v", testCase.name, err)
		}
	}
}
