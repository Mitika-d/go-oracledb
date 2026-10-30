/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
**
** Subject to the condition set forth below, permission is hereby granted to any
** person obtaining a copy of this software, associated documentation and/or data
** (collectively the "Software"), free of charge and under any and all copyright
** rights in the Software, and under any and all patent rights owned or freely
** licensable by each licensor hereunder covering either (i) the unmodified
** Software as contributed to or provided by such licensor, or (ii) the Larger
** Works (as defined below) to deal in both
**
** (a) the Software, and
** (b) any piece of software and/or hardware listed in the lrgrwrks.txt file if
** one is included with the Software (each a "Larger Work" to which the Software
** is contributed by such licensors),
**
** without restriction, including without limitation the rights to copy, create
** derivative works of, display, perform, and distribute the Software and make,
** use, sell, offer for sale, import, export, have made, and have sold the Software
** and the Larger Work(s), and to sublicense the foregoing rights on either these
** or other terms.
**
** This license is subject to the following condition:
** The above copyright notice and either this complete permission notice or at
** a minimum a reference to the UPL must be included in all copies or substantial
** portions of the Software.
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
	"testing"

	"github.com/oracle/go-oracledb/v26/oracle/datatype/lob"
	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
)

// TestDriver_TypedLOBQueryRowScans verifies typed LOB scans with a 128-byte
// prefetch.
//
// Setup and read:
//   - Insert BLOB as []byte and CLOB/NCLOB as strings.
//   - Select with QueryRowContext and scan into lob.Blob, Clob, NClob.
//   - Read each value with ReadContext.
//
// Cases:
//   - Non-NULL LOB values that fit in the prefetch.
//   - SQL NULL scans to invalid typed LOB values.
//   - A CLOB locator cannot scan into a BLOB destination.
func TestDriver_TypedLOBQueryRowScans(t *testing.T) {
	t.Parallel()
	const prefetchSize = 128
	db := openLOBFunctionalDBWithPrefetch(t, prefetchSize)
	ctx := context.Background()
	table := createObjectName("lob_query")
	if err := createTable(ctx, db, table, map[string]string{
		"id": "NUMBER PRIMARY KEY", "bin": "BLOB", "txt": "CLOB", "ntxt": "NCLOB",
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	deferTableCleanup(t, db, table)

	blobWant := []byte{0, 1, 127, 128, 254, 255}
	clobWant := "CLOB café 🙂"
	nclobWant := "NCLOB नमस्ते 🌏"
	// Keep these fixtures below the prefetch size so this case exercises reads
	// entirely from the prefetched values.
	if len(blobWant) >= prefetchSize || len(clobWant) >= prefetchSize || len(nclobWant) >= prefetchSize {
		t.Fatalf("test fixtures must fit within the %d-byte prefetch", prefetchSize)
	}

	// Pin one Oracle session for inserts and the locator reads below.
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("get dedicated connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	insert := fmt.Sprintf("INSERT INTO %s (id, bin, txt, ntxt) VALUES (:1, :2, :3, :4)", table)
	if _, err := conn.ExecContext(ctx, insert, int64(1), blobWant, clobWant, nclobWant); err != nil {
		t.Fatalf("insert typed LOBs: %v", err)
	}
	insertNull := fmt.Sprintf("INSERT INTO %s (id, bin, txt, ntxt) VALUES (:1, NULL, NULL, NULL)", table)
	if _, err := conn.ExecContext(ctx, insertNull, int64(2)); err != nil {
		t.Fatalf("insert SQL NULL LOBs: %v", err)
	}

	var blob lob.Blob
	var clob lob.Clob
	var nclob lob.NClob
	t.Cleanup(func() { _ = blob.Close() })
	t.Cleanup(func() { _ = clob.Close() })
	t.Cleanup(func() { _ = nclob.Close() })
	query := fmt.Sprintf("SELECT bin, txt, ntxt FROM %s WHERE id = :1", table)
	if err := conn.QueryRowContext(ctx, query, int64(1)).Scan(&blob, &clob, &nclob); err != nil {
		t.Fatalf("QueryRow.Scan typed LOBs: %v", err)
	}
	// Case 1: Scan and read non-NULL BLOB, CLOB, and NCLOB values.
	testCases := []struct {
		name         string
		valid        func() bool
		kind         func() lob.Kind
		wantKind     lob.Kind
		read         func(context.Context, []byte) (int, error)
		wantContents []byte
	}{
		{"BLOB", blob.Valid, blob.Kind, lob.KindBLOB, blob.ReadContext, blobWant},
		{"CLOB", clob.Valid, clob.Kind, lob.KindCLOB, clob.ReadContext, []byte(clobWant)},
		{"NCLOB", nclob.Valid, nclob.Kind, lob.KindNCLOB, nclob.ReadContext, []byte(nclobWant)},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			valid, kind := testCase.valid(), testCase.kind()
			if !valid || kind != testCase.wantKind {
				t.Fatalf("metadata = (valid %t, kind %v), want (true, %v)", valid, kind, testCase.wantKind)
			}
			got := readTypedLOBContents(t, ctx, testCase.read)
			if !bytes.Equal(got, testCase.wantContents) {
				t.Fatalf("QueryRow %s = %q, want %q", testCase.name, got, testCase.wantContents)
			}
		})
	}

	// Case 2: Scanning SQL NULL leaves typed LOB destinations invalid.
	var nullBlob lob.Blob
	var nullClob lob.Clob
	var nullNclob lob.NClob
	if err := conn.QueryRowContext(ctx, query, int64(2)).Scan(&nullBlob, &nullClob, &nullNclob); err != nil {
		t.Fatalf("QueryRow.Scan SQL NULL LOBs: %v", err)
	}
	if nullBlob.Valid() || nullClob.Valid() || nullNclob.Valid() {
		t.Fatal("typed LOB destinations from SQL NULL should be invalid")
	}

	// Case 3: A CLOB locator cannot be scanned into a BLOB destination.
	var wrongKind lob.Blob
	t.Cleanup(func() { _ = wrongKind.Close() })
	wrongKindQuery := fmt.Sprintf("SELECT txt FROM %s WHERE id = :1", table)
	if err := conn.QueryRowContext(ctx, wrongKindQuery, int64(1)).Scan(&wrongKind); err == nil {
		t.Fatal("Blob.Scan() unexpectedly accepted a CLOB locator")
	}
}

// typedLOBGetterCase holds the shared public LOB value and expected metadata.
type typedLOBGetterCase struct {
	name      string
	value     *lob.LOB
	kind      lob.Kind
	size      int64
	temporary bool
}

// checkTypedLOBGetterAPIs calls the public getters shared by typed LOB values.
func checkTypedLOBGetterAPIs(t *testing.T, ctx context.Context, testCases []typedLOBGetterCase) {
	t.Helper()
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			valid, kind, temporary := testCase.value.Valid(), testCase.value.Kind(), testCase.value.IsTemporary()
			if !valid || kind != testCase.kind || temporary != testCase.temporary {
				t.Fatalf("metadata = (valid %t, kind %v, temporary %t), want (true, %v, %t)",
					valid, kind, temporary, testCase.kind, testCase.temporary)
			}
			if open, err := testCase.value.IsOpen(); err != nil || open {
				t.Fatalf("IsOpen() = (%t, %v), want (false, nil)", open, err)
			}
			if open, err := testCase.value.IsOpenContext(ctx); err != nil || open {
				t.Fatalf("IsOpenContext() = (%t, %v), want (false, nil)", open, err)
			}
			if size, err := testCase.value.Size(); err != nil || size != testCase.size {
				t.Fatalf("Size() = (%d, %v), want (%d, nil)", size, err, testCase.size)
			}
			if size, err := testCase.value.SizeContext(ctx); err != nil || size != testCase.size {
				t.Fatalf("SizeContext() = (%d, %v), want (%d, nil)", size, err, testCase.size)
			}
			chunkSize, err := testCase.value.ChunkSize()
			if err != nil || chunkSize <= 0 {
				t.Fatalf("ChunkSize() = (%d, %v), want a positive size", chunkSize, err)
			}
			if got, err := testCase.value.ChunkSizeContext(ctx); err != nil || got != chunkSize {
				t.Fatalf("ChunkSizeContext() = (%d, %v), want (%d, nil)", got, err, chunkSize)
			}
		})
	}
}

// TestDriver_TypedLOBPersistentGetterAPIs checks the public getters on
// persistent BLOB, CLOB, and NCLOB locators.
//
// Setup and scan:
//   - Insert BLOB as []byte and CLOB/NCLOB as strings.
//   - Scan the row with QueryRowContext into the typed LOB values.
//
// Cases:
//   - Valid is true, Kind matches, and IsTemporary is false.
//   - IsOpen and IsOpenContext both report false.
//   - Size and SizeContext match the expected LOB lengths.
//   - ChunkSize is positive and ChunkSizeContext returns the same value.
func TestDriver_TypedLOBPersistentGetterAPIs(t *testing.T) {
	t.Parallel()
	db := openLOBFunctionalDBWithPrefetch(t, 8)
	ctx := context.Background()
	table := createObjectName("lob_getters")
	if err := createTable(ctx, db, table, map[string]string{
		"id": "NUMBER PRIMARY KEY", "bin": "BLOB", "txt": "CLOB", "ntxt": "NCLOB",
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	deferTableCleanup(t, db, table)

	blobWant := []byte("persistent BLOB")
	clobWant := "persistent CLOB café 🙂"
	nclobWant := "persistent NCLOB नमस्ते 🌏"
	insert := fmt.Sprintf("INSERT INTO %s (id, bin, txt, ntxt) VALUES (:1, :2, :3, :4)", table)
	if _, err := db.ExecContext(ctx, insert, int64(1), blobWant, clobWant, nclobWant); err != nil {
		t.Fatalf("insert persistent LOBs: %v", err)
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("get dedicated connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	var persistentBlob lob.Blob
	var persistentClob lob.Clob
	var persistentNclob lob.NClob
	t.Cleanup(func() { _ = persistentBlob.Close() })
	t.Cleanup(func() { _ = persistentClob.Close() })
	t.Cleanup(func() { _ = persistentNclob.Close() })
	query := fmt.Sprintf("SELECT bin, txt, ntxt FROM %s WHERE id = :1", table)
	if err := conn.QueryRowContext(ctx, query, int64(1)).Scan(&persistentBlob, &persistentClob, &persistentNclob); err != nil {
		t.Fatalf("scan persistent LOBs: %v", err)
	}

	testCases := []typedLOBGetterCase{
		{"persistent BLOB", &persistentBlob.LOB, lob.KindBLOB, int64(len(blobWant)), false},
		{"persistent CLOB", &persistentClob.LOB, lob.KindCLOB, functionalLOBTextSize(clobWant), false},
		{"persistent NCLOB", &persistentNclob.LOB, lob.KindNCLOB, functionalLOBTextSize(nclobWant), false},
	}
	checkTypedLOBGetterAPIs(t, ctx, testCases)
}

// TestDriver_TypedLOBTemporaryGetterAPIs checks the public getters on temporary
// BLOB, CLOB, and NCLOB values.
//
// Setup and write:
//   - Create each temporary LOB on a dedicated connection.
//   - Write a small payload through the public Write or WriteStringContext API.
//
// Cases:
//   - Valid is true, Kind matches, and IsTemporary is true.
//   - IsOpen and IsOpenContext both report false.
//   - Size and SizeContext match the expected LOB lengths.
//   - ChunkSize is positive and ChunkSizeContext returns the same value.
func TestDriver_TypedLOBTemporaryGetterAPIs(t *testing.T) {
	t.Parallel()
	db := openLOBFunctionalDBWithPrefetch(t, 8)
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("get dedicated connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	blobWant := []byte("temporary BLOB")
	clobWant := "temporary CLOB café 🙂"
	nclobWant := "temporary NCLOB नमस्ते 🌏"
	temporaryBlob, err := lob.CreateTemporaryBlob(ctx, conn)
	if err != nil {
		t.Fatalf("create temporary BLOB: %v", err)
	}
	t.Cleanup(func() {
		if temporaryBlob.Valid() {
			if err := temporaryBlob.FreeContext(ctx); err != nil {
				t.Errorf("free temporary BLOB: %v", err)
			}
		}
	})
	if n, err := temporaryBlob.Write(blobWant); err != nil || n != len(blobWant) {
		t.Fatalf("write temporary BLOB = (%d, %v), want (%d, nil)", n, err, len(blobWant))
	}
	temporaryClob, err := lob.CreateTemporaryClob(ctx, conn)
	if err != nil {
		t.Fatalf("create temporary CLOB: %v", err)
	}
	t.Cleanup(func() {
		if temporaryClob.Valid() {
			if err := temporaryClob.FreeContext(ctx); err != nil {
				t.Errorf("free temporary CLOB: %v", err)
			}
		}
	})
	if n, err := temporaryClob.WriteStringContext(ctx, clobWant); err != nil || n != len(clobWant) {
		t.Fatalf("write temporary CLOB = (%d, %v), want (%d, nil)", n, err, len(clobWant))
	}
	temporaryNclob, err := lob.CreateTemporaryNClob(ctx, conn)
	if err != nil {
		t.Fatalf("create temporary NCLOB: %v", err)
	}
	t.Cleanup(func() {
		if temporaryNclob.Valid() {
			if err := temporaryNclob.FreeContext(ctx); err != nil {
				t.Errorf("free temporary NCLOB: %v", err)
			}
		}
	})
	if n, err := temporaryNclob.WriteStringContext(ctx, nclobWant); err != nil || n != len(nclobWant) {
		t.Fatalf("write temporary NCLOB = (%d, %v), want (%d, nil)", n, err, len(nclobWant))
	}

	testCases := []typedLOBGetterCase{
		{"temporary BLOB", &temporaryBlob.LOB, lob.KindBLOB, int64(len(blobWant)), true},
		{"temporary CLOB", &temporaryClob.LOB, lob.KindCLOB, functionalLOBTextSize(clobWant), true},
		{"temporary NCLOB", &temporaryNclob.LOB, lob.KindNCLOB, functionalLOBTextSize(nclobWant), true},
	}
	checkTypedLOBGetterAPIs(t, ctx, testCases)
}

// TestDriver_TypedLOBArgumentValidation checks errors for invalid temporary
// LOB constructor and operation arguments.
//
// Cases:
//   - Reject a nil connection when creating a temporary BLOB.
//   - Reject a negative TrimContext length and a nil WriteToContext writer.
//   - Reject invalid UTF-8 passed to a CLOB.
func TestDriver_TypedLOBArgumentValidation(t *testing.T) {
	t.Parallel()
	if TestingConfig == nil {
		t.Skip("No configuration available")
	}

	// Case 1: Creating a temporary BLOB requires a connection.
	t.Run("nil_connection", func(t *testing.T) {
		if _, err := lob.CreateTemporaryBlob(context.Background(), nil); err == nil {
			t.Fatal("CreateTemporaryBlob accepted a nil connection")
		} else {
			assertLOBErrorCode(t, oracleErrors.InvalidConnection, err)
		}
	})

	db, err := openTestDBWithConfig(TestingConfig)
	if err != nil {
		t.Fatalf("open test DB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("open dedicated connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	blob, err := lob.CreateTemporaryBlob(ctx, conn)
	if err != nil {
		t.Fatalf("create temporary BLOB: %v", err)
	}
	t.Cleanup(func() { _ = blob.FreeContext(ctx) })
	if _, err := blob.WriteContext(ctx, []byte("payload")); err != nil {
		t.Fatalf("write temporary BLOB: %v", err)
	}

	// Case 2: TrimContext rejects a negative length.
	t.Run("negative_trim_length", func(t *testing.T) {
		if err := blob.TrimContext(ctx, -1); err == nil {
			t.Fatal("TrimContext accepted a negative length")
		} else {
			assertLOBErrorCode(t, oracleErrors.InvalidLOBBuffer, err)
		}
	})

	// Case 3: WriteToContext requires a writer.
	t.Run("nil_writer", func(t *testing.T) {
		if _, err := blob.WriteToContext(ctx, nil); err == nil {
			t.Fatal("WriteToContext accepted a nil writer")
		} else {
			assertLOBErrorCode(t, oracleErrors.InvalidLOBBuffer, err)
		}
	})

	clob, err := lob.CreateTemporaryClob(ctx, conn)
	if err != nil {
		t.Fatalf("create temporary CLOB: %v", err)
	}
	t.Cleanup(func() { _ = clob.FreeContext(ctx) })

	// Case 4: CLOB writes reject invalid UTF-8.
	t.Run("invalid_clob_utf8", func(t *testing.T) {
		if _, err := clob.WriteContext(ctx, []byte{0xff}); err == nil {
			t.Fatal("CLOB WriteContext accepted invalid UTF-8")
		} else {
			assertLOBErrorCode(t, oracleErrors.InvalidLobInput, err)
		}
	})
}

// TestDriver_TypedLOBTemporaryOpenCloseTrim checks temporary BLOB, CLOB, and
// NCLOB lifecycle operations on a dedicated connection.
//
// Cases:
//   - BLOB writes bytes with Write; CLOB and NCLOB write strings.
//   - Each case opens and closes the server LOB, checks both IsOpen variants,
//     trims the value, checks its size, frees it, and verifies Valid is false.
func TestDriver_TypedLOBTemporaryOpenCloseTrim(t *testing.T) {
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
		t.Fatalf("open dedicated connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	blobPayload := []byte("temporary BLOB payload")
	blob, err := lob.CreateTemporaryBlob(ctx, conn)
	if err != nil {
		t.Fatalf("create temporary BLOB: %v", err)
	}
	t.Cleanup(func() {
		if blob.Valid() {
			if err := blob.FreeContext(ctx); err != nil {
				t.Errorf("free temporary BLOB: %v", err)
			}
		}
	})

	clobPayload := "temporary CLOB payload"
	clob, err := lob.CreateTemporaryClob(ctx, conn)
	if err != nil {
		t.Fatalf("create temporary CLOB: %v", err)
	}
	t.Cleanup(func() {
		if clob.Valid() {
			if err := clob.FreeContext(ctx); err != nil {
				t.Errorf("free temporary CLOB: %v", err)
			}
		}
	})

	nclobPayload := "temporary NCLOB payload"
	nclob, err := lob.CreateTemporaryNClob(ctx, conn)
	if err != nil {
		t.Fatalf("create temporary NCLOB: %v", err)
	}
	t.Cleanup(func() {
		if nclob.Valid() {
			if err := nclob.FreeContext(ctx); err != nil {
				t.Errorf("free temporary NCLOB: %v", err)
			}
		}
	})

	testCases := []struct {
		name        string
		value       *lob.LOB
		payloadSize int
		write       func() (int, error)
	}{
		// Case 1: BLOB writes its payload as bytes.
		{
			name: "BLOB", value: &blob.LOB, payloadSize: len(blobPayload),
			write: func() (int, error) { return blob.Write(blobPayload) },
		},
		// Case 2: CLOB writes its payload as text.
		{
			name: "CLOB", value: &clob.LOB, payloadSize: len(clobPayload),
			write: func() (int, error) { return clob.WriteStringContext(ctx, clobPayload) },
		},
		// Case 3: NCLOB writes national-character text.
		{
			name: "NCLOB", value: &nclob.LOB, payloadSize: len(nclobPayload),
			write: func() (int, error) { return nclob.WriteStringContext(ctx, nclobPayload) },
		},
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			if n, err := testCase.write(); err != nil || n != testCase.payloadSize {
				t.Fatalf("write temporary %s = (%d, %v), want (%d, nil)", testCase.name, n, err, testCase.payloadSize)
			}
			if err := testCase.value.OpenContext(ctx); err != nil {
				t.Fatalf("open temporary %s: %v", testCase.name, err)
			}
			if open, err := testCase.value.IsOpen(); err != nil || !open {
				t.Fatalf("%s IsOpen() = (%t, %v), want (true, nil)", testCase.name, open, err)
			}
			if open, err := testCase.value.IsOpenContext(ctx); err != nil || !open {
				t.Fatalf("%s IsOpenContext() = (%t, %v), want (true, nil)", testCase.name, open, err)
			}
			if err := testCase.value.CloseServerContext(ctx); err != nil {
				t.Fatalf("close temporary %s: %v", testCase.name, err)
			}
			if open, err := testCase.value.IsOpen(); err != nil || open {
				t.Fatalf("%s IsOpen() after close = (%t, %v), want (false, nil)", testCase.name, open, err)
			}
			if open, err := testCase.value.IsOpenContext(ctx); err != nil || open {
				t.Fatalf("%s IsOpenContext() after close = (%t, %v), want (false, nil)", testCase.name, open, err)
			}
			trimSize := int64(testCase.payloadSize - 1)
			if err := testCase.value.TrimContext(ctx, trimSize); err != nil {
				t.Fatalf("trim temporary %s: %v", testCase.name, err)
			}
			if size, err := testCase.value.SizeContext(ctx); err != nil || size != trimSize {
				t.Fatalf("%s SizeContext() after trim = (%d, %v), want (%d, nil)", testCase.name, size, err, trimSize)
			}
			if err := testCase.value.FreeContext(ctx); err != nil {
				t.Fatalf("free temporary %s: %v", testCase.name, err)
			}
			if testCase.value.Valid() {
				t.Fatalf("%s Valid() = true after FreeContext()", testCase.name)
			}
		})
	}
}
