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

package lob

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"testing"

	internallob "github.com/oracle/go-oracledb/v26/internal/lob"
	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
)

type testAdopter struct {
	adoption internallob.PersistentLOBAdoption
	kind     internallob.Kind
	err      error
	gotKind  internallob.Kind
}

func (source *testAdopter) AdoptPersistentLocator(kind internallob.Kind) (internallob.PersistentLOBAdoption, error) {
	source.gotKind = kind
	if kind != source.kind {
		return internallob.PersistentLOBAdoption{}, errors.New("LOB kind mismatch")
	}
	return source.adoption, source.err
}

type testLOBSession struct {
	valid     bool
	data      []byte
	length    uint64
	opened    bool
	freeCalls int
}

type testConnector struct{ session *testLOBSession }

func (connector testConnector) Connect(context.Context) (driver.Conn, error) {
	return &testDriverConn{session: connector.session}, nil
}
func (testConnector) Driver() driver.Driver { return testDriver{} }

type testDriver struct{}

func (testDriver) Open(string) (driver.Conn, error) { return nil, errors.New("unexpected Open") }

type testDriverConn struct{ session *testLOBSession }

func (*testDriverConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unsupported") }
func (*testDriverConn) Close() error                        { return nil }
func (*testDriverConn) Begin() (driver.Tx, error)           { return nil, errors.New("unsupported") }
func (conn *testDriverConn) NewLOBSession() (internallob.LOBSession, error) {
	return conn.session, nil
}

func (conn *testDriverConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &testDriverRows{source: &testAdopter{kind: internallob.BLOB, adoption: internallob.PersistentLOBAdoption{
		Session: conn.session,
		Locator: []byte("locator"),
		Length:  uint64(len(conn.session.data)),
		Offset:  1,
	}}}, nil
}

type testDriverRows struct {
	source any
	done   bool
}

func (*testDriverRows) Columns() []string { return []string{"payload"} }
func (*testDriverRows) Close() error      { return nil }
func (rows *testDriverRows) Next(dest []driver.Value) error {
	if rows.done {
		return io.EOF
	}
	rows.done = true
	dest[0] = rows.source
	return nil
}

func (session *testLOBSession) ValidateLOBLease() error {
	if !session.valid {
		return errors.New("lease expired")
	}
	return nil
}

func (session *testLOBSession) AcquireLOBLease() (func(), error) {
	if err := session.ValidateLOBLease(); err != nil {
		return nil, err
	}
	return func() {}, nil
}

func (session *testLOBSession) LobCreate(context.Context, internallob.Kind) ([]byte, error) {
	return []byte("locator"), nil
}
func (session *testLOBSession) LobRead(_ context.Context, _ internallob.Kind, _ []byte, offset, amount uint64) ([]byte, uint64, error) {
	if offset == 0 || offset > uint64(len(session.data)) {
		return nil, 0, nil
	}
	end := offset - 1 + amount
	if end > uint64(len(session.data)) {
		end = uint64(len(session.data))
	}
	return append([]byte(nil), session.data[offset-1:end]...), end - (offset - 1), nil
}
func (session *testLOBSession) LobWrite(_ context.Context, kind internallob.Kind, _ []byte, offset uint64, data []byte) (uint64, error) {
	start := offset - 1
	if start > uint64(len(session.data)) {
		return 0, errors.New("write gap")
	}
	logical := uint64(len(data))
	if kind != internallob.BLOB {
		logical = utf16LogicalLength(data)
	}
	end := start + logical
	if end > uint64(len(session.data)) {
		session.data = append(session.data, make([]byte, end-uint64(len(session.data)))...)
	}
	copy(session.data[start:end], data)
	if end > session.length {
		session.length = end
	}
	return logical, nil
}
func (session *testLOBSession) LobLength(_ context.Context, kind internallob.Kind, _ []byte) (uint64, error) {
	if session.length != 0 || len(session.data) == 0 {
		return session.length, nil
	}
	if kind != internallob.BLOB {
		return utf16LogicalLength(session.data), nil
	}
	return uint64(len(session.data)), nil
}
func (session *testLOBSession) LobChunkSize(context.Context, internallob.Kind, []byte) (uint64, error) {
	return 8192, nil
}
func (session *testLOBSession) LobTrim(_ context.Context, _ internallob.Kind, _ []byte, length uint64) (uint64, error) {
	if length < uint64(len(session.data)) {
		session.data = session.data[:length]
	} else {
		session.data = append(session.data, make([]byte, length-uint64(len(session.data)))...)
	}
	session.length = length
	return length, nil
}
func (session *testLOBSession) LobOpen(context.Context, internallob.Kind, []byte, uint8) (bool, []byte, error) {
	wasOpen := session.opened
	session.opened = true
	return !wasOpen, []byte("locator-open"), nil
}
func (session *testLOBSession) LobClose(context.Context, internallob.Kind, []byte) ([]byte, error) {
	session.opened = false
	return []byte("locator-closed"), nil
}
func (session *testLOBSession) LobIsOpen(context.Context, internallob.Kind, []byte) (bool, error) {
	return session.opened, nil
}
func (session *testLOBSession) LobFree(context.Context, []byte) error {
	session.freeCalls++
	return nil
}
func (*testLOBSession) LobSessionKey() any { return "test-session" }

func adoptedSource(kind internallob.Kind, data string, prefix string, offset uint64) (*testAdopter, *testLOBSession) {
	session := &testLOBSession{valid: true, data: []byte(data)}
	return &testAdopter{kind: kind, adoption: internallob.PersistentLOBAdoption{
		Session: session,
		Locator: []byte("locator"),
		Length:  uint64(len(data)),
		Prefix:  []byte(prefix),
		Offset:  offset,
	}}, session
}

func TestBlobScanAndReadAllWithPrefetch(t *testing.T) {
	source, _ := adoptedSource(internallob.BLOB, "prefetched remainder", "prefetched ", uint64(len("prefetched ")+1))
	var blob Blob
	if err := blob.Scan(source); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if source.gotKind != internallob.BLOB {
		t.Fatalf("adopted kind = %v, want BLOB", source.gotKind)
	}
	if !blob.Valid() || blob.Kind() != KindBLOB {
		t.Fatalf("scanned blob valid=%v kind=%v", blob.Valid(), blob.Kind())
	}
	got, err := blob.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(got) != "prefetched remainder" {
		t.Fatalf("ReadAll() = %q, want %q", got, "prefetched remainder")
	}
	if _, err := blob.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("Read() after end error = %v, want EOF", err)
	}
}

func TestTypedScannerRejectsWrongKindAndSource(t *testing.T) {
	source, _ := adoptedSource(internallob.BLOB, "data", "", 1)
	var clob Clob
	if err := clob.Scan(source); err == nil {
		t.Fatal("Clob.Scan() unexpectedly accepted a BLOB locator")
	}
	if source.gotKind != internallob.CLOB {
		t.Fatalf("adopter kind = %v, want CLOB", source.gotKind)
	}
	if clob.Valid() {
		t.Fatal("wrong-kind scan left a valid Clob")
	}
	if err := clob.Scan([]byte("raw text")); err == nil {
		t.Fatal("Clob.Scan() unexpectedly accepted a plain byte slice")
	}
}

func TestTypedScannerNullAndExpiredLease(t *testing.T) {
	var blob Blob
	if err := blob.Scan(nil); err != nil {
		t.Fatalf("Scan(nil) error = %v", err)
	}
	if blob.Valid() || blob.Kind() != KindBLOB {
		t.Fatalf("NULL scan valid=%v kind=%v", blob.Valid(), blob.Kind())
	}
	source, session := adoptedSource(internallob.BLOB, "data", "", 1)
	if err := blob.Scan(source); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	session.valid = false
	if blob.Valid() {
		t.Fatal("Valid() = true after connection lease expired")
	}
	if _, err := blob.Read(make([]byte, 1)); err == nil {
		t.Fatal("Read() unexpectedly succeeded after connection lease expired")
	}
}

func TestPositionalReadsLeaveSequentialCursorUnchanged(t *testing.T) {
	source, _ := adoptedSource(internallob.BLOB, "abcdef", "", 1)
	var blob Blob
	if err := blob.Scan(source); err != nil {
		t.Fatalf("Blob.Scan() error = %v", err)
	}
	window := make([]byte, 3)
	if n, err := blob.ReadAt(window, 2); n != len(window) || err != nil || string(window) != "cde" {
		t.Fatalf("ReadAt() = (%d, %q, %v), want (3, cde, nil)", n, window, err)
	}
	all, err := blob.ReadAll()
	if err != nil || string(all) != "abcdef" {
		t.Fatalf("ReadAll() after ReadAt = (%q, %v), want (abcdef, nil)", all, err)
	}

	textSource, _ := adoptedSource(internallob.CLOB, "abcdef", "", 1)
	var clob Clob
	if err := clob.Scan(textSource); err != nil {
		t.Fatalf("Clob.Scan() error = %v", err)
	}
	text, units, err := clob.ReadTextAt(1, 3)
	if err != nil || units != 3 || string(text) != "bcd" {
		t.Fatalf("ReadTextAt() = (%q, %d, %v), want (bcd, 3, nil)", text, units, err)
	}
	allText, err := clob.ReadAll()
	if err != nil || allText != "abcdef" {
		t.Fatalf("ReadAll() after ReadTextAt = (%q, %v), want (abcdef, nil)", allText, err)
	}
}

func TestLOBMetadataAndServerLifecycle(t *testing.T) {
	source, _ := adoptedSource(internallob.BLOB, "abcdef", "", 1)
	var blob Blob
	if err := blob.Scan(source); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if size, err := blob.Size(); err != nil || size != 6 {
		t.Fatalf("Size() = (%d, %v), want (6, nil)", size, err)
	}
	if size, err := blob.ChunkSize(); err != nil || size != 8192 {
		t.Fatalf("ChunkSize() = (%d, %v), want (8192, nil)", size, err)
	}
	if err := blob.Trim(3); err != nil {
		t.Fatalf("Trim() error = %v", err)
	}
	if size, err := blob.Size(); err != nil || size != 3 {
		t.Fatalf("Size() after Trim() = (%d, %v), want (3, nil)", size, err)
	}
	if err := blob.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := blob.Close(); err == nil {
		t.Fatal("Close() unexpectedly succeeded while server-open")
	}
	if open, err := blob.IsOpen(); err != nil || !open {
		t.Fatalf("IsOpen() = (%v, %v), want (true, nil)", open, err)
	}
	if err := blob.CloseServer(); err != nil {
		t.Fatalf("CloseServer() error = %v", err)
	}
	if err := blob.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if blob.Valid() {
		t.Fatal("Valid() = true after Close()")
	}
}

func TestReadAtShortReadAndWriteTo(t *testing.T) {
	source, _ := adoptedSource(internallob.BLOB, "abcdef", "", 1)
	var blob Blob
	if err := blob.Scan(source); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	window := make([]byte, 4)
	if n, err := blob.ReadAt(window, 4); n != 2 || err != io.EOF || string(window[:n]) != "ef" {
		t.Fatalf("ReadAt() = (%d, %q, %v), want (2, ef, EOF)", n, window[:n], err)
	}
	var output bytes.Buffer
	if n, err := blob.WriteTo(&output); n != 6 || err != nil || output.String() != "abcdef" {
		t.Fatalf("WriteTo() = (%d, %q, %v), want (6, abcdef, nil)", n, output.String(), err)
	}
}

func TestWriteToRejectsNilWriter(t *testing.T) {
	source, _ := adoptedSource(internallob.BLOB, "payload", "", 1)
	var blob Blob
	if err := blob.Scan(source); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if _, err := blob.WriteTo(nil); err == nil {
		t.Fatal("WriteTo(nil) unexpectedly succeeded")
	} else {
		var sqlErr oracleErrors.SQLError
		if !errors.As(err, &sqlErr) || sqlErr.ErrorCode() != string(oracleErrors.InvalidLOBBuffer) {
			t.Fatalf("WriteTo(nil) error = %v, want error code %s", err, oracleErrors.InvalidLOBBuffer)
		}
	}
}

func TestClobReadAllAndWriteString(t *testing.T) {
	source, session := adoptedSource(internallob.CLOB, "héllo", "h", 2)
	var clob Clob
	if err := clob.Scan(source); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	got, err := clob.ReadAllContext(context.Background())
	if err != nil {
		t.Fatalf("ReadAllContext() error = %v", err)
	}
	if got != "héllo" {
		t.Fatalf("ReadAllContext() = %q, want %q", got, "héllo")
	}
	clob.handle.offset = 1
	if err := clob.handle.session.ValidateLOBLease(); err != nil {
		t.Fatal(err)
	}
	clob.handle.offset = 1
	if _, err := clob.WriteString("ok"); err != nil {
		t.Fatalf("WriteString() error = %v", err)
	}
	if string(session.data[:2]) != "ok" {
		t.Fatalf("written data = %q, want %q", session.data[:2], "ok")
	}
	if _, err := clob.Write([]byte{0xff}); err == nil {
		t.Fatal("Write() unexpectedly accepted invalid UTF-8")
	}
}

func TestNClobTextHelpersAndTemporaryConstructors(t *testing.T) {
	source, _ := adoptedSource(internallob.NCLOB, "hello", "", 1)
	var nclob NClob
	if err := nclob.Scan(source); err != nil {
		t.Fatalf("NClob.Scan() error = %v", err)
	}
	text, units, err := nclob.ReadTextAt(1, 3)
	if err != nil || units != 3 || string(text) != "ell" {
		t.Fatalf("NClob.ReadTextAt() = (%q, %d, %v), want (ell, 3, nil)", text, units, err)
	}
	all, err := nclob.ReadAll()
	if err != nil || all != "hello" {
		t.Fatalf("NClob.ReadAll() = (%q, %v), want (hello, nil)", all, err)
	}
	if _, _, err := nclob.ReadTextAt(-1, 1); err == nil {
		t.Fatal("ReadTextAt() unexpectedly accepted a negative offset")
	}

	for _, test := range []struct {
		name   string
		create func(context.Context, *sql.Conn) (io.Closer, error)
		write  func(io.Closer) (int, error)
		want   int64
	}{
		{
			name: "CLOB",
			create: func(ctx context.Context, conn *sql.Conn) (io.Closer, error) {
				return CreateTemporaryClob(ctx, conn)
			},
			write: func(value io.Closer) (int, error) {
				return value.(*Clob).WriteString("café😀")
			},
			want: 6,
		},
		{
			name: "NCLOB",
			create: func(ctx context.Context, conn *sql.Conn) (io.Closer, error) {
				return CreateTemporaryNClob(ctx, conn)
			},
			write: func(value io.Closer) (int, error) {
				return value.(*NClob).WriteString("雪🌏")
			},
			want: 3,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			session := &testLOBSession{valid: true}
			db := sql.OpenDB(testConnector{session: session})
			defer db.Close()
			conn, err := db.Conn(context.Background())
			if err != nil {
				t.Fatalf("DB.Conn() error = %v", err)
			}
			defer conn.Close()
			value, err := test.create(context.Background(), conn)
			if err != nil {
				t.Fatalf("create temporary %s: %v", test.name, err)
			}
			defer value.Close()
			if n, err := test.write(value); err != nil || n == 0 {
				t.Fatalf("WriteString() = (%d, %v)", n, err)
			}
			var size int64
			var freeErr error
			switch typed := value.(type) {
			case *Clob:
				size, err = typed.Size()
				freeErr = typed.Free()
			case *NClob:
				size, err = typed.Size()
				freeErr = typed.Free()
			}
			if err != nil || size != test.want {
				t.Fatalf("Size() = (%d, %v), want (%d, nil)", size, err, test.want)
			}
			if freeErr != nil {
				t.Fatalf("Free() error = %v", freeErr)
			}
		})
	}
}

func TestUTF16PrefixByteAccounting(t *testing.T) {
	if got, want := utf16LogicalLength([]byte("a😀b")), uint64(4); got != want {
		t.Fatalf("utf16LogicalLength() = %d, want %d", got, want)
	}
	if got, want := utf8BytesForLogicalPrefix([]byte("a😀b"), 3), len("a😀"); got != want {
		t.Fatalf("utf8BytesForLogicalPrefix() = %d, want %d", got, want)
	}
}

func TestCreateTemporaryBlobWithDedicatedConn(t *testing.T) {
	session := &testLOBSession{valid: true}
	db := sql.OpenDB(testConnector{session: session})
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("DB.Conn() error = %v", err)
	}
	defer conn.Close()

	blob, err := CreateTemporaryBlob(context.Background(), conn)
	if err != nil {
		t.Fatalf("CreateTemporaryBlob() error = %v", err)
	}
	if !blob.Valid() || !blob.IsTemporary() {
		t.Fatalf("created blob valid=%v temporary=%v", blob.Valid(), blob.IsTemporary())
	}
	if n, err := blob.Write([]byte("temporary")); err != nil || n != len("temporary") {
		t.Fatalf("Write() = (%d, %v), want (%d, nil)", n, err, len("temporary"))
	}
	if err := blob.Free(); err != nil {
		t.Fatalf("Free() error = %v", err)
	}
	if session.freeCalls != 1 {
		t.Fatalf("LobFree calls = %d, want 1", session.freeCalls)
	}
	if blob.Valid() {
		t.Fatal("Valid() = true after Free()")
	}
}

func TestDatabaseSQLRowsScanPublicBlob(t *testing.T) {
	session := &testLOBSession{valid: true, data: []byte("database/sql scanner")}
	db := sql.OpenDB(testConnector{session: session})
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("DB.Conn() error = %v", err)
	}
	defer conn.Close()
	rows, err := conn.QueryContext(context.Background(), "SELECT payload")
	if err != nil {
		t.Fatalf("QueryContext() error = %v", err)
	}
	if !rows.Next() {
		rows.Close()
		t.Fatalf("Rows.Next() = false, Err() = %v", rows.Err())
	}
	var blob Blob
	if err := rows.Scan(&blob); err != nil {
		rows.Close()
		t.Fatalf("Rows.Scan() error = %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("Rows.Close() error = %v", err)
	}
	got, err := blob.ReadAll()
	if err != nil || string(got) != "database/sql scanner" {
		t.Fatalf("Blob.ReadAll() = (%q, %v), want (database/sql scanner, nil)", got, err)
	}
}
