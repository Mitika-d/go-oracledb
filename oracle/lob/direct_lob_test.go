/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
**
** Subject to the condition set forth below, permission is hereby granted to any
** person obtaining a copy of this software, associated documentation and/or data
** (collectively the "Software"), free of charge and under any and all copyright
** rights in the Software, and to any and all patent rights owned or freely
** licensable by each licensor hereunder covering either (i) the unmodified
** Software as contributed to or provided by such licensor, or (ii) the Larger
** Works (as defined below), to deal in both
**
** (a) the Software, and
** (b) any piece of software and/or hardware listed in the lrgrwrks.txt file if
** one is included with the Software (each a "Larger Work" to which the Software
** is contributed by such licensors), and
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

package lob

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"testing"

	internallob "github.com/oracle/go-oracledb/v26/internal/lob"
	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
)

// TestDirectLOB_AcceptedWriteBytes verifies that write acknowledgements map to
// UTF-8 byte counts only at Unicode scalar boundaries.
func TestDirectLOB_AcceptedWriteBytes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		kind        Kind
		data        string
		logical     uint64
		wantBytes   int
		wantShort   bool
		wantInvalid bool
	}{
		{name: "blob complete", kind: BLOB, data: "abc", logical: 3, wantBytes: 3},
		{name: "blob short", kind: BLOB, data: "abc", logical: 2, wantBytes: 2, wantShort: true},
		{name: "blob over acknowledgement", kind: BLOB, data: "abc", logical: 4, wantInvalid: true},
		{name: "clob complete", kind: CLOB, data: "Aé中", logical: 3, wantBytes: len("Aé中")},
		{name: "clob short before supplementary character", kind: CLOB, data: "A🙂B", logical: 1, wantBytes: 1, wantShort: true},
		{name: "clob short after supplementary character", kind: CLOB, data: "A🙂B", logical: 3, wantBytes: len("A🙂"), wantShort: true},
		{name: "clob split supplementary character", kind: CLOB, data: "A🙂B", logical: 2, wantInvalid: true},
		{name: "clob over acknowledgement", kind: CLOB, data: "A🙂B", logical: 5, wantInvalid: true},
		{name: "nclob complete", kind: NCLOB, data: "🙂", logical: 2, wantBytes: len("🙂")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gotBytes, err := _acceptedWriteBytes(test.kind, []byte(test.data), test.logical)
			if gotBytes != test.wantBytes {
				t.Fatalf("accepted bytes = %d, want %d", gotBytes, test.wantBytes)
			}
			if test.wantInvalid {
				var sqlErr oracleErrors.SQLError
				if !errors.As(err, &sqlErr) || sqlErr.ErrorCode() != string(oracleErrors.InvalidLOBBuffer) {
					t.Fatalf("error = %v, want %s", err, oracleErrors.InvalidLOBBuffer)
				}
				return
			}
			if test.wantShort {
				if !errors.Is(err, io.ErrShortWrite) {
					t.Fatalf("error = %v, want io.ErrShortWrite", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("_acceptedWriteBytes returned error: %v", err)
			}
		})
	}
}

// TestDirectLOB_IsTemporary reports temporary status for temporary and
// persistent direct LOB handles without performing an RPC.
func TestDirectLOB_IsTemporary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		temporary bool
		want      bool
	}{
		{name: "temporary", temporary: true, want: true},
		{name: "persistent", temporary: false, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := &DirectLOB{temporary: test.temporary}
			if got := value.IsTemporary(); got != test.want {
				t.Fatalf("IsTemporary() = %t, want %t", got, test.want)
			}
		})
	}
}

// TestDirectLOB_OpenPersistentSerializesWithClose verifies that a source close
// cannot win while persistent-locator promotion is detaching the locator.
func TestDirectLOB_OpenPersistentSerializesWithClose(t *testing.T) {
	t.Parallel()

	source := &directLOBPromotionSource{
		testSource:    testSource{kind: BLOB},
		detachStarted: make(chan struct{}),
		allowDetach:   make(chan struct{}),
	}
	conn := newDirectLOBTestConn(t, &directLOBTestRawConn{sessionKey: "session"})
	var value LOB
	if err := value.Scan(source); err != nil {
		t.Fatalf("Scan returned error: %v", err)
	}

	type promotionResult struct {
		value *DirectLOB
		err   error
	}
	promotionDone := make(chan promotionResult, 1)
	go func() {
		promoted, err := OpenPersistent(context.Background(), conn, &value)
		promotionDone <- promotionResult{value: promoted, err: err}
	}()
	<-source.detachStarted

	closeDone := make(chan error, 1)
	go func() { closeDone <- value.Close() }()
	if value.mu.TryLock() {
		value.mu.Unlock()
		close(source.allowDetach)
		<-promotionDone
		t.Fatal("OpenPersistent did not hold the LOB state lock during detach")
	}

	close(source.allowDetach)
	result := <-promotionDone
	if result.err != nil || result.value == nil {
		t.Fatalf("OpenPersistent returned (%v, %v), want a promoted value", result.value, result.err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("LOB.Close returned error: %v", err)
	}
	if source.closeCalls != 0 {
		t.Fatalf("source Close calls = %d, want 0 after promotion", source.closeCalls)
	}
}

// TestDirectLOB_TrimClearsPendingCLOB verifies that successful trimming drops
// UTF-8 bytes prefetched before the server-side length changed.
func TestDirectLOB_TrimClearsPendingCLOB(t *testing.T) {
	t.Parallel()

	readCalls := 0
	raw := &directLOBTestRawConn{
		readFn: func(_ context.Context, _ uint8, _ []byte, _ uint64, _ uint64) ([]byte, uint64, error) {
			readCalls++
			if readCalls == 1 {
				return []byte("A🙂B"), 4, nil
			}
			return nil, 0, nil
		},
		trimFn: func(_ context.Context, _ uint8, _ []byte, length uint64) (uint64, error) {
			if length != 1 {
				t.Fatalf("Trim length = %d, want 1", length)
			}
			return length, nil
		},
	}
	conn := newDirectLOBTestConn(t, raw)
	value := &DirectLOB{
		conn:    conn,
		kind:    CLOB,
		locator: []byte("locator"),
		offset:  1,
	}

	first := make([]byte, 1)
	if n, err := value.ReadContext(context.Background(), first); n != 1 || err != nil || string(first) != "A" {
		t.Fatalf("first ReadContext = (%d, %v, %q), want (1, nil, A)", n, err, first)
	}
	if len(value.pending) == 0 {
		t.Fatal("CLOB read did not retain prefetched data")
	}
	if err := value.Trim(context.Background(), 1); err != nil {
		t.Fatalf("Trim returned error: %v", err)
	}
	if len(value.pending) != 0 {
		t.Fatalf("pending bytes after Trim = %q, want empty", value.pending)
	}

	if n, err := value.ReadContext(context.Background(), make([]byte, 8)); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("ReadContext after Trim = (%d, %v), want (0, io.EOF)", n, err)
	}
}

// TestDirectLOB_OpenRequiresCloseServerBeforeClose verifies an explicitly
// opened persistent LOB cannot be locally closed before server close succeeds.
func TestDirectLOB_OpenRequiresCloseServerBeforeClose(t *testing.T) {
	t.Parallel()

	raw := &directLOBTestRawConn{
		openFn: func(_ context.Context, _ uint8, locator []byte, _ uint8) (bool, []byte, error) {
			return true, locator, nil
		},
		closeFn: func(_ context.Context, _ uint8, locator []byte) ([]byte, error) {
			return locator, nil
		},
	}
	conn := newDirectLOBTestConn(t, raw)
	value := &DirectLOB{conn: conn, kind: BLOB, locator: []byte("locator"), offset: 1}

	if opened, err := value.Open(context.Background(), ReadWrite); !opened || err != nil {
		t.Fatalf("Open = (%t, %v), want (true, nil)", opened, err)
	}
	requireLOBTestErrorCode(t, value.Close(), oracleErrors.LobOpen)
	if value.closed {
		t.Fatal("Close marked the handle closed while server close was required")
	}
	if err := value.CloseServer(context.Background()); err != nil {
		t.Fatalf("CloseServer returned error: %v", err)
	}
	if err := value.Close(); err != nil {
		t.Fatalf("Close after CloseServer returned error: %v", err)
	}
}

// TestDirectLOB_OpenRequiresCloseServerBeforeFree verifies Free also preserves
// an explicitly opened persistent handle until CloseServer succeeds.
func TestDirectLOB_OpenRequiresCloseServerBeforeFree(t *testing.T) {
	t.Parallel()

	raw := &directLOBTestRawConn{
		openFn: func(_ context.Context, _ uint8, locator []byte, _ uint8) (bool, []byte, error) {
			return true, locator, nil
		},
		closeFn: func(_ context.Context, _ uint8, locator []byte) ([]byte, error) {
			return locator, nil
		},
	}
	conn := newDirectLOBTestConn(t, raw)
	value := &DirectLOB{conn: conn, kind: CLOB, locator: []byte("locator"), offset: 1}

	if opened, err := value.Open(context.Background(), ReadOnly); !opened || err != nil {
		t.Fatalf("Open = (%t, %v), want (true, nil)", opened, err)
	}
	requireLOBTestErrorCode(t, value.Free(context.Background()), oracleErrors.LobOpen)
	if value.closed || value.freed {
		t.Fatal("Free changed local state while server close was required")
	}
	if err := value.CloseServer(context.Background()); err != nil {
		t.Fatalf("CloseServer returned error: %v", err)
	}
	if err := value.Free(context.Background()); err != nil {
		t.Fatalf("Free after CloseServer returned error: %v", err)
	}
}

// TestDirectLOB_CloseServerFailurePreservesOpenState verifies a failed server
// close is retryable and does not permit local handle release in the interim.
func TestDirectLOB_CloseServerFailurePreservesOpenState(t *testing.T) {
	t.Parallel()

	closeCalls := 0
	closeErr := errors.New("server close failed")
	raw := &directLOBTestRawConn{
		openFn: func(_ context.Context, _ uint8, locator []byte, _ uint8) (bool, []byte, error) {
			return true, locator, nil
		},
		closeFn: func(_ context.Context, _ uint8, locator []byte) ([]byte, error) {
			closeCalls++
			if closeCalls == 1 {
				return nil, closeErr
			}
			return locator, nil
		},
	}
	conn := newDirectLOBTestConn(t, raw)
	value := &DirectLOB{conn: conn, kind: BLOB, locator: []byte("locator"), offset: 1}

	if opened, err := value.Open(context.Background(), ReadWrite); !opened || err != nil {
		t.Fatalf("Open = (%t, %v), want (true, nil)", opened, err)
	}
	if err := value.CloseServer(context.Background()); !errors.Is(err, closeErr) {
		t.Fatalf("first CloseServer error = %v, want %v", err, closeErr)
	}
	requireLOBTestErrorCode(t, value.Close(), oracleErrors.LobOpen)
	if err := value.CloseServer(context.Background()); err != nil {
		t.Fatalf("retry CloseServer returned error: %v", err)
	}
	if err := value.Close(); err != nil {
		t.Fatalf("Close after successful retry returned error: %v", err)
	}
	if closeCalls != 2 {
		t.Fatalf("CloseServer calls = %d, want 2", closeCalls)
	}
}

// TestDirectLOB_BlobReadCapsRefillAndConsumesPending verifies BLOB refills use
// the bounded TTC chunk and that prefetched bytes are served before another RPC.
func TestDirectLOB_BlobReadCapsRefillAndConsumesPending(t *testing.T) {
	t.Parallel()

	readCalls := 0
	var requested []uint64
	raw := &directLOBTestRawConn{
		readFn: func(_ context.Context, _ uint8, _ []byte, _ uint64, amount uint64) ([]byte, uint64, error) {
			readCalls++
			requested = append(requested, amount)
			data := make([]byte, int(amount))
			for index := range data {
				data[index] = 'x'
			}
			return data, amount, nil
		},
	}
	conn := newDirectLOBTestConn(t, raw)
	value := &DirectLOB{
		conn:    conn,
		kind:    BLOB,
		locator: []byte("locator"),
		offset:  1,
	}

	first := make([]byte, 1)
	if n, err := value.ReadContext(context.Background(), first); n != 1 || err != nil || string(first) != "x" {
		t.Fatalf("first ReadContext = (%d, %v, %q), want (1, nil, x)", n, err, first)
	}
	if len(requested) != 1 || requested[0] != uint64(internallob.DefaultBlobLobChunkBytes) {
		t.Fatalf("BLOB refill requests = %v, want one request of %d bytes", requested, internallob.DefaultBlobLobChunkBytes)
	}

	second := make([]byte, 2)
	if n, err := value.ReadContext(context.Background(), second); n != 2 || err != nil || string(second) != "xx" {
		t.Fatalf("second ReadContext = (%d, %v, %q), want (2, nil, xx)", n, err, second)
	}
	if readCalls != 1 {
		t.Fatalf("LobRead calls after consuming pending data = %d, want 1", readCalls)
	}
}

type directLOBPromotionSource struct {
	testSource
	detachStarted chan struct{}
	allowDetach   chan struct{}
}

func (source *directLOBPromotionSource) DetachPersistentLocator(any) ([]byte, error) {
	close(source.detachStarted)
	<-source.allowDetach
	return []byte("promoted-locator"), nil
}

type directLOBTestConnector struct {
	raw driver.Conn
}

func (connector directLOBTestConnector) Connect(context.Context) (driver.Conn, error) {
	return connector.raw, nil
}

func (connector directLOBTestConnector) Driver() driver.Driver {
	return directLOBTestDriver{raw: connector.raw}
}

type directLOBTestDriver struct {
	raw driver.Conn
}

func (driver directLOBTestDriver) Open(string) (driver.Conn, error) {
	return driver.raw, nil
}

func newDirectLOBTestConn(t *testing.T, raw *directLOBTestRawConn) *sql.Conn {
	t.Helper()
	db := sql.OpenDB(directLOBTestConnector{raw: raw})
	t.Cleanup(func() { _ = db.Close() })
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("DB.Conn returned error: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// directLOBTestRawConn embeds the private driver contract so each test only
// overrides the raw operation it exercises.
type directLOBTestRawConn struct {
	directLOBDriver
	sessionKey any
	readFn     func(context.Context, uint8, []byte, uint64, uint64) ([]byte, uint64, error)
	trimFn     func(context.Context, uint8, []byte, uint64) (uint64, error)
	openFn     func(context.Context, uint8, []byte, uint8) (bool, []byte, error)
	closeFn    func(context.Context, uint8, []byte) ([]byte, error)
}

func (driver *directLOBTestRawConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unused")
}

func (driver *directLOBTestRawConn) Close() error { return nil }

func (driver *directLOBTestRawConn) Begin() (driver.Tx, error) {
	return nil, errors.New("unused")
}

func (driver *directLOBTestRawConn) LobSessionKey() any {
	return driver.sessionKey
}

func (driver *directLOBTestRawConn) LobRead(ctx context.Context, kind uint8, locator []byte, offset, amount uint64) ([]byte, uint64, error) {
	if driver.readFn == nil {
		return nil, 0, errors.New("LobRead unused")
	}
	return driver.readFn(ctx, kind, locator, offset, amount)
}

func (driver *directLOBTestRawConn) LobTrim(ctx context.Context, kind uint8, locator []byte, length uint64) (uint64, error) {
	if driver.trimFn == nil {
		return 0, errors.New("LobTrim unused")
	}
	return driver.trimFn(ctx, kind, locator, length)
}

func (driver *directLOBTestRawConn) LobOpen(ctx context.Context, kind uint8, locator []byte, mode uint8) (bool, []byte, error) {
	if driver.openFn == nil {
		return false, nil, errors.New("LobOpen unused")
	}
	return driver.openFn(ctx, kind, locator, mode)
}

func (driver *directLOBTestRawConn) LobClose(ctx context.Context, kind uint8, locator []byte) ([]byte, error) {
	if driver.closeFn == nil {
		return nil, errors.New("LobClose unused")
	}
	return driver.closeFn(ctx, kind, locator)
}
