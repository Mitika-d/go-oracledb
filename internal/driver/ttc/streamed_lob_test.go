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

package ttc

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	driverCommon "github.com/oracle/go-oracledb/v26/internal/driver/common"
	internallob "github.com/oracle/go-oracledb/v26/internal/lob"
	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
)

// lobEventRecorder records event delivery for tests in this package.
type lobEventRecorder struct {
	events []eventType
}

// streamedLobWriteFunc adapts a small write callback for WriterTo tests.
type streamedLobWriteFunc func([]byte) (int, error)

// Write delegates one WriterTo write to the configured test behavior.
func (writer streamedLobWriteFunc) Write(data []byte) (int, error) {
	return writer(data)
}

// notify implements EventListener.
func (listener *lobEventRecorder) notify(event eventType) {
	listener.events = append(listener.events, event)
}

// newLobTestRows constructs the minimum live Rows owner needed by a streamed LOB.
func newLobTestRows() *ttcRows {
	rows := newTTCRows(nil)
	rows.shelf = newShelf[driverCommon.MessageType]()
	rows.setContext(context.Background())
	return rows
}

// newTestStreamedLobManager constructs a concrete LOB manager backed by the
// deterministic TTC fixture factory and streamer used by executor tests.
func newTestStreamedLobManager(t *testing.T, rows *ttcRows) (*lobManager, *fakeStreamer) {
	t.Helper()
	fixtureShelf, _, _ := newLobTestShelf(8192)
	rowsShelf := rows.shelf
	if rowsShelf == nil {
		t.Fatal("streamed LOB test owner has no TTC shelf")
	}
	rowsShelf.RegisterMessageFactory(fixtureShelf.GetMessageFactory())
	streamer := &fakeStreamer{
		events:    make([]driverCommon.Message[driverCommon.MessageType], 0),
		preHooks:  make(map[driverCommon.MessageType]StreamerPreUnmarshallCallback),
		postHooks: make(map[driverCommon.MessageType]StreamerPostUnmarshallCallback),
	}
	rowsShelf.RegisterMessageStreamer(streamer)
	manager := newLobManager(rowsShelf, newTestSessionContext())
	return manager, streamer
}

// appendTestLobResponse appends one deterministic OLOBOPS response to the
// fake stream. payload is a raw BLOB or encoded CLOB response, while logical
// is the server-reported Oracle amount.
func appendTestLobResponse(streamer *fakeStreamer, payload []byte, logical driverCommon.UB8, terminalErr error) {
	if payload != nil {
		streamer.events = append(streamer.events, newTTIlobd())
		streamer.lobdPayloads = append(streamer.lobdPayloads, payload)
	}
	streamer.events = append(streamer.events, newTTILobRPA(), &mockOer{err: terminalErr})
	streamer.lobRpaAmounts = append(streamer.lobRpaAmounts, logical)
}

// closeRowsBeforeRelease asserts Rows.Close returns while the fake LOB RPC is
// blocked, then releases the RPC so a failing test cannot strand its goroutine.
func closeRowsBeforeRelease(t *testing.T, rows *ttcRows, release chan struct{}) {
	t.Helper()
	closeDone := make(chan error, 1)
	go func() { closeDone <- rows.Close() }()
	select {
	case err := <-closeDone:
		close(release)
		if err != nil {
			t.Fatalf("Rows.Close returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("Rows.Close blocked while a LOB RPC was stalled")
	}
}

// TestStreamedLob_BoundedClobReadAmountUsesCharacterChunk verifies that CLOB
// refill amounts are capped in Oracle character units.
func TestStreamedLob_BoundedClobReadAmountUsesCharacterChunk(t *testing.T) {
	t.Parallel()

	wantMaximum := driverCommon.UB8(DefaultCharacterLobChunkChars)
	for _, test := range []struct {
		name   string
		amount driverCommon.UB8
		want   driverCommon.UB8
	}{
		{name: "clamped to maximum", amount: wantMaximum * 2, want: wantMaximum},
		{name: "small amount unchanged", amount: 3, want: 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := boundedClobReadAmount(test.amount); got != test.want {
				t.Fatalf("bounded amount = %d, want %d", got, test.want)
			}
		})
	}
}

func newTestStreamedBlob(t *testing.T, rows *ttcRows, length driverCommon.UB8, prefix []byte) (*streamedLob, *fakeStreamer) {
	manager, streamer := newTestStreamedLobManager(t, rows)
	return &streamedLob{
		owner:       rows,
		kind:        internallob.BLOB,
		manager:     manager,
		locator:     newLocator(newTestLocator(false), 1),
		totalLength: length,
		prefix:      prefix,
		nextOffset:  driverCommon.UB8(len(prefix) + 1),
	}, streamer
}

// newTestStreamedClob constructs a CLOB-shaped LOB for lifecycle and
// refill tests backed by the deterministic TTC fixture streamer.
func newTestStreamedClob(t *testing.T, rows *ttcRows, length driverCommon.UB8, prefix []byte) (*streamedLob, *fakeStreamer) {
	manager, streamer := newTestStreamedLobManager(t, rows)
	return &streamedLob{
		owner:       rows,
		kind:        internallob.CLOB,
		manager:     manager,
		locator:     newLocator(newTestLocator(false), 1),
		totalLength: length,
		prefix:      prefix,
		nextOffset:  1,
	}, streamer
}

// TestStreamedLob_AdoptPersistentLocatorTransfersRowsOwnership verifies a
// typed scanner can keep a persistent locator usable after Rows.Close while
// the owning physical-session lease remains active.
func TestStreamedLob_AdoptPersistentLocatorTransfersRowsOwnership(t *testing.T) {
	t.Parallel()
	rows := newLobTestRows()
	rows.setSessionContext(newTestSessionContext())
	value, _ := newTestStreamedBlob(t, rows, 8, []byte("pref"))

	adoption, err := value.AdoptPersistentLocator(internallob.BLOB)
	if err != nil {
		t.Fatalf("AdoptPersistentLocator() error = %v", err)
	}
	if string(adoption.Prefix) != "pref" || adoption.Offset != 5 {
		t.Fatalf("adopted prefix/offset = (%q, %d), want (%q, 5)", adoption.Prefix, adoption.Offset, "pref")
	}
	if !value.closed {
		t.Fatal("streamed locator remained open after adoption")
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("Rows.Close() error = %v", err)
	}
	if err := adoption.Session.ValidateLOBLease(); err != nil {
		t.Fatalf("adopted lease invalid after Rows.Close(): %v", err)
	}
	rows.shelf.lobState.expireLeases()
	if err := adoption.Session.ValidateLOBLease(); err == nil {
		t.Fatal("adopted lease remained valid after physical connection lease expired")
	}
}

// TestStreamedLob_ReadStreamsPrefixAndLocatorChunks verifies that Read serves
// inline BLOB data first and then uses a length-bounded locator refill.
func TestStreamedLob_ReadStreamsPrefixAndLocatorChunks(t *testing.T) {
	t.Parallel()
	rows := newLobTestRows()
	value, streamer := newTestStreamedBlob(t, rows, 6, []byte("ab"))
	var offset driverCommon.UB8
	var amount driverCommon.UB8
	streamer.onPush = func(def *lobDefinition) {
		offset = def.sourceLocator.offset
		amount = def.lobAmt
	}
	appendTestLobResponse(streamer, []byte("cdef"), 4, nil)

	first := make([]byte, 1)
	if n, err := value.Read(first); n != 1 || err != nil || string(first) != "a" {
		t.Fatalf("first Read = (%d, %v, %q), want (1, nil, a)", n, err, first)
	}
	if len(streamer.pushed) != 0 {
		t.Fatalf("prefix Read sent %d TTC requests, want none", len(streamer.pushed))
	}
	var rest bytes.Buffer
	if _, err := value.WriteTo(&rest); err != nil || rest.String() != "bcdef" {
		t.Fatalf("WriteTo = (%q, %v), want (bcdef, nil)", rest.String(), err)
	}
	if offset != 3 || amount != 4 {
		t.Fatalf("RPC request = (offset %d, amount %d), want (3, 4)", offset, amount)
	}
}

// TestStreamedLob_InlinePrefixCannotExceedDeclaredLength verifies that equal
// inline prefixes are accepted while oversized BLOB, CLOB, and NCLOB prefixes
// are rejected before the value becomes usable.
func TestStreamedLob_InlinePrefixCannotExceedDeclaredLength(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		dtype  DtyType
		form   driverCommon.UB1
		prefix []byte
		length driverCommon.UB8
		wantOK bool
	}{
		{name: "BLOB equal", dtype: DtyBlob, prefix: []byte("abc"), length: 3, wantOK: true},
		{name: "BLOB overflow", dtype: DtyBlob, prefix: []byte("abc"), length: 2},
		{name: "CLOB equal", dtype: DtyClob, form: FormChar, prefix: []byte("A🙂"), length: 3, wantOK: true},
		{name: "CLOB overflow", dtype: DtyClob, form: FormChar, prefix: []byte("A🙂"), length: 2},
		{name: "NCLOB equal", dtype: DtyClob, form: FormNChar, prefix: []byte{0xD8, 0x3D, 0xDE, 0x42}, length: 2, wantOK: true},
		{name: "NCLOB overflow", dtype: DtyClob, form: FormNChar, prefix: []byte{0xD8, 0x3D, 0xDE, 0x42}, length: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rows := newLobTestRows()
			rows.sessionContext = driverCommon.NewSessionContext()
			rows.sessionContext.SetSessionCharacterSets(al32Utf8CharSet, al16Utf16CharSet)
			locator := make(driverCommon.B1Array, koll4FlagOffset+1)
			value, err := newStreamedLob(rows, test.dtype, test.prefix, lobColumnContext{
				CharsetForm:    test.form,
				TotalLobLength: test.length,
				LobLocator:     locator,
			})
			if test.wantOK {
				if err != nil {
					t.Fatalf("newStreamedLob returned error: %v", err)
				}
				if value == nil {
					t.Fatal("newStreamedLob returned nil value")
				}
				return
			}
			if value != nil {
				t.Fatal("newStreamedLob returned a value for an oversized prefix")
			}
			requireErrorCode(t, err, oracleErrors.InvalidLOBBuffer)
		})
	}
}

// TestStreamedLob_ReadClobUsesDeclaredLengthAndStopsAtEOF verifies that CLOB
// requests use UTF-16 length units and that the final refill does not trigger
// an extra end-of-value locator RPC.
func TestStreamedLob_ReadClobUsesDeclaredLengthAndStopsAtEOF(t *testing.T) {
	t.Parallel()
	rows := newLobTestRows()
	var requested driverCommon.UB8
	value, streamer := newTestStreamedClob(t, rows, 2, nil)
	streamer.onPush = func(def *lobDefinition) { requested = def.lobAmt }
	appendTestLobResponse(streamer, []byte("🙂"), 2, nil)

	dst := make([]byte, len("🙂"))
	if n, err := value.Read(dst); n != len(dst) || err != nil || string(dst) != "🙂" {
		t.Fatalf("Read = (%d, %v, %q), want (%d, nil, 🙂)", n, err, dst, len(dst))
	}
	if requested != 2 {
		t.Fatalf("CLOB request amount = %d, want 2 UTF-16 units", requested)
	}
	if _, err := value.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("final Read error = %v, want io.EOF", err)
	}
	if len(streamer.pushed) != 1 {
		t.Fatalf("CLOB RPC count = %d, want 1", len(streamer.pushed))
	}
}

// TestStreamedLob_ReadRejectsInvalidRefills verifies malformed responses,
// overlarge amounts, and supplementary characters beyond the declared size.
func TestStreamedLob_ReadRejectsInvalidRefills(t *testing.T) {
	t.Parallel()
	type response struct {
		payload []byte
		logical driverCommon.UB8
	}
	tests := []struct {
		name          string
		kind          internallob.Kind
		length        driverCommon.UB8
		responses     []response
		variableWidth bool
		wantRPCs      int
	}{
		{
			name:      "empty response before declared end",
			kind:      internallob.BLOB,
			length:    2,
			responses: []response{{}},
			wantRPCs:  1,
		},
		{
			name:      "empty payload with nonzero amount",
			kind:      internallob.BLOB,
			length:    2,
			responses: []response{{logical: 1}},
			wantRPCs:  1,
		},
		{
			name:      "payload with zero amount",
			kind:      internallob.BLOB,
			length:    2,
			responses: []response{{payload: []byte("a")}},
			wantRPCs:  1,
		},
		{
			name:      "amount exceeds request",
			kind:      internallob.CLOB,
			length:    2,
			responses: []response{{payload: []byte("a"), logical: 3}},
			wantRPCs:  1,
		},
		{
			name:   "supplementary character exceeds declared length",
			kind:   internallob.CLOB,
			length: 1,
			responses: []response{
				{payload: []byte{0xD8, 0x3D}, logical: 1},
				{payload: []byte{0xDE, 0x42}, logical: 1},
			},
			variableWidth: true,
			wantRPCs:      2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rows := newLobTestRows()
			var value *streamedLob
			var streamer *fakeStreamer
			if test.kind == internallob.BLOB {
				value, streamer = newTestStreamedBlob(t, rows, test.length, nil)
			} else {
				value, streamer = newTestStreamedClob(t, rows, test.length, nil)
			}
			if test.variableWidth {
				value.locator = newLocator(newTestLocator(true), 1)
			}
			for _, response := range test.responses {
				appendTestLobResponse(streamer, response.payload, response.logical, nil)
			}

			n, err := value.Read(make([]byte, 1))
			if err == nil {
				t.Fatalf("invalid refill unexpectedly succeeded: n=%d definition=%+v", n, streamer.definition)
			} else {
				requireErrorCode(t, err, oracleErrors.InvalidLOBBuffer)
			}
			rpcCount := len(streamer.pushed)
			if rpcCount != test.wantRPCs {
				t.Fatalf("TTC request count = %d, want %d", rpcCount, test.wantRPCs)
			}
			if _, err := value.Read(make([]byte, 1)); err == nil {
				t.Fatal("invalidated LOB unexpectedly remained readable")
			} else {
				requireErrorCode(t, err, oracleErrors.LobValueInvalidated)
			}
			if len(streamer.pushed) != rpcCount {
				t.Fatalf("read after invalid refill sent another TTC exchange: got %d messages, want %d", len(streamer.pushed), rpcCount)
			}
		})
	}
}

// TestStreamedLob_APIsRejectCanceledContext verifies Read, Size, and ChunkSize
// reject canceled contexts before sending an RPC.
func TestStreamedLob_APIsRejectCanceledContext(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		create func(*testing.T, *ttcRows) (*streamedLob, *fakeStreamer)
		invoke func(*streamedLob) error
	}{
		{
			name: "Read",
			create: func(t *testing.T, rows *ttcRows) (*streamedLob, *fakeStreamer) {
				return newTestStreamedBlob(t, rows, 1, nil)
			},
			invoke: func(value *streamedLob) error {
				_, err := value.Read(make([]byte, 1))
				return err
			},
		},
		{
			name: "Size",
			create: func(t *testing.T, rows *ttcRows) (*streamedLob, *fakeStreamer) {
				return newTestStreamedClob(t, rows, 1, nil)
			},
			invoke: func(value *streamedLob) error {
				_, err := value.Size()
				return err
			},
		},
		{
			name: "ChunkSize",
			create: func(t *testing.T, rows *ttcRows) (*streamedLob, *fakeStreamer) {
				return newTestStreamedBlob(t, rows, 1, nil)
			},
			invoke: func(value *streamedLob) error {
				_, err := value.ChunkSize()
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rows := newLobTestRows()
			value, streamer := test.create(t, rows)
			parent, cancel := context.WithCancel(context.Background())
			cancel()
			defer cancel()
			rows.setContext(parent)

			if err := test.invoke(value); err == nil {
				t.Fatal("operation unexpectedly succeeded after its context was canceled")
			} else {
				requireErrorCode(t, err, oracleErrors.LobValueInvalidated)
			}
			if len(streamer.pushed) != 0 {
				t.Fatalf("operation sent %d TTC messages after context cancellation", len(streamer.pushed))
			}
			_ = value.Close()
		})
	}
}

// TestStreamedLob_ReadRejectsInvalidOwnerBeforeRPC verifies that Rows closure
// and connection invalidation reject a read before sending a TTC request.
func TestStreamedLob_ReadRejectsInvalidOwnerBeforeRPC(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup func(*ttcRows) error
	}{
		{
			name: "Rows closed",
			setup: func(rows *ttcRows) error {
				return rows.Close()
			},
		},
		{
			name: "physical connection invalidated",
			setup: func(rows *ttcRows) error {
				conn := &connection{shelf: rows.shelf, _isValid: true}
				conn.markInvalid()
				return nil
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rows := newLobTestRows()
			value, streamer := newTestStreamedBlob(t, rows, 1, nil)
			if err := test.setup(rows); err != nil {
				t.Fatalf("setup failed: %v", err)
			}
			if _, err := value.Read(make([]byte, 1)); err == nil {
				t.Fatal("Read with invalid owner unexpectedly succeeded")
			} else {
				requireErrorCode(t, err, oracleErrors.LobValueInvalidated)
			}
			if len(streamer.pushed) != 0 {
				t.Fatal("Read with invalid owner performed an RPC")
			}
			_ = value.Close()
		})
	}
}

// TestStreamedLob_RowsCloseDoesNotWaitForStalledLobRead verifies that Rows.Close
// remains non-blocking while a locator RPC ignores cancellation.
func TestStreamedLob_RowsCloseDoesNotWaitForStalledLobRead(t *testing.T) {
	t.Parallel()
	rows := newLobTestRows()
	entered, release := make(chan struct{}), make(chan struct{})
	value, streamer := newTestStreamedBlob(t, rows, 1, nil)
	streamer.onFlush = func() {
		close(entered)
		<-release
	}
	streamer.flushErr = errors.New("transport stopped")
	readDone := make(chan error, 1)
	go func() { _, err := value.Read(make([]byte, 1)); readDone <- err }()
	<-entered
	closeRowsBeforeRelease(t, rows, release)
	if err := <-readDone; err == nil {
		t.Fatal("in-flight Read unexpectedly succeeded")
	}
}

// TestStreamedLob_InFlightSuccessAfterRowsCloseIsRejected verifies that a
// context-ignoring successful Read, Size, or ChunkSize cannot publish a result
// after Rows.Close has invalidated the owning value.
func TestStreamedLob_InFlightSuccessAfterRowsCloseIsRejected(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		value  func(*testing.T, *ttcRows) (*streamedLob, *fakeStreamer)
		setup  func(*fakeStreamer)
		invoke func(*streamedLob) (int64, error)
		result int64
	}{
		{
			name: "Read",
			value: func(t *testing.T, rows *ttcRows) (*streamedLob, *fakeStreamer) {
				return newTestStreamedBlob(t, rows, 1, nil)
			},
			setup: func(streamer *fakeStreamer) {
				appendTestLobResponse(streamer, []byte("l"), 1, nil)
			},
			invoke: func(value *streamedLob) (int64, error) {
				n, err := value.Read(make([]byte, 1))
				return int64(n), err
			},
		},
		{
			name: "Size",
			value: func(t *testing.T, rows *ttcRows) (*streamedLob, *fakeStreamer) {
				return newTestStreamedClob(t, rows, 1, nil)
			},
			setup: func(streamer *fakeStreamer) {
				appendTestLobResponse(streamer, nil, 1, nil)
			},
			invoke: func(value *streamedLob) (int64, error) {
				return value.Size()
			},
		},
		{
			name: "ChunkSize",
			value: func(t *testing.T, rows *ttcRows) (*streamedLob, *fakeStreamer) {
				return newTestStreamedClob(t, rows, 1, nil)
			},
			setup: func(streamer *fakeStreamer) {
				appendTestLobResponse(streamer, nil, 1, nil)
			},
			invoke: func(value *streamedLob) (int64, error) {
				return value.ChunkSize()
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			rows := newLobTestRows()
			entered, release := make(chan struct{}), make(chan struct{})
			value, streamer := test.value(t, rows)
			test.setup(streamer)
			streamer.onFlush = func() {
				close(entered)
				<-release
			}
			operationDone := make(chan struct {
				result int64
				err    error
			}, 1)
			go func() {
				result, err := test.invoke(value)
				operationDone <- struct {
					result int64
					err    error
				}{result: result, err: err}
			}()
			<-entered

			closeRowsBeforeRelease(t, rows, release)
			result := <-operationDone
			if result.result != test.result {
				t.Fatalf("operation result = %d, want %d", result.result, test.result)
			}
			requireErrorCode(t, result.err, oracleErrors.LobValueInvalidated)
		})
	}
}

// TestStreamedLob_ReadRPCFailuresInvalidateRows verifies locator RPC errors
// invalidate the owning Rows and notify the session-stale listener.
func TestStreamedLob_ReadRPCFailuresInvalidateRows(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name          string
		terminalError bool
	}{
		{name: "transport error"},
		{name: "terminal response error", terminalError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			rows := newLobTestRows()
			recorder := &lobEventRecorder{}
			rows.shelf.getEventService().register(recorder, streamerStaleEvent)
			value, streamer := newTestStreamedBlob(t, rows, 1, nil)
			if test.terminalError {
				appendTestLobResponse(streamer, nil, 0, errors.New("server rejected locator operation"))
			} else {
				streamer.pullErr = errors.New("locator transport failed")
			}
			if _, err := value.Read(make([]byte, 1)); err == nil {
				t.Fatal("Read unexpectedly succeeded after an RPC error")
			}
			if !rows.isClosed() {
				t.Fatal("Rows remained open after a locator RPC error")
			}
			if len(recorder.events) != 1 {
				t.Fatalf("stale events = %v, want one", recorder.events)
			}
		})
	}
}

// TestStreamedLob_SizeAndChunkSize verifies cached and server-reported sizes,
// and that RPC failures invalidate Rows for both metadata APIs.
func TestStreamedLob_SizeAndChunkSize(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		value     func(*testing.T, *ttcRows) (*streamedLob, *fakeStreamer)
		setup     func(*fakeStreamer)
		invoke    func(*streamedLob) (int64, error)
		want      int64
		wantRPCs  int
		wantError bool
	}{
		{
			name: "BLOB cached size",
			value: func(t *testing.T, rows *ttcRows) (*streamedLob, *fakeStreamer) {
				return newTestStreamedBlob(t, rows, 7, nil)
			},
			invoke:   func(value *streamedLob) (int64, error) { return value.Size() },
			want:     7,
			wantRPCs: 0,
		},
		{
			name: "CLOB server size",
			value: func(t *testing.T, rows *ttcRows) (*streamedLob, *fakeStreamer) {
				return newTestStreamedClob(t, rows, 1, nil)
			},
			setup:    func(streamer *fakeStreamer) { appendTestLobResponse(streamer, nil, 8, nil) },
			invoke:   func(value *streamedLob) (int64, error) { return value.Size() },
			want:     8,
			wantRPCs: 1,
		},
		{
			name: "CLOB chunk size",
			value: func(t *testing.T, rows *ttcRows) (*streamedLob, *fakeStreamer) {
				return newTestStreamedClob(t, rows, 1, nil)
			},
			setup:    func(streamer *fakeStreamer) { appendTestLobResponse(streamer, nil, 16, nil) },
			invoke:   func(value *streamedLob) (int64, error) { return value.ChunkSize() },
			want:     16,
			wantRPCs: 1,
		},
		{
			name:      "Size terminal response error",
			wantError: true,
			wantRPCs:  1,
			value: func(t *testing.T, rows *ttcRows) (*streamedLob, *fakeStreamer) {
				return newTestStreamedClob(t, rows, 1, nil)
			},
			setup: func(streamer *fakeStreamer) {
				appendTestLobResponse(streamer, nil, 0, errors.New("server rejected metadata request"))
			},
			invoke: func(value *streamedLob) (int64, error) { return value.Size() },
		},
		{
			name:      "Size transport error",
			wantError: true,
			wantRPCs:  1,
			value: func(t *testing.T, rows *ttcRows) (*streamedLob, *fakeStreamer) {
				return newTestStreamedClob(t, rows, 1, nil)
			},
			setup:  func(streamer *fakeStreamer) { streamer.pullErr = errors.New("metadata transport failed") },
			invoke: func(value *streamedLob) (int64, error) { return value.Size() },
		},
		{
			name:      "ChunkSize terminal response error",
			wantError: true,
			wantRPCs:  1,
			value: func(t *testing.T, rows *ttcRows) (*streamedLob, *fakeStreamer) {
				return newTestStreamedClob(t, rows, 1, nil)
			},
			setup: func(streamer *fakeStreamer) {
				appendTestLobResponse(streamer, nil, 0, errors.New("server rejected metadata request"))
			},
			invoke: func(value *streamedLob) (int64, error) { return value.ChunkSize() },
		},
		{
			name:      "ChunkSize transport error",
			wantError: true,
			wantRPCs:  1,
			value: func(t *testing.T, rows *ttcRows) (*streamedLob, *fakeStreamer) {
				return newTestStreamedClob(t, rows, 1, nil)
			},
			setup:  func(streamer *fakeStreamer) { streamer.pullErr = errors.New("metadata transport failed") },
			invoke: func(value *streamedLob) (int64, error) { return value.ChunkSize() },
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			rows := newLobTestRows()
			recorder := &lobEventRecorder{}
			rows.shelf.getEventService().register(recorder, streamerStaleEvent)
			value, streamer := test.value(t, rows)
			if test.setup != nil {
				test.setup(streamer)
			}
			got, err := test.invoke(value)
			if test.wantError {
				if err == nil {
					t.Fatal("metadata operation unexpectedly succeeded")
				}
				if !rows.isClosed() {
					t.Fatal("Rows remained open after a metadata RPC error")
				}
				if len(recorder.events) != 1 {
					t.Fatalf("stale events = %v, want one", recorder.events)
				}
			} else {
				if err != nil || got != test.want {
					t.Fatalf("metadata result = (%d, %v), want (%d, nil)", got, err, test.want)
				}
				if rows.isClosed() {
					t.Fatal("Rows closed after a successful metadata operation")
				}
				_ = value.Close()
			}
			if len(streamer.pushed) != test.wantRPCs {
				t.Fatalf("metadata TTC requests = %d, want %d", len(streamer.pushed), test.wantRPCs)
			}
		})
	}
}

// TestStreamedLob_CloseIsIdempotent verifies local close behavior without a
// server-side locator operation.
func TestStreamedLob_CloseIsIdempotent(t *testing.T) {
	t.Parallel()
	rows := newLobTestRows()
	value, streamer := newTestStreamedBlob(t, rows, 1, nil)
	if err := value.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	if err := value.Close(); err != nil {
		t.Fatalf("second Close returned error: %v", err)
	}
	if rows.isClosed() {
		t.Fatal("closing a LOB value closed its owning Rows")
	}
	if len(streamer.pushed) != 0 {
		t.Fatalf("LOB Close sent %d TTC requests, want none", len(streamer.pushed))
	}
}

// TestStreamedLob_APIsRejectClosedValue verifies every locator API reports the
// closed-value error without issuing another TTC request.
func TestStreamedLob_APIsRejectClosedValue(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		invoke func(*streamedLob) error
	}{
		{
			name: "Read",
			invoke: func(value *streamedLob) error {
				_, err := value.Read(make([]byte, 1))
				return err
			},
		},
		{
			name: "Size",
			invoke: func(value *streamedLob) error {
				_, err := value.Size()
				return err
			},
		},
		{
			name: "ChunkSize",
			invoke: func(value *streamedLob) error {
				_, err := value.ChunkSize()
				return err
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			rows := newLobTestRows()
			value, streamer := newTestStreamedBlob(t, rows, 1, nil)
			if err := value.Close(); err != nil {
				t.Fatalf("Close returned error: %v", err)
			}
			if err := test.invoke(value); err == nil {
				t.Fatal("API after Close unexpectedly succeeded")
			} else {
				requireErrorCode(t, err, oracleErrors.LobValueClosed)
			}
			if len(streamer.pushed) != 0 {
				t.Fatalf("API after Close sent %d TTC requests, want none", len(streamer.pushed))
			}
		})
	}
}

// TestStreamedLob_ClobPrefixConversionUsesCorrectLogicalUnits verifies that
// text prefixes are decoded to UTF-8 while offsets count UTF-16 code units.
func TestStreamedLob_ClobPrefixConversionUsesCorrectLogicalUnits(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		nclob    bool
		input    []byte
		want     []byte
		wantUnit driverCommon.UB8
	}{
		{name: "CLOB", input: []byte("A🙂"), want: []byte("A🙂"), wantUnit: 3},
		{name: "NCLOB", nclob: true, input: []byte{0xD8, 0x3D, 0xDE, 0x42}, want: []byte("🙂"), wantUnit: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			session := driverCommon.NewSessionContext()
			session.SetSessionCharacterSets(al32Utf8CharSet, al16Utf16CharSet)
			executor := newClobExecutor(session)
			loc := newLocator(make(driverCommon.B1Array, koll4FlagOffset+1), 1)
			payload, logical, _, err := executor.decodeReadPayload(loc, test.nclob, test.input, 0, false)
			if err != nil {
				t.Fatalf("decodeReadPayload returned error: %v", err)
			}
			if !bytes.Equal(payload, test.want) || logical != test.wantUnit {
				t.Fatalf("decoded prefix = (%q, %d), want (%q, %d UTF-16 units)", payload, logical, test.want, test.wantUnit)
			}
		})
	}
}

// TestStreamedLob_JoinsSurrogateAcrossPrefetchBoundary reproduces the JDBC
// behavior where a prefetched high surrogate is followed by its low surrogate
// in the first locator response.
func TestStreamedLob_JoinsSurrogateAcrossPrefetchBoundary(t *testing.T) {
	t.Parallel()

	rows := newLobTestRows()
	rows.sessionContext = driverCommon.NewSessionContext()
	rows.sessionContext.SetSessionCharacterSets(al32Utf8CharSet, al16Utf16CharSet)
	_, streamer := newTestStreamedLobManager(t, rows)

	value, err := newStreamedLob(rows, DtyClob, []byte{0xD8, 0x3D}, lobColumnContext{
		CharsetForm:    FormChar,
		TotalLobLength: 2,
		LobLocator:     newTestLocator(true),
	})
	if err != nil {
		t.Fatalf("newStreamedLob returned error: %v", err)
	}
	appendTestLobResponse(streamer, []byte{0xDE, 0x42}, 1, nil)

	got, err := io.ReadAll(value)
	if err != nil {
		t.Fatalf("ReadAll returned error: %v", err)
	}
	if string(got) != "🙂" {
		t.Fatalf("ReadAll = %q, want 🙂", got)
	}
	if value.pendingHighSurrogate != 0 {
		t.Fatal("surrogate carry remained after locator read")
	}
}

// TestStreamedLob_CompletesSurrogatePastReadLimit verifies CLOB decoding can
// consume one extra UTF-16 unit when a requested chunk ends on a high surrogate.
func TestStreamedLob_CompletesSurrogatePastReadLimit(t *testing.T) {
	t.Parallel()

	rows := newLobTestRows()
	chunkUnits := DefaultCharacterLobChunkChars
	value, streamer := newTestStreamedClob(t, rows, driverCommon.UB8(chunkUnits+1), nil)
	value.locator = newLocator(newTestLocator(true), 1)
	var offsets []driverCommon.UB8
	streamer.onPush = func(definition *lobDefinition) {
		offsets = append(offsets, definition.sourceLocator.offset)
	}
	firstPayload := bytes.Repeat([]byte{0, 'A'}, chunkUnits-1)
	firstPayload = append(firstPayload, 0xD8, 0x3D)
	appendTestLobResponse(streamer, firstPayload, driverCommon.UB8(chunkUnits), nil)
	appendTestLobResponse(streamer, []byte{0xDE, 0x42}, 1, nil)

	got, err := io.ReadAll(value)
	want := append(bytes.Repeat([]byte("A"), chunkUnits-1), []byte("🙂")...)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("ReadAll = (%d bytes, %v), want (%d bytes, nil)", len(got), err, len(want))
	}
	if len(offsets) != 2 || offsets[0] != 1 || offsets[1] != driverCommon.UB8(chunkUnits+1) {
		t.Fatalf("locator offsets = %v, want [1 %d]", offsets, chunkUnits+1)
	}
}

// TestStreamedLob_ConstructorRejectsUnsupportedSources verifies constructor
// validation happens before a streamed value is exposed to callers.
func TestStreamedLob_ConstructorRejectsUnsupportedSources(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		owner     func() *ttcRows
		dtype     DtyType
		prefix    []byte
		locator   func() driverCommon.B1Array
		wantError oracleErrors.ErrorCode
	}{
		{
			name: "temporary locator", owner: newLobTestRows, dtype: DtyBlob,
			locator: func() driverCommon.B1Array {
				locator := make(driverCommon.B1Array, koll4FlagOffset+1)
				locator[koll4FlagOffset] = kolblTemporaryFlagByte
				return locator
			},
			wantError: oracleErrors.InvalidLobSource,
		},
		{
			name: "malformed CLOB prefix",
			owner: func() *ttcRows {
				rows := newLobTestRows()
				rows.sessionContext = newTestSessionContext()
				return rows
			},
			dtype:     DtyClob,
			prefix:    []byte{0xff},
			locator:   func() driverCommon.B1Array { return make(driverCommon.B1Array, koll4FlagOffset+1) },
			wantError: oracleErrors.InvalidLOBBuffer,
		},
		{
			name: "abstract locator", owner: newLobTestRows, dtype: DtyBlob,
			locator: func() driverCommon.B1Array {
				locator := make(driverCommon.B1Array, koll4FlagOffset+1)
				locator[koll1FlagOffset] = kolblAbstractLocatorFlag
				return locator
			},
			wantError: oracleErrors.InvalidLobSource,
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			_, err := newStreamedLob(test.owner(), test.dtype, test.prefix, lobColumnContext{LobLocator: test.locator()})
			if err == nil {
				t.Fatal("newStreamedLob unexpectedly succeeded")
			}
			requireErrorCode(t, err, test.wantError)
		})
	}
}

// TestStreamedLob_BufferedRefillIsConsumedBeforeAnotherRPC verifies a partial
// caller read retains and then drains a converted refill.
func TestStreamedLob_BufferedRefillIsConsumedBeforeAnotherRPC(t *testing.T) {
	t.Parallel()

	rows := newLobTestRows()
	value, streamer := newTestStreamedBlob(t, rows, 4, nil)
	appendTestLobResponse(streamer, []byte("abcd"), 4, nil)
	if n, err := value.Read(nil); n != 0 || err != nil {
		t.Fatalf("zero-length Read = (%d, %v), want (0, nil)", n, err)
	}
	if len(streamer.pushed) != 0 {
		t.Fatalf("zero-length Read sent %d TTC requests, want none", len(streamer.pushed))
	}
	first := make([]byte, 1)
	if n, err := value.Read(first); n != 1 || err != nil || string(first) != "a" {
		t.Fatalf("first Read = (%d, %v, %q), want (1, nil, a)", n, err, first)
	}
	if len(value.pending) != 3 {
		t.Fatalf("pending bytes after partial refill = %d, want 3", len(value.pending))
	}
	remaining := make([]byte, 3)
	if n, err := value.Read(remaining); n != 3 || err != nil || string(remaining) != "bcd" {
		t.Fatalf("second Read = (%d, %v, %q), want (3, nil, bcd)", n, err, remaining)
	}
	if len(streamer.pushed) != 1 {
		t.Fatalf("after buffered refill: RPCs=%d, want 1", len(streamer.pushed))
	}
}

// TestStreamedLob_WriteToPropagatesWriterAndReadFailures verifies WriterTo
// preserves bytes written and distinguishes short, writer, and read errors.
func TestStreamedLob_WriteToPropagatesWriterAndReadFailures(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		value    func(*testing.T, *ttcRows) (*streamedLob, *fakeStreamer)
		writer   io.Writer
		readErr  bool
		wantN    int64
		checkErr func(*testing.T, error)
	}{
		{
			name: "nil writer",
			value: func(t *testing.T, rows *ttcRows) (*streamedLob, *fakeStreamer) {
				return newTestStreamedBlob(t, rows, 1, nil)
			},
			checkErr: func(t *testing.T, err error) {
				requireErrorCode(t, err, oracleErrors.InvalidLOBBuffer)
			},
		},
		{
			name: "writer error",
			value: func(t *testing.T, rows *ttcRows) (*streamedLob, *fakeStreamer) {
				return newTestStreamedBlob(t, rows, 3, []byte("abc"))
			},
			writer: streamedLobWriteFunc(func([]byte) (int, error) {
				return 1, errors.New("destination failed")
			}),
			wantN: 1,
			checkErr: func(t *testing.T, err error) {
				if err == nil || !strings.Contains(err.Error(), "destination failed") {
					t.Fatalf("WriterTo error = %v, want destination failure", err)
				}
			},
		},
		{
			name: "short writer",
			value: func(t *testing.T, rows *ttcRows) (*streamedLob, *fakeStreamer) {
				return newTestStreamedBlob(t, rows, 3, []byte("abc"))
			},
			writer: streamedLobWriteFunc(func([]byte) (int, error) { return 1, nil }),
			wantN:  1,
			checkErr: func(t *testing.T, err error) {
				if !errors.Is(err, io.ErrShortWrite) {
					t.Fatalf("WriterTo error = %v, want io.ErrShortWrite", err)
				}
			},
		},
		{
			name: "read error",
			value: func(t *testing.T, rows *ttcRows) (*streamedLob, *fakeStreamer) {
				return newTestStreamedBlob(t, rows, 1, nil)
			},
			writer:  io.Discard,
			readErr: true,
			checkErr: func(t *testing.T, err error) {
				if err == nil {
					t.Fatal("WriterTo unexpectedly succeeded after read failure")
				}
			},
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			rows := newLobTestRows()
			value, streamer := test.value(t, rows)
			if test.readErr {
				streamer.pullErr = errors.New("read failed")
			}
			got, err := value.WriteTo(test.writer)
			if got != test.wantN {
				t.Fatalf("WriterTo bytes = %d, want %d", got, test.wantN)
			}
			test.checkErr(t, err)
			_ = value.Close()
		})
	}
}

// TestStreamedLob_InvalidateRejectsFurtherReads verifies invalidated values
// reject reads before sending another TTC request.
func TestStreamedLob_InvalidateRejectsFurtherReads(t *testing.T) {
	t.Parallel()

	rows := newLobTestRows()
	value, streamer := newTestStreamedBlob(t, rows, 1, nil)
	value.invalidate()
	if _, err := value.Read(make([]byte, 1)); err == nil {
		t.Fatal("Read unexpectedly succeeded after explicit invalidation")
	} else {
		requireErrorCode(t, err, oracleErrors.LobValueInvalidated)
	}
	if len(streamer.pushed) != 0 {
		t.Fatal("Read after invalidation performed an RPC")
	}
	_ = value.Close()
}

// TestStreamedLob_CheckedLengthRejectsOverflow verifies protocol sizes that
// exceed the public int64 range are rejected.
func TestStreamedLob_CheckedLengthRejectsOverflow(t *testing.T) {
	t.Parallel()
	if got, err := checkedLobLength(driverCommon.UB8(uint64(^uint64(0)>>1) + 1)); got != 0 || err == nil {
		t.Fatalf("checkedLobLength overflow = (%d, %v), want InvalidLOBBuffer", got, err)
	} else {
		requireErrorCode(t, err, oracleErrors.InvalidLOBBuffer)
	}
}

// TestStreamedLob_QueryRowCopiesLobPrefixLocatorAndMetadata verifies that row
// decoding transfers independent copies of locator inputs and LOB metadata.
func TestStreamedLob_QueryRowCopiesLobPrefixLocatorAndMetadata(t *testing.T) {
	t.Parallel()

	locatorBytes := make(driverCommon.B1Array, koll4FlagOffset+1)
	locatorBytes[koll4FlagOffset] = kolblTemporaryFlagByte
	metadata := &lobColumnContext{
		CharsetForm:       FormNChar,
		CharsetID:         al16Utf16CharSet,
		LocatorByteLength: 99,
		TotalLobLength:    6,
		LobLocator:        locatorBytes,
	}
	rxd := &tTIrxd{
		row: []columnPayload{{data: driverCommon.B1Array("prefix"), lob: metadata}},
	}
	state := &queryRunState{rows: newTTCRows(nil)}
	state.handleRXDRow(rxd)

	// Mutate every source category after ownership transfer.
	rxd.row[0].data.(driverCommon.B1Array)[0] = 'X'
	metadata.CharsetID = 0
	metadata.LobLocator[koll4FlagOffset] = 0

	copied := state.rows.rowData[0][0]
	if string(copied.data.(driverCommon.B1Array)) != "prefix" {
		t.Fatalf("copied prefix = %q, want prefix", copied.data)
	}
	if copied.lob.CharsetForm != FormNChar || copied.lob.CharsetID != al16Utf16CharSet || copied.lob.LocatorByteLength != 99 || copied.lob.TotalLobLength != 6 {
		t.Fatalf("copied metadata changed: %+v", copied.lob)
	}
	if copied.lob.LobLocator[koll4FlagOffset]&kolblTemporaryFlagByte == 0 {
		t.Fatalf("temporary locator flag was not copied: %+v", copied.lob)
	}
}
