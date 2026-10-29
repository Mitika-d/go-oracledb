/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
**
** Subject to the condition set forth below, permission is hereby granted to any
** person obtaining a copy of this software, associated documentation and/or data
** (collectively the "Software"), free of charge and under any and all copyright
** and related or neighboring rights under any and all patent rights owned or
** freely licensable by each licensor hereunder covering either (i) the
** unmodified Software as contributed to or provided by such licensors, or (ii)
** the Larger Works (as defined below), to deal in both
**
** (a) the Software, and
** (b) any piece of software and/or hardware listed in the lrgrwrks.txt file if
** one is included with the Software (each a "Larger Work" to which the Software
** is contributed by such licensors),
**
** without restriction, including without limitation the rights to copy, create
** derivative works of, display, perform, and distribute the Software, and to
** sublicense the foregoing rights on either these or other terms.
**
** This license is subject to the condition that the above copyright notice and
** this permission notice shall be included in all copies or substantial portions
** of the Software.
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
	"runtime"
	"sync"
	"testing"
	"time"

	internalCommon "github.com/oracle/go-oracledb/v26/internal/common"
	driverCommon "github.com/oracle/go-oracledb/v26/internal/driver/common"
	internallob "github.com/oracle/go-oracledb/v26/internal/lob"
	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
)

// TestConnectionLob_NewLeaseRejectsInvalidInputs exercises the constructor's
// negative cases for missing dependencies and an abandoned physical session.
func TestConnectionLob_NewLeaseRejectsInvalidInputs(t *testing.T) {
	t.Parallel()

	invalidatedShelf := newShelf[driverCommon.MessageType]()
	invalidatedShelf.lobState.invalidate()
	tests := []struct {
		name       string
		shelf      *ttiShelf[driverCommon.MessageType]
		sessionCtx *driverCommon.SessionContext
	}{
		{name: "nil shelf", sessionCtx: newTestSessionContext()},
		{name: "nil session context", shelf: newShelf[driverCommon.MessageType]()},
		{name: "invalidated session", shelf: invalidatedShelf, sessionCtx: newTestSessionContext()},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lease, err := newConnectionLOBLease(test.shelf, test.sessionCtx)
			if lease != nil {
				t.Fatal("lease returned for invalid connection inputs")
			}
			requireErrorCode(t, err, oracleErrors.LobValueInvalidated)
		})
	}
}

// TestConnectionLob_LeaseValidationRejectsInvalidState covers malformed,
// invalidated, and generation-expired lease capabilities.
func TestConnectionLob_LeaseValidationRejectsInvalidState(t *testing.T) {
	t.Parallel()

	invalidatedShelf := newShelf[driverCommon.MessageType]()
	invalidatedShelf.lobState.invalidate()
	expiredShelf := newShelf[driverCommon.MessageType]()
	expiredLease, err := newConnectionLOBLease(expiredShelf, newTestSessionContext())
	if err != nil {
		t.Fatalf("create lease for expiry case: %v", err)
	}
	expiredShelf.lobState.expireLeases()
	tests := []struct {
		name  string
		lease *connection
	}{
		{name: "nil shelf", lease: &connection{}},
		{name: "nil session state", lease: &connection{shelf: &ttiShelf[driverCommon.MessageType]{}}},
		{name: "missing generation", lease: &connection{shelf: newShelf[driverCommon.MessageType]()}},
		{name: "invalidated session", lease: &connection{shelf: invalidatedShelf, leaseGeneration: new(uint64)}},
		{name: "expired generation", lease: expiredLease.(*connection)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requireErrorCode(t, test.lease.ValidateLOBLease(), oracleErrors.LobValueInvalidated)
		})
	}
}

// TestConnectionLob_AcquireLeaseRejectsInvalidState covers acquire failures
// before and after taking the lease read lock.
func TestConnectionLob_AcquireLeaseRejectsInvalidState(t *testing.T) {
	t.Parallel()

	expiredShelf := newShelf[driverCommon.MessageType]()
	expiredLease, err := newConnectionLOBLease(expiredShelf, newTestSessionContext())
	if err != nil {
		t.Fatalf("create lease for expiry case: %v", err)
	}
	expiredShelf.lobState.expireLeases()
	tests := []struct {
		name  string
		lease *connection
	}{
		{name: "missing dependencies", lease: &connection{}},
		{name: "expired generation", lease: expiredLease.(*connection)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			release, err := test.lease.AcquireLOBLease()
			if release != nil {
				release()
				t.Fatal("release function returned for invalid lease")
			}
			requireErrorCode(t, err, oracleErrors.LobValueInvalidated)
		})
	}
}

// TestConnectionLob_LeaseExpiresAfterPoolReturn verifies the pool-return
// validation boundary revokes a locator capability from the previous lease.
func TestConnectionLob_LeaseExpiresAfterPoolReturn(t *testing.T) {
	t.Parallel()

	shelf := newShelf[driverCommon.MessageType]()
	sessionContext := newTestSessionContext()
	physicalConnection := &connection{
		shelf:    shelf,
		sessCtx:  sessionContext,
		ns:       &mockNetworkSession{},
		_isValid: true,
	}
	lease, err := newConnectionLOBLease(shelf, sessionContext)
	if err != nil {
		t.Fatalf("create connection LOB lease: %v", err)
	}
	if err := lease.ValidateLOBLease(); err != nil {
		t.Fatalf("new LOB lease is invalid: %v", err)
	}

	if !physicalConnection.IsValid() {
		t.Fatal("connection was invalidated while returning it to the pool")
	}
	requireErrorCode(t, lease.ValidateLOBLease(), oracleErrors.LobValueInvalidated)
}

// TestConnectionLob_LeaseExpiryWaitsForOperation verifies pool-return
// validation waits until an in-flight LOB operation releases its lease pin.
func TestConnectionLob_LeaseExpiryWaitsForOperation(t *testing.T) {
	t.Parallel()

	shelf := newShelf[driverCommon.MessageType]()
	sessionContext := newTestSessionContext()
	physicalConnection := &connection{
		shelf:    shelf,
		sessCtx:  sessionContext,
		ns:       &mockNetworkSession{},
		_isValid: true,
	}
	lease, err := newConnectionLOBLease(shelf, sessionContext)
	if err != nil {
		t.Fatalf("create connection LOB lease: %v", err)
	}
	if err := lease.ValidateLOBLease(); err != nil {
		t.Fatalf("new LOB lease is invalid: %v", err)
	}

	releaseOperation, err := lease.AcquireLOBLease()
	if err != nil {
		t.Fatalf("acquire LOB operation lease: %v", err)
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(releaseOperation) }
	defer release()

	validationDone := make(chan bool, 1)
	go func() { validationDone <- physicalConnection.IsValid() }()

	// Wait until the validator is queued for the lease write lock. TryRLock
	// fails once that writer is waiting, making the blocked-state assertion
	// independent of scheduler timing.
	queueTimer := time.NewTimer(time.Second)
	defer queueTimer.Stop()
	for {
		if !shelf.lobState.leaseMu.TryRLock() {
			break
		}
		shelf.lobState.leaseMu.RUnlock()
		select {
		case valid := <-validationDone:
			t.Fatalf("pool validation completed while a LOB operation held its lease: valid=%v", valid)
		case <-queueTimer.C:
			t.Fatal("pool validator did not wait on the active LOB lease")
		default:
			runtime.Gosched()
		}
	}

	// The validator is now known to be waiting for this operation's read lock.
	release()
	if valid := <-validationDone; !valid {
		t.Fatal("connection was invalidated while returning it to the pool")
	}
	requireErrorCode(t, lease.ValidateLOBLease(), oracleErrors.LobValueInvalidated)
}

// TestConnectionLob_LobOperationRejectsUnavailableState exercises invalid
// connection, invalidated session, and expired lease paths before an RPC.
func TestConnectionLob_LobOperationRejectsUnavailableState(t *testing.T) {
	t.Parallel()

	invalidatedShelf := newShelf[driverCommon.MessageType]()
	invalidatedShelf.lobState.invalidate()
	expiredShelf := newShelf[driverCommon.MessageType]()
	expiredLease, err := newConnectionLOBLease(expiredShelf, newTestSessionContext())
	if err != nil {
		t.Fatalf("create lease for expiry case: %v", err)
	}
	expiredShelf.lobState.expireLeases()
	tests := []struct {
		name string
		conn *connection
	}{
		{name: "closed connection", conn: &connection{_isClosed: true, shelf: newShelf[driverCommon.MessageType]()}},
		{name: "invalid connection", conn: &connection{shelf: newShelf[driverCommon.MessageType]()}},
		{name: "missing shelf", conn: &connection{_isValid: true}},
		{name: "invalidated session", conn: &connection{shelf: invalidatedShelf, _isValid: true}},
		{name: "expired lease", conn: expiredLease.(*connection)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.conn._lobOperation(context.Background(), func(context.Context) error {
				t.Fatal("operation ran for an unavailable connection")
				return nil
			})
			requireErrorCode(t, err, oracleErrors.LobValueInvalidated)
		})
	}
}

// TestConnectionLob_LobOperationAbandonsSessionAfterTerminalError ensures an
// unconsumed TTC error invalidates the LOB session before another request.
func TestConnectionLob_LobOperationAbandonsSessionAfterTerminalError(t *testing.T) {
	t.Parallel()

	shelf := newShelf[driverCommon.MessageType]()
	conn := &connection{shelf: shelf, _isValid: true}
	wantErr := internalCommon.NewOracleError(oracleErrors.LobExecError, errors.New("wire failure"), "test")
	err := conn._lobOperation(context.Background(), func(context.Context) error { return wantErr })
	if !errors.Is(err, wantErr) {
		t.Fatalf("LOB operation error = %v, want wrapped %v", err, wantErr)
	}
	if !shelf.lobState.isInvalidated() {
		t.Fatal("LOB session remained usable after a terminal TTC error")
	}
}

// TestConnectionLob_LobCreateRejectsIncompleteSession verifies the internal
// bridge reports missing connection dependencies without starting an RPC.
func TestConnectionLob_LobCreateRejectsIncompleteSession(t *testing.T) {
	t.Parallel()

	conn := &connection{}
	_, err := conn.LobCreate(context.Background(), uint8(internallob.BLOB))
	requireErrorCode(t, err, oracleErrors.InternalError)
}

// TestConnectionLob_LobFreeQueuesTemporaryLocator covers the successful local
// free path and verifies the locator is queued for piggyback cleanup.
func TestConnectionLob_LobFreeQueuesTemporaryLocator(t *testing.T) {
	t.Parallel()

	shelf := newShelf[driverCommon.MessageType]()
	conn := &connection{shelf: shelf, sessCtx: newTestSessionContext(), _isValid: true}
	loc := newTestLobReferenceLocator(81)
	if err := conn.LobFree(context.Background(), loc.locatorBytes); err != nil {
		t.Fatalf("LobFree: %v", err)
	}
	batch, err := shelf.lobState.lobReferenceRegistry.ReservePending()
	if err != nil || batch == nil {
		t.Fatalf("pending free = %v, %v; want queued temporary locator", batch, err)
	}
}

// TestConnectionLob_LocatorOperationsRejectEmptyLocator verifies every
// locator-based LOB operation rejects an empty locator before any TTC exchange.
func TestConnectionLob_LocatorOperationsRejectEmptyLocator(t *testing.T) {
	t.Parallel()

	conn := &connection{}
	ctx := context.Background()
	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "read",
			call: func() error {
				_, _, err := conn.LobRead(ctx, uint8(internallob.BLOB), nil, 1, 1)
				return err
			},
		},
		{
			name: "write",
			call: func() error {
				_, err := conn.LobWrite(ctx, uint8(internallob.BLOB), nil, 1, nil)
				return err
			},
		},
		{
			name: "length",
			call: func() error {
				_, err := conn.LobLength(ctx, uint8(internallob.BLOB), nil)
				return err
			},
		},
		{
			name: "chunk size",
			call: func() error {
				_, err := conn.LobChunkSize(ctx, uint8(internallob.BLOB), nil)
				return err
			},
		},
		{
			name: "trim",
			call: func() error {
				_, err := conn.LobTrim(ctx, uint8(internallob.BLOB), nil, 0)
				return err
			},
		},
		{
			name: "open",
			call: func() error {
				_, _, err := conn.LobOpen(ctx, uint8(internallob.BLOB), nil, uint8(lobOpenModeReadOnly))
				return err
			},
		},
		{
			name: "close",
			call: func() error {
				_, err := conn.LobClose(ctx, uint8(internallob.BLOB), nil)
				return err
			},
		},
		{
			name: "is open",
			call: func() error {
				_, err := conn.LobIsOpen(ctx, uint8(internallob.BLOB), nil)
				return err
			},
		},
		{
			name: "free",
			call: func() error { return conn.LobFree(ctx, nil) },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requireErrorCode(t, test.call(), oracleErrors.InvalidLOBBuffer)
		})
	}
}

// TestConnectionLob_ReadWriteRejectZeroOffset verifies the 1-based locator
// contract is enforced before starting a TTC exchange.
func TestConnectionLob_ReadWriteRejectZeroOffset(t *testing.T) {
	t.Parallel()

	conn := &connection{}
	ctx := context.Background()
	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "read",
			call: func() error {
				_, _, err := conn.LobRead(ctx, uint8(internallob.BLOB), []byte("locator"), 0, 1)
				return err
			},
		},
		{
			name: "write",
			call: func() error {
				_, err := conn.LobWrite(ctx, uint8(internallob.BLOB), []byte("locator"), 0, []byte("data"))
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requireErrorCode(t, test.call(), oracleErrors.InvalidLOBBuffer)
		})
	}
}

// TestConnectionLob_ReadReturnsPayloadAndLogicalAmount verifies a successful
// connection LOB read uses the manager and TTC executor while
// preserving the caller's locator bytes.
func TestConnectionLob_ReadReturnsPayloadAndLogicalAmount(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	want := driverCommon.B1Array{0x00, 0xff, 0x10, 0x80, 0x41}
	locator := newTestLocator(false)
	locator[koll1FlagOffset] = kolblBlobFlag
	originalLocator := append(driverCommon.B1Array(nil), locator...)
	fixtureShelf, _, _ := newLobTestShelf(8192)
	shelf := newShelf[driverCommon.MessageType]()
	shelf.RegisterMessageFactory(fixtureShelf.GetMessageFactory())
	shelf.RegisterMessageStreamer(&fakeStreamer{
		events: []driverCommon.Message[driverCommon.MessageType]{
			newTTIlobd(),
			newTTILobRPA(),
			&mockOer{},
		},
		preHooks:      make(map[driverCommon.MessageType]StreamerPreUnmarshallCallback),
		postHooks:     make(map[driverCommon.MessageType]StreamerPostUnmarshallCallback),
		lobdPayloads:  [][]byte{want},
		lobRpaAmounts: []driverCommon.UB8{driverCommon.UB8(len(want))},
	})
	conn := &connection{
		shelf:    shelf,
		sessCtx:  newTestSessionContext(),
		_isValid: true,
	}

	got, logical, err := conn.LobRead(ctx, uint8(internallob.BLOB), locator, 1, uint64(len(want)))
	if err != nil {
		t.Fatalf("LobRead() error = %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("LobRead() payload = % X, want % X", got, want)
	}
	if logical != uint64(len(want)) {
		t.Fatalf("LobRead() logical amount = %d, want %d", logical, len(want))
	}
	if !bytes.Equal(locator, originalLocator) {
		t.Fatalf("LobRead() changed locator bytes: got % X, want % X", locator, originalLocator)
	}
}

// TestConnectionLob_LobOperationsRejectUnknownKind verifies LOB operations
// reject an unsupported LOB family before attempting a TTC exchange.
func TestConnectionLob_LobOperationsRejectUnknownKind(t *testing.T) {
	t.Parallel()

	shelf := newShelf[driverCommon.MessageType]()
	conn := &connection{
		shelf:    shelf,
		sessCtx:  driverCommon.NewSessionContext(),
		_isValid: true,
	}
	ctx := context.Background()
	locator := []byte("locator")
	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "create",
			call: func() error {
				_, err := conn.LobCreate(ctx, uint8(internallob.Unknown))
				return err
			},
		},
		{
			name: "read",
			call: func() error {
				_, _, err := conn.LobRead(ctx, uint8(internallob.Unknown), locator, 1, 1)
				return err
			},
		},
		{
			name: "write",
			call: func() error {
				_, err := conn.LobWrite(ctx, uint8(internallob.Unknown), locator, 1, nil)
				return err
			},
		},
		{
			name: "length",
			call: func() error {
				_, err := conn.LobLength(ctx, uint8(internallob.Unknown), locator)
				return err
			},
		},
		{
			name: "chunk size",
			call: func() error {
				_, err := conn.LobChunkSize(ctx, uint8(internallob.Unknown), locator)
				return err
			},
		},
		{
			name: "trim",
			call: func() error {
				_, err := conn.LobTrim(ctx, uint8(internallob.Unknown), locator, 0)
				return err
			},
		},
		{
			name: "open",
			call: func() error {
				_, _, err := conn.LobOpen(ctx, uint8(internallob.Unknown), locator, uint8(lobOpenModeReadOnly))
				return err
			},
		},
		{
			name: "close",
			call: func() error {
				_, err := conn.LobClose(ctx, uint8(internallob.Unknown), locator)
				return err
			},
		},
		{
			name: "is open",
			call: func() error {
				_, err := conn.LobIsOpen(ctx, uint8(internallob.Unknown), locator)
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requireErrorCode(t, test.call(), oracleErrors.UnsupportedLobOperation)
		})
	}
}

// TestConnectionLob_LobSessionKeyReturnsPhysicalSession verifies the session
// key identifies the physical shelf that owns the LOB locator.
func TestConnectionLob_LobSessionKeyReturnsPhysicalSession(t *testing.T) {
	t.Parallel()

	shelf := newShelf[driverCommon.MessageType]()
	conn := &connection{shelf: shelf}
	if got := conn.LobSessionKey(); got != shelf {
		t.Fatalf("LobSessionKey() = %p, want %p", got, shelf)
	}
}
