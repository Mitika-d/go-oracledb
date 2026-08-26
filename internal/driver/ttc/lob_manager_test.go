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
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/oracle/go-oracledb/v26/internal/driver/common"
	internallob "github.com/oracle/go-oracledb/v26/internal/lob"
	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
)

// admissionProbeContext signals when sessionSynchronizer.begin has reached
// its admission select. It lets tests prove that an operation is waiting for
// the physical-session token without relying on scheduler timing.
type admissionProbeContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

// Done signals that the synchronizer is about to wait for admission and then
// returns the wrapped context's cancellation channel.
func (ctx *admissionProbeContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.entered) })
	return ctx.Context.Done()
}

// Err delegates cancellation checks to the wrapped context.
func (ctx *admissionProbeContext) Err() error {
	return ctx.Context.Err()
}

// TestLobManager_OwnsTemporaryLease verifies that temporary-locator ownership
// is recorded and released through the manager, not through the shelf.
func TestLobManager_OwnsTemporaryLease(t *testing.T) {
	t.Parallel()
	shelf := newShelf[common.MessageType]()
	manager, err := newLobManager(shelf, newTestSessionContext())
	if err != nil {
		t.Fatalf("newLobManager: %v", err)
	}
	loc := newTestLobReferenceLocator(71)

	lease, err := manager.retainLobReference(loc)
	if err != nil {
		t.Fatalf("retainLobReference: %v", err)
	}
	if err := manager.releaseLobReference(lease); err != nil {
		t.Fatalf("releaseLobReference: %v", err)
	}
	if loc.isTemporaryLocator() {
		t.Fatal("released locator still appears temporary")
	}
	if got := lobReferenceEntryCount(shelf); got != 1 {
		t.Fatalf("pending temporary entries = %d, want 1", got)
	}
}

// TestLobManager_RejectsUnsupportedKind verifies the shared dispatch boundary
// rejects unsupported kinds before it reaches a type-specific executor.
func TestLobManager_RejectsUnsupportedKind(t *testing.T) {
	t.Parallel()
	manager, err := newLobManager(newShelf[common.MessageType](), newTestSessionContext())
	if err != nil {
		t.Fatalf("newLobManager: %v", err)
	}
	_, err = manager.createTemporary(context.Background(), internallob.Kind(99))
	var coded oracleErrors.SQLError
	if err == nil || !errors.As(err, &coded) || coded.ErrorCode() != string(oracleErrors.InvalidLobSource) {
		t.Fatalf("createTemporary unsupported kind error = %v, want %s", err, oracleErrors.InvalidLobSource)
	}
}

// TestLobManager_SharesSessionExecutors verifies that managers created for one
// physical session reuse one executor for each LOB family.
func TestLobManager_SharesSessionExecutors(t *testing.T) {
	t.Parallel()
	shelf := newShelf[common.MessageType]()
	sessionContext := newTestSessionContext()
	first, err := newLobManager(shelf, sessionContext)
	if err != nil {
		t.Fatalf("newLobManager first: %v", err)
	}
	second, err := newLobManager(shelf, sessionContext)
	if err != nil {
		t.Fatalf("newLobManager second: %v", err)
	}
	if first.getBlobExecutor() != second.getBlobExecutor() {
		t.Fatal("BLOB executor was not shared by session managers")
	}
	if first.getClobExecutor() != second.getClobExecutor() {
		t.Fatal("CLOB executor was not shared by session managers")
	}
}

// TestLobManager_SharedExecutorExchangesAreAdmissionSerialized verifies that
// shared executors are used only inside exclusive physical-session admission.
func TestLobManager_SharedExecutorExchangesAreAdmissionSerialized(t *testing.T) {
	t.Parallel()
	ttcShelf := newShelf[common.MessageType]()
	fixtureShelf, _, _ := newLobTestShelf(8192)
	ttcShelf.RegisterMessageFactory(fixtureShelf.GetMessageFactory())

	firstFlush, allowFirstFlush := make(chan struct{}), make(chan struct{})
	secondAdmitted := make(chan struct{})
	var allowOnce sync.Once
	allow := func() { allowOnce.Do(func() { close(allowFirstFlush) }) }
	defer allow()
	flushes := 0
	streamer := &fakeStreamer{
		events:    make([]common.Message[common.MessageType], 0, 4),
		preHooks:  make(map[common.MessageType]StreamerPreUnmarshallCallback),
		postHooks: make(map[common.MessageType]StreamerPostUnmarshallCallback),
		onFlush: func() {
			flushes++
			if flushes == 1 {
				close(firstFlush)
				<-allowFirstFlush
			}
		},
	}
	factory := ttcShelf.GetMessageFactory().(Factory)
	for index := 0; index < 2; index++ {
		response, err := factory.GetMessageForFunction(TTIRPA, oLobOps)
		if err != nil {
			t.Fatalf("GetMessageForFunction: %v", err)
		}
		streamer.events = append(streamer.events, response, &mockOer{})
	}
	streamer.lobRpaAmounts = []common.UB8{0, 0}
	ttcShelf.RegisterMessageStreamer(streamer)

	firstManager, err := newLobManager(ttcShelf, newTestSessionContext())
	if err != nil {
		t.Fatalf("newLobManager first: %v", err)
	}
	secondManager, err := newLobManager(ttcShelf, newTestSessionContext())
	if err != nil {
		t.Fatalf("newLobManager second: %v", err)
	}
	if firstManager.getBlobExecutor() != secondManager.getBlobExecutor() {
		t.Fatal("BLOB executor was not shared by session managers")
	}

	firstStarted := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		release, beginErr := ttcShelf.synchronizer.begin(context.Background())
		if beginErr != nil {
			firstDone <- beginErr
			return
		}
		close(firstStarted)
		_, _, readErr := firstManager.read(context.Background(), internallob.BLOB, newLocator(newTestLocator(false), 1), 1)
		release()
		firstDone <- readErr
	}()
	<-firstStarted
	<-firstFlush

	secondWaiting := make(chan struct{})
	secondDone := make(chan error, 1)
	secondContext := &admissionProbeContext{
		Context: context.Background(),
		entered: secondWaiting,
	}
	go func() {
		release, beginErr := ttcShelf.synchronizer.begin(secondContext)
		if beginErr != nil {
			secondDone <- beginErr
			return
		}
		close(secondAdmitted)
		_, _, readErr := secondManager.read(context.Background(), internallob.BLOB, newLocator(newTestLocator(false), 1), 1)
		release()
		secondDone <- readErr
	}()
	<-secondWaiting
	select {
	case <-secondAdmitted:
		t.Fatal("second LOB exchange acquired admission while first exchange was active")
	default:
	}
	allow()
	if err := <-firstDone; err != nil {
		t.Fatalf("first LOB exchange: %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second LOB exchange: %v", err)
	}
}

// TestLobManager_RejectsIncompleteSession verifies that incomplete session
// dependencies cannot produce an operational manager.
func TestLobManager_RejectsIncompleteSession(t *testing.T) {
	t.Parallel()
	manager, err := newLobManager(newShelf[common.MessageType](), nil)
	if err == nil || manager != nil {
		t.Fatalf("newLobManager incomplete session = (%v, %v), want nil manager and error", manager, err)
	}
	var coded oracleErrors.SQLError
	if !errors.As(err, &coded) || coded.ErrorCode() != string(oracleErrors.InvalidLobInput) {
		t.Fatalf("newLobManager incomplete session error = %v, want %s", err, oracleErrors.InvalidLobInput)
	}
}
