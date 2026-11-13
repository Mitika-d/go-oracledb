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
	"database/sql/driver"
	"unicode/utf8"

	"github.com/oracle/go-oracledb/v26/internal/common"
	driverCommon "github.com/oracle/go-oracledb/v26/internal/driver/common"
	internallob "github.com/oracle/go-oracledb/v26/internal/lob"
	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
)

// lobManager dispatches LOB operations to the cached TTC executors and manages
// references for one physical TTC session. Callers own cancellation and
// decisions to discard an unsafe session.
type lobManager struct {
	shelf      *ttiShelf[driverCommon.MessageType]
	sessionCtx *driverCommon.SessionContext
}

// newLobManager creates a LOB manager bound to one session shelf and its
// negotiated character-set context. Callers supply valid session state.
//
// Returns:
//   - *lobManager: manager bound to the supplied session.
func newLobManager(shelf *ttiShelf[driverCommon.MessageType], sessionCtx *driverCommon.SessionContext) *lobManager {
	return &lobManager{
		shelf:      shelf,
		sessionCtx: sessionCtx,
	}
}

// abandonSession lets execution-scoped cleanup abandon this manager's session.
func (m *lobManager) abandonSession() {
	abandonLobSession(m.shelf)
}

// getBlobExecutor returns the immutable BLOB protocol helper shared by this
// manager's physical session. The manager does not coordinate concurrent TTC
// operations.
func (m *lobManager) getBlobExecutor() *blobExecutor {
	return m.shelf.lobState.getBlobExecutor(m.shelf.Shelf)
}

// getClobExecutor returns the immutable CLOB/NCLOB protocol helper shared by
// this manager's physical session. The manager does not coordinate concurrent
// TTC operations.
func (m *lobManager) getClobExecutor() *clobExecutor {
	return m.shelf.lobState.getClobExecutor(m.shelf.Shelf, m.sessionCtx)
}

// createTemporary creates a session-duration locator for kind.
//
// Parameters:
//   - ctx: context for the create RPC.
//   - kind: BLOB, CLOB, or NCLOB locator family to create.
//
// Returns:
//   - driverCommon.B1Array: newly created locator bytes.
//   - error: validation, create-RPC, or locator-kind error.
func (m *lobManager) createTemporary(ctx context.Context, kind internallob.Kind) (driverCommon.B1Array, error) {
	if err := validateTTCLobKind(kind); err != nil {
		return nil, err
	}
	var bytes driverCommon.B1Array
	var err error
	switch kind {
	case internallob.BLOB:
		bytes, err = m.getBlobExecutor().createTemporaryLob(ctx, false, durationSession)
	case internallob.CLOB:
		bytes, err = m.getClobExecutor().createTemporaryLob(ctx, false, durationSession, FormChar)
	case internallob.NCLOB:
		bytes, err = m.getClobExecutor().createTemporaryLob(ctx, false, durationSession, FormNChar)
	}
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

// read dispatches a bounded locator read and returns data with the Oracle
// logical-unit count consumed from loc.
//
// Parameters:
//   - ctx: context for the read RPC.
//   - kind: BLOB, CLOB, or NCLOB family of loc.
//   - loc: locator to read without changing its caller-owned offset.
//   - amount: maximum Oracle logical units to request.
//   - prefixHighSurrogate: high surrogate carried from a prefetched text
//     prefix, or zero when the read starts on a character boundary.
//
// Returns:
//   - []byte: read BLOB bytes or UTF-8 character-LOB bytes.
//   - driverCommon.UB8: logical units consumed by the read.
//   - error: validation, read-RPC, or locator-kind error.
func (m *lobManager) read(
	ctx context.Context,
	kind internallob.Kind,
	loc *locator,
	amount driverCommon.UB8,
	prefixHighSurrogate uint16,
) ([]byte, driverCommon.UB8, error) {
	if err := validateTTCLobKind(kind); err != nil {
		return nil, 0, err
	}
	if kind == internallob.BLOB {
		if prefixHighSurrogate != 0 {
			return nil, 0, common.NewOracleError(oracleErrors.InvalidLOBBuffer, nil, "read", "BLOB", "UTF-16 carry")
		}
		return m.getBlobExecutor().read(ctx, loc, amount)
	}
	return m.getClobExecutor().read(ctx, loc, amount, kind == internallob.NCLOB, prefixHighSurrogate)
}

// write dispatches data to the type-specific locator writer and returns the
// Oracle logical-unit count acknowledged by the server.
//
// Parameters:
//   - ctx: context for the write RPC.
//   - kind: BLOB, CLOB, or NCLOB family of loc.
//   - loc: locator to update at its caller-owned offset.
//   - data: BLOB bytes or UTF-8 character-LOB bytes.
//
// Returns:
//   - driverCommon.UB8: logical units acknowledged by Oracle.
//   - error: validation, write-RPC, or locator-kind error.
func (m *lobManager) write(ctx context.Context, kind internallob.Kind, loc *locator, data []byte) (driverCommon.UB8, error) {
	if err := validateTTCLobKind(kind); err != nil {
		return 0, err
	}
	if kind == internallob.BLOB {
		return m.getBlobExecutor().write(ctx, loc, data)
	}
	if !utf8.Valid(data) {
		return 0, common.NewOracleError(oracleErrors.InvalidLOBBuffer, nil, "write", "CLOB", "invalid UTF-8")
	}
	runes := []rune(string(data))
	return m.getClobExecutor().write(ctx, loc, kind == internallob.NCLOB, runes, len(runes))
}

// decodePrefix converts an inline CLOB or NCLOB payload to UTF-8 without a TTC
// exchange. It returns the decoded bytes, Oracle character units consumed,
// and a trailing UTF-16 high surrogate to join with a later locator read.
func (m *lobManager) decodePrefix(kind internallob.Kind, loc *locator, payload []byte, allowTrailingHighSurrogate bool) ([]byte, driverCommon.UB8, uint16, error) {
	if kind != internallob.CLOB && kind != internallob.NCLOB {
		return nil, 0, 0, common.NewOracleError(oracleErrors.UnsupportedLobOperation, nil, "LOB kind")
	}
	data, logical, carry, err := m.getClobExecutor().decodeReadPayload(loc, kind == internallob.NCLOB, payload, 0, allowTrailingHighSurrogate)
	return data, logical, carry, err
}

// length returns the server logical length for loc in the units of kind.
//
// Parameters:
//   - ctx: context for the length RPC.
//   - kind: BLOB, CLOB, or NCLOB family of loc.
//   - loc: locator to inspect.
//
// Returns:
//   - driverCommon.UB8: bytes for BLOB or UTF-16 units for CLOB and NCLOB.
//   - error: validation, length-RPC, or locator-kind error.
func (m *lobManager) length(ctx context.Context, kind internallob.Kind, loc *locator) (driverCommon.UB8, error) {
	if err := validateTTCLobKind(kind); err != nil {
		return 0, err
	}
	if kind == internallob.BLOB {
		return m.getBlobExecutor().getLength(ctx, loc)
	}
	return m.getClobExecutor().getLength(ctx, loc)
}

// chunkSize returns Oracle's storage chunk size for loc.
//
// Parameters:
//   - ctx: context for the chunk-size RPC.
//   - kind: BLOB, CLOB, or NCLOB family of loc.
//   - loc: locator to inspect.
//
// Returns:
//   - driverCommon.UB8: server storage chunk size.
//   - error: validation, RPC, or locator-kind error.
func (m *lobManager) chunkSize(ctx context.Context, kind internallob.Kind, loc *locator) (driverCommon.UB8, error) {
	if err := validateTTCLobKind(kind); err != nil {
		return 0, err
	}
	if kind == internallob.BLOB {
		return m.getBlobExecutor().getChunkSize(ctx, loc)
	}
	return m.getClobExecutor().getChunkSize(ctx, loc)
}

// trim changes loc to length logical units and returns the server result.
//
// Parameters:
//   - ctx: context for the trim RPC.
//   - kind: BLOB, CLOB, or NCLOB family of loc.
//   - loc: locator to truncate or extend.
//   - length: target length in the logical units of kind.
//
// Returns:
//   - driverCommon.UB8: server-reported resulting logical length.
//   - error: validation, trim-RPC, or locator-kind error.
func (m *lobManager) trim(ctx context.Context, kind internallob.Kind, loc *locator, length driverCommon.UB8) (driverCommon.UB8, error) {
	if err := validateTTCLobKind(kind); err != nil {
		return 0, err
	}
	if kind == internallob.BLOB {
		return m.getBlobExecutor().trim(ctx, loc, length)
	}
	return m.getClobExecutor().trim(ctx, loc, length)
}

// open requests server-side open state for loc and reports whether it changed.
//
// Parameters:
//   - ctx: context for the open RPC.
//   - kind: BLOB, CLOB, or NCLOB family of loc.
//   - loc: locator to open.
//   - mode: server-side read-only or read-write mode.
//
// Returns:
//   - bool: true when this call changed server state.
//   - error: validation, open-RPC, or locator-kind error.
func (m *lobManager) open(ctx context.Context, kind internallob.Kind, loc *locator, mode lobOpenMode) (bool, error) {
	if err := validateTTCLobKind(kind); err != nil {
		return false, err
	}
	if mode != lobOpenModeReadOnly && mode != lobOpenModeReadWrite {
		return false, common.NewOracleError(oracleErrors.UnsupportedLobOperation, nil, "LOB open mode")
	}
	if kind == internallob.BLOB {
		return m.getBlobExecutor().open(ctx, loc, mode)
	}
	return m.getClobExecutor().open(ctx, loc, mode)
}

// close requests Oracle's server-side close operation for loc.
//
// Parameters:
//   - ctx: context for the close RPC.
//   - kind: BLOB, CLOB, or NCLOB family of loc.
//   - loc: locator to close server-side.
//
// Returns:
//   - error: validation, close-RPC, or locator-kind error.
func (m *lobManager) close(ctx context.Context, kind internallob.Kind, loc *locator) error {
	if err := validateTTCLobKind(kind); err != nil {
		return err
	}
	if kind == internallob.BLOB {
		return m.getBlobExecutor().close(ctx, loc)
	}
	return m.getClobExecutor().close(ctx, loc)
}

// isOpen reports loc's current server-side open state.
//
// Parameters:
//   - ctx: context for the state RPC.
//   - kind: BLOB, CLOB, or NCLOB family of loc.
//   - loc: locator to inspect.
//
// Returns:
//   - bool: current server-side open state.
//   - error: validation, RPC, or locator-kind error.
func (m *lobManager) isOpen(ctx context.Context, kind internallob.Kind, loc *locator) (bool, error) {
	if err := validateTTCLobKind(kind); err != nil {
		return false, err
	}
	if kind == internallob.BLOB {
		return m.getBlobExecutor().isOpen(ctx, loc)
	}
	return m.getClobExecutor().isOpen(ctx, loc)
}

// retainLobReference records one local owner after creation or locator transfer.
func (m *lobManager) retainLobReference(loc *locator) (*lobReference, error) {
	return m.shelf.lobState.lobReferenceRegistry.Retain(loc)
}

// releaseLobReference releases one local owner and clears its locator flags.
// The last release queues a server free for the next ordinary TTC function.
func (m *lobManager) releaseLobReference(reference *lobReference) error {
	return m.shelf.lobState.lobReferenceRegistry.Release(reference)
}

// freeLobReference queues a standalone locator for piggyback free without
// issuing a TTC request. Release clears the caller's locator flags.
func (m *lobManager) freeLobReference(loc *locator) error {
	reference, err := m.retainLobReference(loc)
	if err != nil {
		return err
	}
	return m.releaseLobReference(reference)
}

// abandonLobSession discards session-local LOB state and posts a stale-stream
// event after an exchange whose terminal response may not have been consumed.
// It needs only the shelf because teardown may lack session character-set data.
func abandonLobSession(shelf *ttiShelf[driverCommon.MessageType]) {
	discardLobSessionState(shelf)
	shelf.getEventService().post(streamerStaleEvent)
}

// discardLobSessionState invalidates LOB operations and drops local temporary
// LOB bookkeeping when a session closes or becomes unusable. Physical-session
// teardown reclaims server-side session-duration LOBs; this sends no TTC RPC.
func discardLobSessionState(shelf *ttiShelf[driverCommon.MessageType]) {
	shelf.lobState.markInvalid()
	shelf.lobState.lobReferenceRegistry.Discard()
}

// validateTTCLobKind rejects LOB kinds that have no TTC executor.
func validateTTCLobKind(kind internallob.Kind) error {
	if !internallob.ValidKind(kind) {
		return common.NewOracleError(oracleErrors.UnsupportedLobOperation, nil, "LOB kind")
	}
	return nil
}

// --- Streamed bind preparation ---

// prepareBinds performs temporary creation, streaming, reference retention, and
// failure abandonment as one ownership transaction. args must already be
// normalized and validated; firstLobBindIndex is the index returned by that
// validation pass. The statement layer receives locator bind values and an
// opaque cleanup owner.
//
// Parameters:
//   - ctx: context for temporary-LOB creation and streaming RPCs.
//   - args: normalized statement arguments containing validated LOB inputs.
//   - firstLobBindIndex: index of the first streamed LOB input.
//
// Returns:
//   - []driver.NamedValue: copied arguments with streamed inputs replaced by locator binds.
//   - *preparedLobBinds: owner of locators created during preparation.
//   - error: preparation or cleanup-registration error.
func (m *lobManager) prepareBinds(ctx context.Context, args []driver.NamedValue, firstLobBindIndex int) ([]driver.NamedValue, *preparedLobBinds, error) {
	// Keep statement arguments separate from the caller's slice because each LOB
	// marker is replaced with a locator-backed bind value.
	prepared := append([]driver.NamedValue(nil), args...)
	preparedBinds := &preparedLobBinds{manager: m}
	for index := firstLobBindIndex; index < len(prepared); index++ {
		if value, ok := prepared[index].Value.(internallob.LOBLocatorBindValue); ok {
			bind, err := m.bindLOBLocatorValue(value)
			if err != nil {
				if cleanupErr := preparedBinds.free(); cleanupErr != nil {
					common.Odl.Error("temporary LOB cleanup after locator bind failure failed", "error", cleanupErr)
				}
				return nil, nil, err
			}
			prepared[index].Value = bind
			continue
		}
		input, ok := prepared[index].Value.(lobBindInput)
		if !ok {
			continue
		}
		// Materialize the input before the statement encoder sees it, and retain
		// the locator before handling either success or a post-create error.
		bind, loc, disposition, err := m.streamInput(ctx, input)
		if loc != nil {
			if retainErr := preparedBinds.add(loc); retainErr != nil {
				// Without a stable LOB ID the locator cannot be queued safely. Tear
				// down the physical session rather than issuing a standalone free.
				preparedBinds.abandon()
				return nil, nil, retainErr
			}
		}
		if err != nil {
			// A completed exchange can queue cleanup; an ambiguous exchange must
			// abandon the physical session instead.
			if disposition == lobCleanupFreeNow {
				if cleanupErr := preparedBinds.free(); cleanupErr != nil {
					common.Odl.Error("temporary LOB cleanup after bind failure failed", "error", cleanupErr)
				}
			} else {
				preparedBinds.abandon()
			}
			return nil, nil, err
		}
		// The statement encoder will turn this private value into LOB-specific
		// RXD and OAC data.
		prepared[index].Value = bind
	}
	return prepared, preparedBinds, nil
}

// bindLOBLocatorValue converts a caller-owned locator into TTC bind metadata.
// The locator must belong to this physical session; its lifetime remains owned
// by the caller after statement execution.
func (m *lobManager) bindLOBLocatorValue(value internallob.LOBLocatorBindValue) (lobLocatorBind, error) {
	if value.SessionKey != m.shelf || len(value.Locator) == 0 || !internallob.ValidKind(value.Kind) {
		return lobLocatorBind{}, common.NewOracleError(oracleErrors.InvalidLobSource, nil, "LOB locator connection or metadata")
	}
	locator := append(driverCommon.B1Array(nil), value.Locator...)
	bind := lobLocatorBind{
		locator: locator,
		kind:    value.Kind,
	}
	switch value.Kind {
	case internallob.CLOB:
		if len(locator) <= koll3FlagOffset {
			return lobLocatorBind{}, common.NewOracleError(oracleErrors.InvalidLobSource, nil, "CLOB locator")
		}
		// CLOB locator binds use the same varying-width flag as streamed binds.
		locator[koll3FlagOffset] |= kolblVaryingWidthFlag
		bind.charsetForm = driverCommon.UB1(FormChar)
		bind.charsetID = m.sessionCtx.DriverCharacterSet()
	case internallob.NCLOB:
		bind.charsetForm = driverCommon.UB1(FormNChar)
		bind.charsetID = m.sessionCtx.SessionNCharCharacterSet()
	}
	return bind, nil
}

// streamInput creates, fills, and describes one temporary LOB.
//
// Parameters:
//   - ctx: context for creation and write RPCs.
//   - input: validated application input stream.
//
// Returns:
//   - lobLocatorBind: RXD and OAC metadata after a successful stream.
//   - *locator: created locator as soon as ownership exists.
//   - lobCleanupDisposition: safe cleanup action if an error follows creation.
//   - error: creation, input, encoding, or write failure.
func (m *lobManager) streamInput(
	ctx context.Context,
	input lobBindInput,
) (bind lobLocatorBind, loc *locator, disposition lobCleanupDisposition, err error) {
	switch input.kind {
	case internallob.BLOB:
		// Create a session-duration temporary BLOB and retain the executor used for
		// all writes in this bind.
		executor := m.getBlobExecutor()
		locatorBytes, createErr := executor.createTemporaryLob(ctx, false, durationSession)
		if createErr != nil {
			return bind, nil, lobCleanupAbandon, createErr
		}
		loc = newLocator(append(driverCommon.B1Array(nil), locatorBytes...), 1)
		write := func(ctx context.Context, loc *locator, payload driverCommon.B1Array) (driverCommon.UB8, error) {
			return executor.write(ctx, loc, payload)
		}
		if disposition, err = streamBlobInput(ctx, write, loc, input); err != nil {
			return bind, loc, disposition, err
		}
		// Make an empty temporary BLOB explicitly non-NULL before binding it.
		// Oracle can represent a newly-created but untouched temporary locator as
		// SQL NULL when it is bound. An explicit zero-length trim preserves the
		// public BindBlob([]byte{}) contract as an empty, non-NULL BLOB.
		if loc.offset == 1 {
			if _, trimErr := executor.trim(ctx, loc, 0); trimErr != nil {
				return bind, loc, lobCleanupAbandon, trimErr
			}
		}
		// Borrow the locator bytes until RXD encoding takes its independent snapshot.
		bind = lobLocatorBind{locator: loc.locatorBytes, kind: input.kind}
		return bind, loc, lobCleanupFreeNow, nil

	case internallob.CLOB, internallob.NCLOB:
		// Select character form and charset metadata before creating the temporary
		// locator. The same CLOB executor serves both CLOB and NCLOB operations.
		isNClob := input.kind == internallob.NCLOB
		form := driverCommon.UB2(FormChar)
		charsetID := m.sessionCtx.DriverCharacterSet()
		if isNClob {
			form = driverCommon.UB2(FormNChar)
			charsetID = m.sessionCtx.SessionNCharCharacterSet()
		}
		executor := m.getClobExecutor()
		locatorBytes, createErr := executor.createTemporaryLob(ctx, false, durationSession, form)
		if createErr != nil {
			return bind, nil, lobCleanupAbandon, createErr
		}
		loc = newLocator(append(driverCommon.B1Array(nil), locatorBytes...), 1)
		// CLOB uses the varying-width flag for the negotiated AL32UTF8 path;
		// NCLOB retains its national-character locator metadata.
		if !isNClob {
			loc.locatorBytes[koll3FlagOffset] |= kolblVaryingWidthFlag
		}
		write := func(ctx context.Context, loc *locator, isNClob bool, runes []rune) (driverCommon.UB8, error) {
			return executor.write(ctx, loc, isNClob, runes, len(runes))
		}
		if disposition, err = streamClobInput(ctx, write, executor.logicalAmount, loc, input, isNClob); err != nil {
			return bind, loc, disposition, err
		}
		// Preserve the non-NULL meaning of an explicitly empty character LOB.
		// See the BLOB branch above. This applies equally to CLOB and NCLOB;
		// zero is expressed in their UTF-16 logical-unit offset space.
		if loc.offset == 1 {
			if _, trimErr := executor.trim(ctx, loc, 0); trimErr != nil {
				return bind, loc, lobCleanupAbandon, trimErr
			}
		}
		// Carry the character form and charset ID into the LOB-specific OAC.
		bind = lobLocatorBind{
			locator:     loc.locatorBytes,
			kind:        input.kind,
			charsetForm: driverCommon.UB1(form),
			charsetID:   charsetID,
		}
		return bind, loc, lobCleanupFreeNow, nil
	default:
		return bind, nil, lobCleanupFreeNow, common.NewOracleError(oracleErrors.InvalidLobInput, nil, "LOB kind")
	}
}
