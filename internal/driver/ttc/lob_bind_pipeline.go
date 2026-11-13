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
	"bufio"
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/oracle/go-oracledb/v26/internal/common"
	driverCommon "github.com/oracle/go-oracledb/v26/internal/driver/common"
	internallob "github.com/oracle/go-oracledb/v26/internal/lob"
	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
)

// --- Cleanup disposition ---
// lobCleanupDisposition states whether a created locator can be released on
// the current TTC stream or must be left to session teardown.
type lobCleanupDisposition uint8

const (
	// lobCleanupFreeNow permits releasing the temporary locator on this session.
	lobCleanupFreeNow lobCleanupDisposition = iota
	// lobCleanupAbandon leaves cleanup to session teardown after an ambiguous exchange.
	lobCleanupAbandon
)

// --- LOB locator bind ---
// lobLocatorBind is the private value substituted for lobBindInput after
// its source has been copied into a session-local LOB locator. Its locator bytes
// borrow the session locator until encoding; encodeLobLocatorBind snapshots them
// before cleanup can mutate locator flags.
type lobLocatorBind struct {
	// locator borrows the locator bytes until encodeLobLocatorBind snapshots them.
	locator driverCommon.B1Array
	// kind selects the TTC datatype family used by the bind encoder.
	kind internallob.Kind
	// charsetForm identifies the database or national character form for text binds.
	charsetForm driverCommon.UB1
	// charsetID identifies the character set used to encode text bind data.
	charsetID driverCommon.UB2
}

// --- Prepared bind ownership ---
// preparedLobBinds owns the references created while preparing TTC locator
// binds. It releases them in reverse order or abandons them with the session.
type preparedLobBinds struct {
	// manager owns the session registry used to release the created locators.
	manager *lobManager
	// references are the typed session-registry obligations created for these binds.
	references []*lobReference
}

// add retains one connection-level reference immediately after temporary LOB
// creation succeeds.
//
// Parameters:
//   - loc: newly created locator to register for execution cleanup.
//
// Returns:
//   - error: non-nil when loc cannot be registered.
func (preparedBinds *preparedLobBinds) add(loc *locator) error {
	reference, err := preparedBinds.manager.retainLobReference(loc)
	if err != nil {
		return err
	}
	preparedBinds.references = append(preparedBinds.references, reference)
	return nil
}

// free releases registered locators in reverse creation order. A failed free
// discards the connection because session teardown is then the only reliable
// cleanup owner for the remaining temporary locator.
//
// Returns:
//   - error: the first cleanup error, if any.
func (preparedBinds *preparedLobBinds) free() error {
	references := preparedBinds.references
	preparedBinds.references = nil
	for index := len(references) - 1; index >= 0; index-- {
		if err := preparedBinds.manager.releaseLobReference(references[index]); err != nil {
			common.Odl.Error("temporary LOB cleanup failed", "error", err)
			preparedBinds.manager.abandonSession()
			return err
		}
	}
	return nil
}

// abandon transfers cleanup to physical-session teardown after an ambiguous
// TTC failure. It does not return an error because sending kplobTmpFree on an
// unsynchronized stream is unsafe.
func (preparedBinds *preparedLobBinds) abandon() {
	preparedBinds.references = nil
	preparedBinds.manager.abandonSession()
}

// releaseLobBindsAfterExecution frees execution-scoped LOB locators when the
// TTC exchange has reached a terminal response. Ambiguous failures abandon the
// locators with the physical session instead of sending cleanup RPCs.
//
// Parameters:
//   - preparedBinds: owner of locators created while preparing streamed binds.
//   - executionErr: result of the statement's query or execution exchange.
//
// Returns:
//   - error: executionErr, unless cleanup fails after a successful execution.
func releaseLobBindsAfterExecution(preparedBinds *preparedLobBinds, executionErr error) error {
	if preparedBinds == nil {
		return executionErr
	}
	if executionErr != nil && !isTerminalOracleError(executionErr) {
		preparedBinds.abandon()
		return executionErr
	}
	if cleanupErr := preparedBinds.free(); executionErr == nil {
		return cleanupErr
	}
	return executionErr
}

// --- Bind preparation ---
// prepareStreamedLobBinds converts streamed inputs and caller-owned LOB locators
// into locator binds before OALL/RXD marshalling begins.
//
// Parameters:
//   - ctx: context for temporary-LOB creation and streaming RPCs.
//   - shelf: session TTC state and LOB reference registry.
//   - sessionContext: character-set information for CLOB and NCLOB binds.
//   - args: normalized and validated statement arguments to copy and prepare.
//   - firstLobBindIndex: first streamed LOB argument index returned by validation.
//
// Returns:
//   - []driver.NamedValue: prepared argument copy, or nil on failure.
//   - *preparedLobBinds: owner of created locators, or nil when none exist.
//   - error: validation, streaming, or cleanup-registration error.
func prepareStreamedLobBinds(
	ctx context.Context,
	shelf *ttiShelf[driverCommon.MessageType],
	sessionContext *driverCommon.SessionContext,
	args []driver.NamedValue,
	firstLobBindIndex int,
) ([]driver.NamedValue, *preparedLobBinds, error) {
	if firstLobBindIndex < 0 {
		return args, nil, nil
	}
	// Construct the policy facade only when a streamed LOB actually needs the
	// session-owned executors and LOB reference registry.
	manager := newLobManager(shelf, sessionContext)
	return manager.prepareBinds(ctx, args, firstLobBindIndex)
}

// --- LOB input streaming ---
// streamBlobInput copies input into loc as bounded binary chunks and advances
// loc only by the amount acknowledged in TTIRPA.
//
// Parameters:
//   - ctx: context for input consumption and write RPCs.
//   - write: sends one binary chunk at loc's current offset.
//   - loc: created locator whose offset is advanced.
//   - input: validated BLOB input stream.
//
// Returns:
//   - lobCleanupDisposition: safe cleanup action after an error.
//   - error: input or write failure.
func streamBlobInput(ctx context.Context, write func(context.Context, *locator, driverCommon.B1Array) (driverCommon.UB8, error), loc *locator, input lobBindInput) (lobCleanupDisposition, error) {
	// Read into one bounded buffer and send each populated portion as one LOB
	// write. The limiter prevents a source from being over-consumed.
	limited := sizedLobReader(input)
	buffer := make([]byte, DefaultBlobLobChunkBytes)
	for {
		// Check cancellation before consuming more application input or entering a
		// new TTC write.
		if err := ctx.Err(); err != nil {
			return lobCleanupFreeNow, common.NewOracleError(oracleErrors.InvalidLobInput, err, "BLOB context")
		}
		n, readErr := limited.Read(buffer)
		if n > 0 {
			// The locator offset advances only after the server acknowledges the full
			// chunk.
			common.Odl.Debug("streamBlobInput: write chunk", "offset", loc.offset, "payloadLength", n)
			written, writeErr := write(ctx, loc, driverCommon.B1Array(buffer[:n]))
			if writeErr != nil {
				return lobCleanupAbandon, writeErr
			}
			if written != driverCommon.UB8(n) {
				return lobCleanupFreeNow, common.NewOracleError(oracleErrors.InvalidLobInput, io.ErrShortWrite, "BLOB write")
			}
			loc.offset += written
		}
		if readErr != nil {
			// EOF is valid only after the exact declared size was consumed; all other
			// reader failures are input errors.
			if errors.Is(readErr, io.EOF) {
				if limited.N != 0 {
					return lobCleanupFreeNow, common.NewOracleError(oracleErrors.InvalidLobInput, io.ErrUnexpectedEOF, "BLOB length")
				}
				return lobCleanupFreeNow, nil
			}
			return lobCleanupFreeNow, common.NewOracleError(oracleErrors.InvalidLobInput, readErr, "BLOB read")
		}
		if n == 0 {
			// A reader that reports no data and no error cannot make progress safely.
			return lobCleanupFreeNow, common.NewOracleError(oracleErrors.InvalidLobInput, io.ErrNoProgress, "BLOB progress")
		}
	}
}

// streamClobInput incrementally decodes input's UTF-8 into bounded rune chunks.
// The buffered reader retains incomplete sequences across source reads; a
// one-byte RuneError is rejected as malformed UTF-8.
//
// Parameters:
//   - ctx: context for input consumption and write RPCs.
//   - write: sends one character chunk at loc's current offset.
//   - logicalAmount: translates runes to Oracle logical units.
//   - loc: created locator whose offset is advanced.
//   - input: validated CLOB or NCLOB input stream.
//   - isNClob: selects the CLOB or NCLOB write payload path.
//
// Returns:
//   - lobCleanupDisposition: safe cleanup action after an error.
//   - error: decoding, input, or write failure.
func streamClobInput(ctx context.Context, write func(context.Context, *locator, bool, []rune) (driverCommon.UB8, error), logicalAmount func([]rune) (driverCommon.UB8, error), loc *locator, input lobBindInput, isNClob bool) (lobCleanupDisposition, error) {
	// Limit the source by bytes, then decode it incrementally so a UTF-8 sequence
	// split across underlying reads is preserved by bufio.Reader.
	limited := sizedLobReader(input)
	buffered := bufio.NewReaderSize(limited, DefaultCharacterLobChunkChars)
	runes := make([]rune, 0, DefaultCharacterLobChunkChars)
	flush := func() (lobCleanupDisposition, error) {
		if len(runes) == 0 {
			return lobCleanupFreeNow, nil
		}
		// Convert the complete rune chunk to Oracle logical units before writing it.
		expected, translateErr := logicalAmount(runes)
		if translateErr != nil {
			return lobCleanupFreeNow, common.NewOracleError(oracleErrors.InvalidLobInput, translateErr, "CLOB offset")
		}
		written, writeErr := write(ctx, loc, isNClob, runes)
		common.Odl.Debug("streamClobInput: write chunk", "offset", loc.offset, "runeCount", len(runes), "logicalAmount", expected, "isNCLOB", isNClob)
		if writeErr != nil {
			return lobCleanupAbandon, writeErr
		}
		if written != expected {
			// Do not advance the locator when Oracle acknowledges anything other than
			// the complete logical chunk.
			return lobCleanupFreeNow, common.NewOracleError(oracleErrors.InvalidLobInput, io.ErrShortWrite, "CLOB write")
		}
		loc.offset += written
		runes = runes[:0]
		return lobCleanupFreeNow, nil
	}

	for {
		// Cancellation is checked between chunks so a long source cannot continue
		// feeding the server after its statement context has ended.
		if err := ctx.Err(); err != nil {
			return lobCleanupFreeNow, common.NewOracleError(oracleErrors.InvalidLobInput, err, "CLOB context")
		}
		r, size, readErr := buffered.ReadRune()
		if readErr != nil {
			// EOF is valid only after the exact byte count was consumed. Other errors
			// are reported as input-read failures.
			if errors.Is(readErr, io.EOF) {
				if limited.N != 0 {
					return lobCleanupFreeNow, common.NewOracleError(oracleErrors.InvalidLobInput, io.ErrUnexpectedEOF, "CLOB length")
				}
				return flush()
			}
			return lobCleanupFreeNow, common.NewOracleError(oracleErrors.InvalidLobInput, readErr, "CLOB read")
		}
		if r == utf8.RuneError && size == 1 {
			// A one-byte RuneError is the decoder's signal for malformed UTF-8; a
			// valid U+FFFD encoding occupies three bytes and is accepted.
			return lobCleanupFreeNow, common.NewOracleError(oracleErrors.InvalidLobInput, errors.New("malformed UTF-8"), "UTF-8 input")
		}
		runes = append(runes, r)
		if len(runes) == cap(runes) {
			if disposition, err := flush(); err != nil {
				return disposition, err
			}
		}
	}
}

// --- Helpers ---
// isTerminalOracleError reports whether err is a direct Oracle server error
// received in a terminal TTIOER response. Statement executors return TTIOER
// errors directly, so no error-tree traversal is required here.
//
// Parameters:
//   - err: statement execution error to classify.
//
// Returns:
//   - bool: true when err is a direct ORA error; otherwise false.
func isTerminalOracleError(err error) bool {
	sqlErr, ok := err.(oracleErrors.SQLError)
	return ok && strings.HasPrefix(sqlErr.ErrorCode(), "ORA-")
}

// encodeLobLocatorBind supplies RXD payload and OAC metadata without routing a
// locator through RAW or VARCHAR encoders.
//
// Parameters:
//   - bind: prepared locator and LOB metadata.
//
// Returns:
//   - driverCommon.B1Array: copied locator payload for RXD.
//   - driverCommon.Marshallable: OAC metadata describing that payload.
func encodeLobLocatorBind(bind lobLocatorBind) (driverCommon.B1Array, driverCommon.Marshallable) {
	dtype := DtyBlob
	if bind.kind == internallob.CLOB || bind.kind == internallob.NCLOB {
		// Both character families use TTC's CLOB datatype; form and charset fields
		// distinguish CLOB from NCLOB in the OAC.
		dtype = DtyClob
	}
	// Return an independent locator payload because later cleanup mutates the
	// temporary locator's local flags.
	oac := newTTIoac(dtype, max_lob_length)
	oac.characterSetForm = bind.charsetForm
	oac.characterSetID = bind.charsetID
	return append(driverCommon.B1Array(nil), bind.locator...), oac
}
