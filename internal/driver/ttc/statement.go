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
	"sync"
	"time"

	"github.com/oracle/go-oracledb/v26/internal/common"
	driverCommon "github.com/oracle/go-oracledb/v26/internal/driver/common"
	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
)

const (
	// The timeout for statement cancellation
	cancelTimeout = time.Second * 10
)

// statementCancellationContextKey stores per-execution cancellation state on a
// statement context without colliding with caller-provided context values.
type statementCancellationContextKey struct{}

// statementCancellationResult carries the timeout-bounded context created by
// the cancellation after-function and the matching cancel function.
type statementCancellationResult struct {
	context.Context
	context.CancelFunc
}

// statementCancellationState coordinates one statement execution's cancellation
// after-function with the execution loop that authorizes break/reset.
type statementCancellationState struct {
	startOnce sync.Once
	started   chan struct{}
	start     chan bool
	completed chan statementCancellationResult
	done      chan struct{}
}

// newStatementCancellationState creates the channels used to coordinate a
// single statement execution's cancellation callback.
func newStatementCancellationState() *statementCancellationState {
	return &statementCancellationState{
		started:   make(chan struct{}),
		start:     make(chan bool, 1),
		completed: make(chan statementCancellationResult, 1),
		done:      make(chan struct{}),
	}
}

// requestBreakReset allows the after-function to run break/reset and waits for
// its timeout context. It returns false if cleanup already released the callback.
func (s *statementCancellationState) requestBreakReset() (statementCancellationResult, bool) {
	requested := false
	s.startOnce.Do(func() {
		requested = true
		s.start <- true
	})
	if !requested {
		return statementCancellationResult{}, false
	}
	return <-s.completed, true
}

// abortBreakReset releases a fired after-function without running break/reset.
func (s *statementCancellationState) abortBreakReset() {
	s.startOnce.Do(func() {
		s.start <- false
	})
}

/*
statemementCancellationFunction is a callback that attempts a server-side
break/reset to cancel the currently executing statement when the parent
context is canceled or times out.

It is invoked by the context after-function installed by
Statement.createSubContextWithCancelAfterfunction.

The provided ctx is a short-lived context (bounded by cancelTimeout) that
implementations must honor while issuing the cancellation request. The function
should return a non-nil error if the cancellation cannot be performed or
delivered; callers may use the error for logging/diagnostics.
*/
type statemementCancellationFunction func(ctx context.Context) error

// preflightLobBindShape applies the SQL or PL/SQL bind mapping before streamed
// LOB inputs can be consumed or temporary locators created. It runs only for
// LOB-bearing calls so ordinary bind handling remains unchanged.
//
// Parameters:
//   - query: parsed SQL or PL/SQL statement and its bind metadata.
//   - args: normalized arguments to check against the bind metadata.
//
// Returns:
//   - error: bind-name, ordinal, or required-value error; otherwise nil.
func preflightLobBindShape(query *qualifiedSQLStatement, args []driver.NamedValue) error {
	if query.kind == plsql {
		// PL/SQL maps arguments by unique placeholder name or ordinal.
		if _, err := extractInputBindValuesForPlSql(query.binds, args); err != nil {
			return err
		}
		return validateStreamedLobBindCompleteness(query.binds, args, true)
	}
	// SQL mapping expands a named argument to each occurrence of that placeholder.
	if _, err := extractInputBindValues(query.binds, args); err != nil {
		return err
	}
	return validateStreamedLobBindCompleteness(query.binds, args, false)
}

// normalizeAndPreflightLobBinds converts explicit LOB markers and checks bind
// shape before streamed inputs can be consumed.
//
// Parameters:
//   - args: statement arguments that may contain typed LOB markers.
//
// Returns:
//   - []driver.NamedValue: normalized statement arguments.
//   - int: index of the first streamed LOB input, or -1 when there is none.
//   - error: marker validation or bind-shape error; otherwise nil.
func (s *Statement) normalizeAndPreflightLobBinds(args []driver.NamedValue) ([]driver.NamedValue, int, error) {
	normalizedArgs, firstLobBindIndex, err := normalizeAndValidateLobBindArgs(args)
	if err != nil {
		return nil, -1, err
	}
	if firstLobBindIndex >= 0 {
		if err := preflightLobBindShape(s.qualifiedQuery, normalizedArgs); err != nil {
			return nil, -1, err
		}
	}
	return normalizedArgs, firstLobBindIndex, nil
}

// validateStreamedLobBindCompleteness checks for omitted placeholders before
// LOB readers are consumed. The bind extractors validate names, ordinals, and
// duplicates but return only supplied values, so this pass checks completeness.
//
// Parameters:
//   - binds: parsed SQL or PL/SQL placeholder metadata.
//   - args: normalized statement arguments.
//   - isPlSql: whether PL/SQL unique-name mapping applies.
//
// Returns:
//   - error: missing-value error for the first omitted placeholder; otherwise nil.
func validateStreamedLobBindCompleteness(binds *bindDetails, args []driver.NamedValue, isPlSql bool) error {
	count := len(binds.bindNames)
	if isPlSql {
		count = len(binds.uniqueNames)
	}
	filled := make([]bool, count)
	for _, arg := range args {
		if isPlSql {
			// PL/SQL has one slot per unique name; ordinals are already validated.
			index := int(arg.Ordinal) - 1
			if arg.Name != "" {
				index = int(binds.uniqueNames[arg.Name])
			}
			if index >= 0 && index < len(filled) {
				filled[index] = true
			}
			continue
		}
		// Mark every SQL occurrence because a named bind can appear more than once.
		name := arg.Name
		if name == "" && arg.Ordinal > 0 && int(arg.Ordinal) <= len(binds.bindNames) {
			name = binds.bindNames[arg.Ordinal-1]
		}
		for _, index := range binds.bindMap[name] {
			filled[index] = true
		}
	}
	if isPlSql {
		for _, name := range binds.bindNames {
			index := binds.uniqueNames[name]
			if !filled[index] {
				return common.NewOracleError(oracleErrors.StatementParsingMissingValue, nil, name, int(index)+1)
			}
		}
		return nil
	}
	for index, isFilled := range filled {
		if !isFilled {
			return common.NewOracleError(oracleErrors.StatementParsingMissingValue, nil, binds.bindNames[index], index+1)
		}
	}
	return nil
}

// Statement represents a SQL statement bound to a connection and query text.
type Statement struct {
	shelf                  *ttiShelf[driverCommon.MessageType]
	qualifiedQuery         *qualifiedSQLStatement
	stmtCancellation       statemementCancellationFunction
	queryStatementExecutor QueryWithContext
	execStatementExecutor  ExecWithContext
	_rows                  driver.Rows // reference on created Rows.
	sessionContext         *driverCommon.SessionContext
}

/*
newStatement constructs a Statement for a given SQL text.

It performs the one-time parsing/classification work needed by subsequent executions:
  - classifies the SQL text into a sqlKind (SELECT, DML, PL/SQL, etc.)
  - parses and records bind placeholders (positional and/or named)
  - selects the appropriate query/exec executors for the sqlKind and injects the shelf when supported

The returned Statement is safe to hand to database/sql as a driver.Stmt; cancellation
support is provided via stmtCancellation, which will be invoked by an after-function
installed on per-execution sub-contexts (see createSubContextWithCancelAfterfunction).

Parameters:
  - shelf: the per-connection TTC shelf used by downstream executor implementations.
  - sessionCtx: the session context used to get the session character set
  - query: the SQL text for this statement.

Returns:
  - (*Statement, nil) on success, or (nil, error) if SQL classification or placeholder parsing fails.
*/
func newStatement(
	shelf *ttiShelf[driverCommon.MessageType],
	sessionCtx *driverCommon.SessionContext,
	query string,
) (*Statement, error) {
	classifiedQ, err := newQualifiedSQLStatement(query)
	if err != nil {
		return nil, err
	}

	queryExecutor := getQueryStatementExecutorFor(classifiedQ)
	execExecutor := getExecStatementExecutorFor(classifiedQ)
	if su, ok := queryExecutor.(ttiShelfUser); ok {
		su.SetShelf(shelf)
	}
	if scu, ok := queryExecutor.(SessionContextUser); ok {
		scu.SetSessionContext(sessionCtx)
	}
	if su, ok := execExecutor.(ttiShelfUser); ok {
		su.SetShelf(shelf)
	}
	if scu, ok := execExecutor.(SessionContextUser); ok {
		scu.SetSessionContext(sessionCtx)
	}
	stmt := &Statement{
		shelf:                  shelf,
		sessionContext:         sessionCtx,
		qualifiedQuery:         classifiedQ,
		queryStatementExecutor: queryExecutor,
		execStatementExecutor:  execExecutor,
		_rows:                  nil,
	}
	// register myself to the shelf so connection close can garbage me.
	stmt.shelf.AddStatement(stmt)
	return stmt, nil
}

/*
QueryContext executes the statement as a query and returns a streaming Rows.

It is the primary entry point used by database/sql when the driver implements
driver.StmtQueryContext.

Implementation details:
  - Validates args against the parsed bind placeholders (count and names).
  - Normalizes LOB bind markers and prepares streamed inputs before the statement
    executor marshals SQL binds.
  - Creates a child context with a cancellation after-function that can attempt
    a server-side break/reset when ctx is canceled or times out.
  - Delegates execution to the queryStatementExecutor, which performs the TTC
    pipeline and returns a driver.Rows implementation.
  - Releases temporary LOB locators after the exchange reaches a known terminal
    boundary; ambiguous failures discard the session.

Callers must fully consume and Close the returned Rows.
*/
func (s *Statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	normalizedArgs, firstLobBindIndex, err := s.normalizeAndPreflightLobBinds(args)
	if err != nil {
		return nil, s.shelf.LocalizeError(err)
	}
	subContext, cancelSubContext, cleanup := s.createSubContextWithCancelAfterfunction(ctx)
	defer cleanup()

	if s.shelf.isInTransaction() {
		// if in a transaction, add an after function on the transaction context
		// that will cancel the statements context if the transaction context is
		// cancelled triggering the break/reset protocol
		stopTransAfterFunction := context.AfterFunc(s.shelf.getTransaction().getTransactionContext(), func() {
			cancelSubContext()
		})
		defer stopTransAfterFunction()
	}
	preparedArgs, preparedBinds, err := prepareStreamedLobBinds(subContext, s.shelf, s.sessionContext, normalizedArgs, firstLobBindIndex)
	if err != nil {
		return nil, s.shelf.LocalizeError(err)
	}
	selectedRows, executionErr := s.queryStatementExecutor.QueryContext(subContext, s.qualifiedQuery, preparedArgs)
	// Release bind locators only after a terminal response; ambiguous failures
	// abandon them with the physical session instead of sending cleanup RPCs.
	executionErr = releaseLobBindsAfterExecution(preparedBinds, executionErr)
	if executionErr != nil && selectedRows != nil {
		_ = selectedRows.Close()
		selectedRows = nil
	}
	if executionErr == nil {
		s._rows = selectedRows
	}

	if err := s.shelf.checkCurrentState(ctx); err != nil {
		return nil, err
	}

	if executionErr != nil {
		return nil, s.shelf.LocalizeError(executionErr)
	}
	return selectedRows, nil
}

// _closeCursor closes the statement cursorID if not 0
func (s *Statement) _closeCursor() error {
	if s.qualifiedQuery.cursorID == 0 {
		return nil
	}
	msg, err := s.shelf.GetMessageFactory().(Factory).GetMessageForFunction(TTIPFN, occa)
	if err != nil {
		common.Odl.Error("Statement.Close: GetMessageForFunction(TTIPFN,occa) failed", "error", err)
		return s.shelf.LocalizeError(err)
	}

	common.Odl.Debug("Closing cursorID", "ID", s.qualifiedQuery.cursorID)
	occaMsg := msg.(*tTIOcca)
	occaMsg.setCursorIDs([]driverCommon.UB4{driverCommon.UB4(s.qualifiedQuery.cursorID)})

	// Push (no flush; keep existing previous behavior).
	stmr := s.shelf.GetMessageStreamer().(MessageStreamerInterface)
	if err := stmr.Push(context.Background(), occaMsg); err != nil {
		common.Odl.Error("Statement.Close: Push(OCCA) failed", "error", err)
		return s.shelf.LocalizeError(err)
	}
	s.qualifiedQuery.cursorID = 0
	return nil
}

// Close implements driver.Stmt.Close.
func (s *Statement) Close() error {
	var finalErr oracleErrors.SQLError

	// close the associated rows if any
	if s._rows != nil {
		err := s._rows.Close()
		if err != nil {
			common.Odl.Debug("Failed to close rows", "error", err)
			finalErr = s.shelf.LocalizeError(common.NewOracleError(oracleErrors.RowsCloseFailed, err)).(oracleErrors.SQLError)
		}
		s._rows = nil
	}
	// close cursors
	err := s._closeCursor()
	if err != nil {
		common.Odl.Debug("Failed to close statement", "error", err)
		finalErr = s.shelf.LocalizeError(common.NewOracleError(oracleErrors.StatementCloseFailed, err)).(oracleErrors.SQLError)
	}

	s.shelf.RemoveStatement(s)

	return finalErr
}

// NumInput implements driver.Stmt.NumInput. -1 indicates unknown/variadic.
func (s *Statement) NumInput() int {
	return len(s.qualifiedQuery.binds.bindNames)
}

// CheckNamedValue allows sql.Out binds to pass through database/sql conversion.
// For all other values, we delegate back to database/sql default conversion.
func (s *Statement) CheckNamedValue(nv *driver.NamedValue) error {
	return s.shelf.LocalizeError(checkNamedValue(nv))
}

/*
ExecContext executes the statement as a non-query (DML/DDL/PLSQL) and returns a Result.

It is the primary entry point used by database/sql when the driver implements
driver.StmtExecContext.

Implementation details:
  - Validates args against the parsed bind placeholders (count and names).
  - Normalizes LOB bind markers and prepares streamed inputs before the statement
    executor marshals SQL binds.
  - Creates a child context with a cancellation after-function that can attempt
    a server-side break/reset when ctx is canceled or times out.
  - Delegates execution to the execStatementExecutor, which performs the TTC
    operations and returns a driver.Result.
  - Releases temporary LOB locators after the exchange reaches a known terminal
    boundary; ambiguous failures discard the session.

The returned Result may expose rows-affected metadata when the server provides it.
*/
func (s *Statement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	normalizedArgs, firstLobBindIndex, err := s.normalizeAndPreflightLobBinds(args)
	if err != nil {
		return nil, s.shelf.LocalizeError(err)
	}
	subContext, cancelSubContext, cleanup := s.createSubContextWithCancelAfterfunction(ctx)
	defer cleanup()

	if s.shelf.isInTransaction() {
		// if in a transaction, add an after function on the transaction context
		// that will cancel the statements context if the transaction context is
		// cancelled triggering the break/reset protocol
		stopTransAfterFunction := context.AfterFunc(s.shelf.getTransaction().getTransactionContext(), func() {
			cancelSubContext()
		})
		defer stopTransAfterFunction()
	}

	preparedArgs, preparedBinds, err := prepareStreamedLobBinds(subContext, s.shelf, s.sessionContext, normalizedArgs, firstLobBindIndex)
	if err != nil {
		return nil, s.shelf.LocalizeError(err)
	}
	// Keep temporary bind locators until execution finishes so cleanup can
	// respond to whether the TTC exchange succeeded or left the stream ambiguous.
	result, err := s.execStatementExecutor.ExecContext(subContext, s.qualifiedQuery, preparedArgs)
	err = releaseLobBindsAfterExecution(preparedBinds, err)
	if err := s.shelf.checkCurrentState(ctx); err != nil {
		return nil, err
	}
	return result, s.shelf.LocalizeError(err)
}

/*
Exec implements driver.Stmt.Exec.

database/sql calls this legacy method when context-aware execution is not used.
The implementation adapts positional []driver.Value to []driver.NamedValue
(1-based Ordinal) and delegates to ExecContext with context.Background().
*/
func (s *Statement) Exec(args []driver.Value) (driver.Result, error) {
	nvs := make([]driver.NamedValue, len(args))
	for i, v := range args {
		nvs[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return s.ExecContext(context.Background(), nvs)
}

/*
Query implements driver.Stmt.Query.

database/sql calls this legacy method when context-aware execution is not used.
The implementation adapts positional []driver.Value to []driver.NamedValue
(1-based Ordinal) and delegates to QueryContext with context.Background().
*/
func (s *Statement) Query(args []driver.Value) (driver.Rows, error) {
	nvs := make([]driver.NamedValue, len(args))
	for i, v := range args {
		nvs[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return s.QueryContext(context.Background(), nvs)
}

// createSubContextWithCancelAfterfunction creates a sub context and attaches an
// after function that will handle the statement cancellation in case of context
// cancellation. A statementCancellationState value is attached to the context
// so the execution loop can explicitly authorize the break-reset protocol when
// it observes cancellation. Cleanup releases a fired after-function when
// execution fails before the loop reaches cancellation handling.
//
// Parameters:
//   - ctx the parent context
//
// Returns:
//   - the new child context
//   - function that cancels the sub-context
//   - cleanup function that stops or releases the cancellation after-function
func (s *Statement) createSubContextWithCancelAfterfunction(ctx context.Context) (context.Context, context.CancelFunc, func()) {
	cancellationState := newStatementCancellationState()
	subContext := context.WithValue(ctx, statementCancellationContextKey{}, cancellationState)

	common.Odl.Debug("Creating cancellable sub context")
	subContext, cancelSubContext := context.WithCancel(subContext)

	// attach and after function to the new context
	stop := context.AfterFunc(subContext, func() {
		defer close(cancellationState.done)
		ctx, cancel := context.WithTimeout(context.Background(), cancelTimeout)
		// Wait until we can start
		common.Odl.Debug("Break-reset after function started")
		close(cancellationState.started)
		if start := <-cancellationState.start; !start {
			cancel()
			return
		}
		common.Odl.Debug("Start break-reset")
		err := s.shelf.cancelExecution(ctx)
		if err != nil {
			common.Odl.Error("Error during statement cancellation.", "error", err)
		}
		common.Odl.Debug("Break-reset completed")
		// Allow statement execution to continue
		cancellationState.completed <- statementCancellationResult{ctx, cancel}
	})
	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(func() {
			if !stop() {
				cancellationState.abortBreakReset()
				<-cancellationState.done
			}
			cancelSubContext()
		})
	}
	// return the subcontext, its cancel function, and after-function cleanup
	return subContext, cancelSubContext, cleanup
}
