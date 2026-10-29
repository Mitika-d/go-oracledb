/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
**
** Subject to the condition set forth below, permission is hereby granted to any
** person obtaining a copy of this software, associated documentation and/or data
** (collectively the "Software"), free of charge and under any and all copyright
** rights and any and all patent rights owned or freely licensable by each licensor
** hereunder covering either (i) the unmodified Software as contributed to or
** provided by such licensor, or (ii) the Larger Works (as defined below), to deal
** in both
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
	"database/sql/driver"
	"errors"
	"strings"

	"github.com/oracle/go-oracledb/v26/internal/common"
	driverCommon "github.com/oracle/go-oracledb/v26/internal/driver/common"
	internallob "github.com/oracle/go-oracledb/v26/internal/lob"
	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
)

// blobLocatorBind is the private TTC-ready bind value for a BLOB locator.
type blobLocatorBind struct {
	locator driverCommon.B1Array // Owned TTC locator bytes emitted by the bind encoder.
}

// clobLocatorBind is the private TTC-ready bind value for a CLOB or NCLOB
// locator. Its charset metadata distinguishes database and national character data.
type clobLocatorBind struct {
	locator     driverCommon.B1Array // Owned TTC locator bytes emitted by the bind encoder.
	charsetForm driverCommon.UB1     // Oracle character-set form for the CLOB or NCLOB OAC.
	charsetID   driverCommon.UB2     // Session database or national character-set ID for the OAC.
}

// prepareLOBLocatorBinds resolves typed LOB values, validates their session,
// and converts them to private TTC bind values. The caller retains ownership
// of each locator.
func prepareLOBLocatorBinds(
	shelf *ttiShelf[driverCommon.MessageType],
	sessionContext *driverCommon.SessionContext,
	args []driver.NamedValue,
) ([]driver.NamedValue, bool, error) {
	var (
		prepared []driver.NamedValue
		manager  *lobManager
	)
	for index := range args {
		var value internallob.LOBLocatorBindValue
		switch typedValue := args[index].Value.(type) {
		case internallob.LOBLocatorBindValue:
			value = typedValue
		case internallob.LOBLocatorBindValueProvider:
			var err error
			value, err = typedValue.DriverLOBLocatorBindValue()
			if err != nil {
				return nil, false, err
			}
		default:
			continue
		}
		if manager == nil {
			var err error
			manager, err = newLobManager(shelf, sessionContext)
			if err != nil {
				return nil, false, err
			}
			prepared = append([]driver.NamedValue(nil), args...)
		}
		bind, err := manager.bindLOBLocatorValue(value)
		if err != nil {
			return nil, false, err
		}
		prepared[index].Value = bind
	}
	if prepared == nil {
		return args, false, nil
	}
	return prepared, true, nil
}

// finishLOBLocatorBindExecution invalidates the session after an ambiguous
// statement failure involving a caller-owned locator. The caller remains
// responsible for freeing its temporary LOB after a successful exchange.
func finishLOBLocatorBindExecution(
	shelf *ttiShelf[driverCommon.MessageType],
	hasLOBLocator bool,
	executionErr error,
) error {
	if hasLOBLocator && executionErr != nil && !isTerminalOracleError(executionErr) {
		abandonLobSession(shelf)
	}
	return executionErr
}

// isTerminalOracleError reports whether err is a direct Oracle error returned
// after the TTC exchange reached a terminal server response.
func isTerminalOracleError(err error) bool {
	for err != nil {
		if sqlErr, ok := err.(oracleErrors.SQLError); ok && strings.HasPrefix(sqlErr.ErrorCode(), "ORA-") {
			return true
		}
		err = errors.Unwrap(err)
	}
	return false
}

// encodeLOBLocatorBind returns the locator bytes from a TTC-ready LOB bind.
// bindLOBLocatorValue has already validated the bind metadata and copied the
// caller-owned locator, so this encoder can return the owned bytes directly.
//
// Parameters:
//   - value: A driver.Value containing a blobLocatorBind or clobLocatorBind.
//
// Returns:
//   - driverCommon.B1Array: The owned TTC locator bytes.
//   - error: InvalidLobSource if value is not a LOB locator bind value.
func encodeLOBLocatorBind(value driver.Value) (driverCommon.B1Array, error) {
	switch bind := value.(type) {
	case blobLocatorBind:
		return bind.locator, nil
	case clobLocatorBind:
		return bind.locator, nil
	default:
		return nil, common.NewOracleError(oracleErrors.InvalidLobSource, nil, "LOB locator bind type")
	}
}

// applyLOBLocatorBindOACMetadata applies character-set metadata to CLOB and
// NCLOB OACs and clears character-set metadata from BLOB OACs selected by
// BindOacRegistry from the private bind type.
func applyLOBLocatorBindOACMetadata(oacs []driverCommon.Marshallable, args []driver.Value) error {
	for index, value := range args {
		var charsetForm driverCommon.UB1
		var charsetID driverCommon.UB2
		switch bind := value.(type) {
		case blobLocatorBind:
			// BLOB data is binary and carries no character-set metadata.
		case clobLocatorBind:
			charsetForm = bind.charsetForm
			charsetID = bind.charsetID
		default:
			continue
		}
		if index >= len(oacs) {
			return common.NewOracleError(oracleErrors.InternalError, nil)
		}
		oac, ok := oacs[index].(*tTIoac)
		if !ok {
			return common.NewOracleError(oracleErrors.InternalError, nil)
		}
		oac.characterSetForm = charsetForm
		oac.characterSetID = charsetID
	}
	return nil
}
