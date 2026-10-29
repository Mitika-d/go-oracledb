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

// lobLocatorBind is the TTC bind value for a caller-owned typed LOB locator.
type lobLocatorBind struct {
	locator     driverCommon.B1Array
	kind        internallob.Kind
	charsetForm driverCommon.UB1
	charsetID   driverCommon.UB2
}

// prepareLOBLocatorBinds copies args and converts typed LOB locators to the
// private TTC representation. The caller retains ownership of each locator.
func prepareLOBLocatorBinds(
	shelf *ttiShelf[driverCommon.MessageType],
	sessionContext *driverCommon.SessionContext,
	args []driver.NamedValue,
) ([]driver.NamedValue, bool, error) {
	firstLOBIndex := -1
	for index := range args {
		if _, ok := args[index].Value.(internallob.LOBLocatorBindValue); ok {
			firstLOBIndex = index
			break
		}
	}
	if firstLOBIndex < 0 {
		return args, false, nil
	}

	prepared := append([]driver.NamedValue(nil), args...)
	manager, err := newLobManager(shelf, sessionContext)
	if err != nil {
		return nil, false, err
	}
	for index := firstLOBIndex; index < len(prepared); index++ {
		value, ok := prepared[index].Value.(internallob.LOBLocatorBindValue)
		if !ok {
			continue
		}
		bind, err := manager.bindLOBLocatorValue(value)
		if err != nil {
			return nil, false, err
		}
		prepared[index].Value = bind
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

// encodeLOBLocatorBind supplies the locator bytes to the registered LOB bind
// encoder. bindLOBLocatorValue has already copied and validated the locator.
func encodeLOBLocatorBind(value driver.Value) (driverCommon.B1Array, error) {
	bind, ok := value.(lobLocatorBind)
	if !ok {
		return nil, common.NewOracleError(oracleErrors.InvalidLobSource, nil, "LOB locator bind type")
	}
	return bind.locator, nil
}

// adjustLOBLocatorBindOACs fills in the kind and session charset on OACs
// created by BindOacRegistry for the private LOB locator bind type.
func adjustLOBLocatorBindOACs(oacs []driverCommon.Marshallable, args []driver.Value) error {
	for index, value := range args {
		bind, ok := value.(lobLocatorBind)
		if !ok {
			continue
		}
		if index >= len(oacs) {
			return common.NewOracleError(oracleErrors.InternalError, nil)
		}
		oac, ok := oacs[index].(*tTIoac)
		if !ok {
			return common.NewOracleError(oracleErrors.InternalError, nil)
		}
		oac.characterSetForm = bind.charsetForm
		oac.characterSetID = bind.charsetID
		if bind.kind == internallob.CLOB || bind.kind == internallob.NCLOB {
			oac.dataType = driverCommon.UB1(DtyClob)
			oac.requestedtype = int16(DtyClob)
		}
	}
	return nil
}
