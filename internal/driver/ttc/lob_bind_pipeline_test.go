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
	"bytes"
	"context"
	"database/sql/driver"
	"testing"

	driverCommon "github.com/oracle/go-oracledb/v26/internal/driver/common"
	internallob "github.com/oracle/go-oracledb/v26/internal/lob"
	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
)

// TestLobBindPipeline_PrepareLOBLocatorBinds checks that typed LOB arguments
// are copied into TTC locator binds while the caller keeps ownership of them.
func TestLobBindPipeline_PrepareLOBLocatorBinds(t *testing.T) {
	t.Parallel()

	shelf := newShelf[driverCommon.MessageType]()
	locator := make([]byte, koll3FlagOffset+1)
	locator[koll3FlagOffset] = 0x01
	args := []driver.NamedValue{{Ordinal: 1, Value: internallob.LOBLocatorBindValue{
		SessionKey: shelf,
		Kind:       internallob.CLOB,
		Locator:    locator,
	}}}
	prepared, hasLOB, err := prepareLOBLocatorBinds(shelf, newTestSessionContext(), args)
	if err != nil {
		t.Fatalf("prepareLOBLocatorBinds returned error: %v", err)
	}
	if !hasLOB {
		t.Fatal("prepareLOBLocatorBinds did not report the locator bind")
	}
	if _, ok := args[0].Value.(internallob.LOBLocatorBindValue); !ok {
		t.Fatalf("original argument type changed to %T", args[0].Value)
	}
	bind, ok := prepared[0].Value.(lobLocatorBind)
	if !ok {
		t.Fatalf("prepared argument type = %T, want lobLocatorBind", prepared[0].Value)
	}
	if bind.kind != internallob.CLOB || bind.locator[koll3FlagOffset]&kolblVaryingWidthFlag == 0 {
		t.Fatalf("prepared CLOB locator metadata = %+v", bind)
	}
	if locator[koll3FlagOffset]&kolblVaryingWidthFlag != 0 {
		t.Fatal("preparing a CLOB bind mutated the caller's locator")
	}
}

// TestLobBindPipeline_RejectsForeignOrInvalidLocators ensures a locator cannot
// be bound through another session or with incomplete metadata.
func TestLobBindPipeline_RejectsForeignOrInvalidLocators(t *testing.T) {
	t.Parallel()

	shelf := newShelf[driverCommon.MessageType]()
	manager, err := newLobManager(shelf, newTestSessionContext())
	if err != nil {
		t.Fatalf("newLobManager: %v", err)
	}
	testCases := []struct {
		name  string
		value internallob.LOBLocatorBindValue
	}{
		{name: "foreign session", value: internallob.LOBLocatorBindValue{SessionKey: newShelf[driverCommon.MessageType](), Kind: internallob.BLOB, Locator: []byte{1}}},
		{name: "missing locator", value: internallob.LOBLocatorBindValue{SessionKey: shelf, Kind: internallob.BLOB}},
		{name: "invalid kind", value: internallob.LOBLocatorBindValue{SessionKey: shelf, Kind: internallob.Unknown, Locator: []byte{1}}},
	}
	for _, test := range testCases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := manager.bindLOBLocatorValue(test.value); err == nil {
				t.Fatal("bindLOBLocatorValue unexpectedly accepted invalid metadata")
			} else {
				requireErrorCode(t, err, oracleErrors.InvalidLobSource)
			}
		})
	}
}

// TestLobBindPipeline_UsesBindCodecRegistry verifies locator bytes and OACs
// flow through the registered bind encoder, wrapper, and OAC factory.
func TestLobBindPipeline_UsesBindCodecRegistry(t *testing.T) {
	t.Parallel()

	shelf := newShelf[driverCommon.MessageType]()
	registerTestCodecs(shelf, 20)
	sessionContext := newTestSessionContext()
	tests := []struct {
		name        string
		kind        internallob.Kind
		dataType    DtyType
		charsetForm driverCommon.UB1
		charsetID   driverCommon.UB2
	}{
		{name: "BLOB", kind: internallob.BLOB, dataType: DtyBlob},
		{name: "CLOB", kind: internallob.CLOB, dataType: DtyClob, charsetForm: FormChar, charsetID: sessionContext.DriverCharacterSet()},
		{name: "NCLOB", kind: internallob.NCLOB, dataType: DtyClob, charsetForm: FormNChar, charsetID: sessionContext.SessionNCharCharacterSet()},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bind := lobLocatorBind{
				locator:     driverCommon.B1Array("locator"),
				kind:        test.kind,
				charsetForm: test.charsetForm,
				charsetID:   test.charsetID,
			}
			metadata, value := normalizeBindValue(bind)
			codec := shelf.GetCodecFactory()
			encoder, err := codec.getEncoderForGoType(metadata)
			if err != nil {
				t.Fatalf("get LOB encoder: %v", err)
			}
			payload, err := encoder.encodeToType(value)
			if err != nil {
				t.Fatalf("encode LOB locator: %v", err)
			}

			buffer, marshaller := newMarshalEngine(driverCommon.BIG_ENDIAN, B4, Native, 128)
			if err := encoder.wrapColumn(context.Background(), marshaller, payload); err != nil {
				t.Fatalf("wrap LOB locator: %v", err)
			}
			wire := buffer.bytes[:buffer.currentWritePosition]
			if !bytes.HasSuffix(wire, []byte("locator")) || len(wire) <= len("locator") {
				t.Fatalf("LOB locator wire bytes = %v, want length-prefixed CLR ending in locator", wire)
			}

			bindOAC, err := codec.getBindOac(metadata, driverCommon.UB4(len(payload)))
			if err != nil {
				t.Fatalf("get LOB bind OAC: %v", err)
			}
			oacs := []driverCommon.Marshallable{bindOAC}
			if err := adjustLOBLocatorBindOACs(oacs, []driver.Value{bind}); err != nil {
				t.Fatalf("adjust LOB bind OAC: %v", err)
			}
			oac := oacs[0].(*tTIoac)
			if DtyType(oac.dataType) != test.dataType || oac.characterSetForm != test.charsetForm || oac.characterSetID != test.charsetID {
				t.Fatalf("LOB bind OAC = %+v, want type=%v form=%d charset=%d", oac, test.dataType, test.charsetForm, test.charsetID)
			}
		})
	}
}

// TestLobBindPipeline_ResendsOACWhenLOBCharsetChanges checks a prepared
// statement resends bind metadata when a CLOB bind changes to an NCLOB bind.
func TestLobBindPipeline_ResendsOACWhenLOBCharsetChanges(t *testing.T) {
	t.Parallel()

	previous := newTTIoac(DtyClob, max_lob_length)
	previous.characterSetForm = FormChar
	previous.characterSetID = al32Utf8CharSet
	current := newTTIoac(DtyClob, max_lob_length)
	current.characterSetForm = FormNChar
	current.characterSetID = al16Utf16CharSet
	processor := statementProcessor{previousOacs: []driverCommon.Marshallable{previous}}

	if !processor.needToSendOACs([]driverCommon.Marshallable{current}) {
		t.Fatal("needToSendOACs = false, want resend when LOB character-set form changes")
	}
}
