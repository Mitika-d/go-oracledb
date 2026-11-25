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
	"errors"
	"testing"

	"github.com/oracle/go-oracledb/v26/internal/common"
	driverCommon "github.com/oracle/go-oracledb/v26/internal/driver/common"
	internallob "github.com/oracle/go-oracledb/v26/internal/lob"
	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
)

type testLOBLocatorBindValueProvider struct {
	value internallob.LOBLocatorBindValue
	err   error
}

func (provider testLOBLocatorBindValueProvider) DriverLOBLocatorBindValue() (internallob.LOBLocatorBindValue, error) {
	return provider.value, provider.err
}

// TestLobBindPipeline_CheckNamedValueAcceptsLocator verifies database/sql
// preserves typed locator providers and that preparation reports provider errors.
func TestLobBindPipeline_CheckNamedValueAcceptsLocator(t *testing.T) {
	t.Parallel()

	shelf := newShelf[driverCommon.MessageType]()
	locator := internallob.LOBLocatorBindValue{
		SessionKey: shelf,
		Kind:       internallob.BLOB,
		Locator:    []byte{1, 2, 3},
	}
	namedValue := &driver.NamedValue{Ordinal: 1, Value: testLOBLocatorBindValueProvider{value: locator}}
	if err := checkNamedValue(namedValue); err != nil {
		t.Fatalf("checkNamedValue(locator provider) returned error: %v", err)
	}
	if _, ok := namedValue.Value.(testLOBLocatorBindValueProvider); !ok {
		t.Fatalf("checkNamedValue changed locator provider to %T", namedValue.Value)
	}

	providerErr := errors.New("locator unavailable")
	namedValue = &driver.NamedValue{Ordinal: 1, Value: testLOBLocatorBindValueProvider{err: providerErr}}
	if err := checkNamedValue(namedValue); err != nil {
		t.Fatalf("checkNamedValue(locator provider error) = %v, want nil until preparation", err)
	}
	if _, _, err := prepareLOBLocatorBinds(shelf, newTestSessionContext(), []driver.NamedValue{*namedValue}); !errors.Is(err, providerErr) {
		t.Fatalf("prepareLOBLocatorBinds(locator provider error) = %v, want %v", err, providerErr)
	}
}

// TestLobBindPipeline_PrepareTypedLOBLocatorBinds checks that typed LOB arguments
// are copied into TTC locator binds while the caller keeps ownership of them.
func TestLobBindPipeline_PrepareTypedLOBLocatorBinds(t *testing.T) {
	t.Parallel()

	shelf := newShelf[driverCommon.MessageType]()
	sessionContext := newTestSessionContext()
	tests := []struct {
		name             string
		kind             internallob.Kind
		charsetForm      driverCommon.UB1
		charsetID        driverCommon.UB2
		varyingWidthFlag bool
	}{
		{name: "BLOB", kind: internallob.BLOB},
		{name: "CLOB", kind: internallob.CLOB, charsetForm: FormChar, charsetID: sessionContext.DriverCharacterSet(), varyingWidthFlag: true},
		{name: "NCLOB", kind: internallob.NCLOB, charsetForm: FormNChar, charsetID: sessionContext.SessionNCharCharacterSet()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			locator := make([]byte, koll3FlagOffset+1)
			locator[koll3FlagOffset] = 0x01
			args := []driver.NamedValue{{Ordinal: 1, Value: internallob.LOBLocatorBindValue{
				SessionKey: shelf,
				Kind:       test.kind,
				Locator:    locator,
			}}}
			prepared, hasLOB, err := prepareLOBLocatorBinds(shelf, sessionContext, args)
			if err != nil {
				t.Fatalf("prepareLOBLocatorBinds returned error: %v", err)
			}
			if !hasLOB {
				t.Fatal("prepareLOBLocatorBinds did not report the locator bind")
			}
			if _, ok := args[0].Value.(internallob.LOBLocatorBindValue); !ok {
				t.Fatalf("original argument type changed to %T", args[0].Value)
			}

			var preparedLocator driverCommon.B1Array
			switch bind := prepared[0].Value.(type) {
			case blobLocatorBind:
				if test.kind != internallob.BLOB {
					t.Fatalf("prepared bind type = blobLocatorBind for %s", test.name)
				}
				preparedLocator = bind.locator
			case clobLocatorBind:
				if test.kind == internallob.BLOB {
					t.Fatal("prepared BLOB bind has character-LOB type")
				}
				if bind.charsetForm != test.charsetForm || bind.charsetID != test.charsetID {
					t.Fatalf("prepared %s charset = form %d, ID %d; want form %d, ID %d", test.name, bind.charsetForm, bind.charsetID, test.charsetForm, test.charsetID)
				}
				preparedLocator = bind.locator
			default:
				t.Fatalf("prepared argument type = %T, want a LOB locator bind", prepared[0].Value)
			}
			if got := preparedLocator[koll3FlagOffset]&kolblVaryingWidthFlag != 0; got != test.varyingWidthFlag {
				t.Fatalf("prepared %s varying-width flag = %t, want %t", test.name, got, test.varyingWidthFlag)
			}
			if &preparedLocator[0] == &locator[0] {
				t.Fatal("prepared bind shares the caller's locator storage")
			}
			if locator[koll3FlagOffset]&kolblVaryingWidthFlag != 0 {
				t.Fatalf("preparing a %s bind mutated the caller's locator", test.name)
			}
		})
	}
}

// TestLobBindPipeline_RejectsInvalidLocatorMetadata ensures a locator cannot
// be bound through another session or with incomplete metadata.
func TestLobBindPipeline_RejectsInvalidLocatorMetadata(t *testing.T) {
	t.Parallel()

	shelf := newShelf[driverCommon.MessageType]()
	manager := newLobManager(shelf, newTestSessionContext())
	testCases := []struct {
		name  string
		value internallob.LOBLocatorBindValue
	}{
		{name: "foreign session", value: internallob.LOBLocatorBindValue{SessionKey: newShelf[driverCommon.MessageType](), Kind: internallob.BLOB, Locator: []byte{1}}},
		{name: "missing locator", value: internallob.LOBLocatorBindValue{SessionKey: shelf, Kind: internallob.BLOB}},
		{name: "invalid kind", value: internallob.LOBLocatorBindValue{SessionKey: shelf, Kind: internallob.Unknown, Locator: []byte{1}}},
		{name: "uncomparable session key", value: internallob.LOBLocatorBindValue{SessionKey: []byte{1}, Kind: internallob.BLOB, Locator: []byte{1}}},
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

// TestLobBindPipeline_PrepareLOBLocatorBindsSkipsOrdinaryValues verifies
// ordinary statements keep their original argument slice and need no work.
func TestLobBindPipeline_PrepareLOBLocatorBindsSkipsOrdinaryValues(t *testing.T) {
	t.Parallel()

	args := []driver.NamedValue{{Ordinal: 1, Value: int64(7)}}
	prepared, hasLocator, err := prepareLOBLocatorBinds(newShelf[driverCommon.MessageType](), newTestSessionContext(), args)
	if err != nil || hasLocator || &prepared[0] != &args[0] {
		t.Fatalf("ordinary bind preparation = (%v, %t, %v), want original args and no locator", prepared, hasLocator, err)
	}
}

// TestLobBindPipeline_FinishLocatorBindExecution verifies only ambiguous
// failures involving typed locators invalidate the physical session.
func TestLobBindPipeline_FinishLocatorBindExecution(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		shelf := newShelf[driverCommon.MessageType]()
		if err := finishLOBLocatorBindExecution(shelf, true, nil); err != nil || shelf.lobState.isInvalid() {
			t.Fatalf("successful locator execution = (%v, invalid %t)", err, shelf.lobState.isInvalid())
		}
	})
	t.Run("terminal Oracle error", func(t *testing.T) {
		shelf := newShelf[driverCommon.MessageType]()
		serverErr := common.NewOracleError(oracleErrors.ErrorCode("ORA-00001"), nil)
		if err := finishLOBLocatorBindExecution(shelf, true, serverErr); err != serverErr || shelf.lobState.isInvalid() {
			t.Fatalf("terminal error result = (%v, invalid %t)", err, shelf.lobState.isInvalid())
		}
	})
	t.Run("ambiguous transport error", func(t *testing.T) {
		shelf := newShelf[driverCommon.MessageType]()
		transportErr := common.NewOracleError(oracleErrors.StreamerReadError, errors.New("EOF"))
		if err := finishLOBLocatorBindExecution(shelf, true, transportErr); err != transportErr || !shelf.lobState.isInvalid() {
			t.Fatalf("transport error result = (%v, invalid %t)", err, shelf.lobState.isInvalid())
		}
	})
	if isTerminalOracleError(common.NewOracleError(oracleErrors.RunExecError, common.NewOracleError(oracleErrors.ErrorCode("ORA-00001"), nil))) {
		t.Fatal("wrapped Oracle error was treated as a terminal TTC response")
	}
}

// TestLobBindPipeline_UsesBindCodecRegistry verifies locator bytes and OACs
// flow through the registered bind encoder, wrapper, and OAC factory.
func TestLobBindPipeline_UsesBindCodecRegistry(t *testing.T) {
	t.Parallel()

	shelf := newShelf[driverCommon.MessageType]()
	registerTestCodecs(shelf, 20)
	sessionContext := newTestSessionContext()
	locator := driverCommon.B1Array("locator")
	tests := []struct {
		name        string
		value       driver.Value
		dataType    DtyType
		charsetForm driverCommon.UB1
		charsetID   driverCommon.UB2
	}{
		{name: "BLOB", value: blobLocatorBind{locator: locator}, dataType: DtyBlob},
		{name: "CLOB", value: clobLocatorBind{locator: locator, charsetForm: FormChar, charsetID: sessionContext.DriverCharacterSet()}, dataType: DtyClob, charsetForm: FormChar, charsetID: sessionContext.DriverCharacterSet()},
		{name: "NCLOB", value: clobLocatorBind{locator: locator, charsetForm: FormNChar, charsetID: sessionContext.SessionNCharCharacterSet()}, dataType: DtyClob, charsetForm: FormNChar, charsetID: sessionContext.SessionNCharCharacterSet()},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			metadata, value := normalizeBindValue(test.value)
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
			if err := applyLOBLocatorBindOACMetadata(oacs, []driver.Value{test.value}); err != nil {
				t.Fatalf("apply LOB bind charset: %v", err)
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

// TestLobBindPipeline_FreeLobReferenceDefersAndInvalidatesLocator verifies
// Free queues a temporary locator for piggybacking and clears its local flags.
func TestLobBindPipeline_FreeLobReferenceDefersAndInvalidatesLocator(t *testing.T) {
	t.Parallel()

	shelf := newShelf[driverCommon.MessageType]()
	streamer := NewMessageStreamer(shelf)
	shelf.RegisterMessageStreamer(streamer)
	loc := newTestLobReferenceLocator(101)
	loc.locatorBytes[koll1FlagOffset] |= kolblAbstractLocatorFlag
	loc.locatorBytes[koll4FlagOffset] |= kolblOpenFlagByte | kolblReadWriteFlagByte

	manager := newLobManager(shelf, newTestSessionContext())
	if err := manager.freeLobReference(loc); err != nil {
		t.Fatalf("freeLobReference returned error: %v", err)
	}
	if streamer.outgoingMessages.Len() != 0 {
		t.Fatal("freeLobReference sent a standalone TTC request")
	}
	if loc.locatorBytes[koll1FlagOffset]&kolblAbstractLocatorFlag != 0 ||
		loc.locatorBytes[koll2FlagOffset]&kolblInitializedFlag != 0 ||
		loc.locatorBytes[koll4FlagOffset]&(kolblTemporaryFlagByte|kolblOpenFlagByte|kolblReadWriteFlagByte) != 0 {
		t.Fatalf("locator flags were not cleared: %v", loc.locatorBytes)
	}
	if err := manager.freeLobReference(loc); err == nil {
		t.Fatal("second free accepted an already-freed locator")
	}
	if err := streamer.Push(context.Background(), &lobRegistryOrdinaryFunction{}); err != nil {
		t.Fatalf("Push ordinary function: %v", err)
	}
	if streamer.outgoingMessages.Len() != 2 {
		t.Fatalf("outgoing message count = %d, want piggyback plus ordinary function", streamer.outgoingMessages.Len())
	}
}
