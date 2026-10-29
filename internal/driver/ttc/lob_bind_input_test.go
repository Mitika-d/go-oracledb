/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
**
** Subject to the condition set forth below, permission is hereby granted to any
** person obtaining a copy of this software, associated documentation and/or data
** (collectively the "Software"), free of charge and under any and all copyright
** rights in the Software, and under any and all patent rights owned or freely
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
	"errors"
	"io"
	"strings"
	"testing"

	internallob "github.com/oracle/go-oracledb/v26/internal/lob"
	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
)

// TestLobBindInput_ValidateRejectsInvalidMetadata verifies invalid readers,
// LOB kinds, and byte sizes are rejected before the source is read.
func TestLobBindInput_ValidateRejectsInvalidMetadata(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		source      io.Reader
		kind        internallob.Kind
		byteSize    int64
		wantMessage string
	}{
		{
			name:        "missing reader",
			kind:        internallob.BLOB,
			byteSize:    7,
			wantMessage: "reader",
		},
		{
			name:        "invalid kind",
			source:      strings.NewReader("payload"),
			kind:        internallob.Unknown,
			byteSize:    7,
			wantMessage: "LOB kind",
		},
		{
			name:        "negative size",
			source:      strings.NewReader("payload"),
			kind:        internallob.BLOB,
			byteSize:    -1,
			wantMessage: "size marker",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := newLobBindInput(test.source, test.kind, test.byteSize)
			err := input.validate()
			if err == nil {
				t.Fatal("validate() returned nil")
			}

			var sqlErr oracleErrors.SQLError
			if !errors.As(err, &sqlErr) {
				t.Fatalf("error = %T, want Oracle SQL error", err)
			}
			if got, want := sqlErr.ErrorCode(), string(oracleErrors.InvalidLobInput); got != want {
				t.Errorf("error code = %q, want %q", got, want)
			}
			if !strings.Contains(err.Error(), test.wantMessage) {
				t.Errorf("error = %q, want it to contain %q", err, test.wantMessage)
			}
			if reader, ok := test.source.(*strings.Reader); ok && reader.Len() != len("payload") {
				t.Errorf("validation consumed the source: remaining bytes = %d, want %d", reader.Len(), len("payload"))
			}
		})
	}
}

// TestLobBindInput_ValidateAcceptsValidInputs verifies each LOB kind and an
// empty BLOB are accepted while preserving their source metadata.
func TestLobBindInput_ValidateAcceptsValidInputs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		source   io.Reader
		kind     internallob.Kind
		byteSize int64
	}{
		{name: "BLOB", source: bytes.NewReader([]byte{1, 2, 3}), kind: internallob.BLOB, byteSize: 3},
		{name: "empty BLOB", source: bytes.NewReader(nil), kind: internallob.BLOB, byteSize: 0},
		{name: "CLOB", source: strings.NewReader("CLOB"), kind: internallob.CLOB, byteSize: 4},
		{name: "NCLOB", source: strings.NewReader("NCLOB"), kind: internallob.NCLOB, byteSize: 5},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := newLobBindInput(test.source, test.kind, test.byteSize)
			if input.source != test.source {
				t.Fatal("input source differs from the supplied reader")
			}
			if input.kind != test.kind {
				t.Errorf("input kind = %v, want %v", input.kind, test.kind)
			}
			if input.byteSize != test.byteSize {
				t.Errorf("input byte size = %d, want %d", input.byteSize, test.byteSize)
			}
			if err := input.validate(); err != nil {
				t.Fatalf("validate() error = %v, want nil", err)
			}
		})
	}
}
