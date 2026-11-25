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
	"testing"

	driverCommon "github.com/oracle/go-oracledb/v26/internal/driver/common"
)

// TestTypeDecoder_DecodeClobUsesLOBCharsetMetadata verifies CLOB decoding uses
// UTF-16 for its LOB charset and UTF-8 for other charset IDs.
func TestTypeDecoder_DecodeClobUsesLOBCharsetMetadata(t *testing.T) {
	tests := []struct {
		name    string
		context columnContext
		data    driverCommon.B1Array
		want    string
	}{
		{
			name: "UTF-16 LOB metadata decodes big-endian payload",
			context: columnContext{LobContext: &lobColumnContext{
				CharsetID: al16Utf16CharSet,
			}},
			data: driverCommon.B1Array{0x00, 'L', 0x00, 'O', 0x00, 'B'},
			want: "LOB",
		},
		{
			name: "non-UTF-16 LOB metadata defaults to UTF-8",
			context: columnContext{LobContext: &lobColumnContext{
				CharsetID: driverCommon.UB2(873),
			}},
			data: driverCommon.B1Array("text"),
			want: "text",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := DecodeClob(test.context, test.data)
			if err != nil {
				t.Fatalf("DecodeClob() error = %v", err)
			}
			if got != test.want {
				t.Errorf("DecodeClob() = %q, want %q", got, test.want)
			}
		})
	}
}
