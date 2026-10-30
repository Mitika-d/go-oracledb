/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
**
** See the LICENSE file for license terms.
 */

package ttc

import (
	"testing"

	driverCommon "github.com/oracle/go-oracledb/v26/internal/driver/common"
)

func TestDecodeClobUsesLOBCharsetMetadata(t *testing.T) {
	tests := []struct {
		name    string
		context columnContext
		data    driverCommon.B1Array
		want    string
	}{
		{
			name: "missing LOB metadata defaults to UTF-8",
			data: driverCommon.B1Array("hello"),
			want: "hello",
		},
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
