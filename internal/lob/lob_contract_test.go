/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
**
** See the LICENSE file for license terms.
 */

package lob

import "testing"

// TestValidKind covers every supported LOB family and rejects absent or
// unrecognized kind values.
func TestValidKind(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		kind Kind
		want bool
	}{
		{name: "unknown", kind: Unknown, want: false},
		{name: "blob", kind: BLOB, want: true},
		{name: "clob", kind: CLOB, want: true},
		{name: "nclob", kind: NCLOB, want: true},
		{name: "out of range", kind: Kind(255), want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := ValidKind(test.kind); got != test.want {
				t.Errorf("ValidKind(%d) = %t, want %t", test.kind, got, test.want)
			}
		})
	}
}
