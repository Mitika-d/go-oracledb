/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
**
** See the LICENSE file for license terms.
 */

package oracle_test

import (
	"bytes"
	"testing"

	"github.com/oracle/go-oracledb/v26/oracle"
)

// TestPublicLOBBindMarkers verifies application packages can construct each
// public marker without importing the driver's internal LOB package.
func TestPublicLOBBindMarkers(t *testing.T) {
	blob := oracle.BindBlob([]byte{1, 2, 3})
	clob := oracle.BindClob("CLOB text 🙂")
	nclob := oracle.BindNClob("NCLOB text 🙂")

	if !bytes.Equal(blob, []byte{1, 2, 3}) {
		t.Fatalf("BindBlob = %v, want [1 2 3]", blob)
	}
	if string(clob) != "CLOB text 🙂" {
		t.Fatalf("BindClob = %q, want CLOB text 🙂", clob)
	}
	if string(nclob) != "NCLOB text 🙂" {
		t.Fatalf("BindNClob = %q, want NCLOB text 🙂", nclob)
	}
}
