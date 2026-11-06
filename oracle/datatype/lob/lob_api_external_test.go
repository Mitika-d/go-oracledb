package lob_test

import (
	"testing"

	lob "github.com/oracle/go-oracledb/v26/oracle/datatype/lob"
)

func TestPublicTypedLOBZeroValues(t *testing.T) {
	var blob lob.Blob
	var clob lob.Clob
	var nclob lob.NClob
	if blob.Kind() != lob.KindBLOB || clob.Kind() != lob.KindCLOB || nclob.Kind() != lob.KindNCLOB {
		t.Fatalf("zero-value kinds = (%v, %v, %v)", blob.Kind(), clob.Kind(), nclob.Kind())
	}
	if blob.Valid() || clob.Valid() || nclob.Valid() {
		t.Fatal("zero-value typed LOB should be invalid")
	}
	if err := blob.Scan(nil); err != nil {
		t.Fatalf("Blob.Scan(nil) error = %v", err)
	}
	if err := clob.Scan(nil); err != nil {
		t.Fatalf("Clob.Scan(nil) error = %v", err)
	}
	if err := nclob.Scan(nil); err != nil {
		t.Fatalf("NClob.Scan(nil) error = %v", err)
	}
	if blob.Valid() || clob.Valid() || nclob.Valid() {
		t.Fatal("NULL scan should leave typed LOBs invalid")
	}
}
