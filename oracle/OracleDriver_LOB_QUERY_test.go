/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
 */

package oracle

import (
	"context"
	"fmt"
	"testing"

	datatypeLOB "github.com/oracle/go-oracledb/v26/oracle/datatype/lob"
)

// TestDriver_TypedLOBQueryRowScans verifies QueryRow.Scan adopts persistent
// locators for all three public scanner types and permits reads afterward.
func TestDriver_TypedLOBQueryRowScans(t *testing.T) {
	t.Parallel()
	if TestingConfig == nil {
		t.Skip("No configuration available")
	}
	db, err := openTestDBWithConfig(TestingConfig)
	if err != nil {
		t.Fatalf("open test DB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	table := createObjectName("lob_query")
	if err := createTable(ctx, db, table, map[string]string{
		"id": "NUMBER PRIMARY KEY", "bin": "BLOB", "txt": "CLOB", "ntxt": "NCLOB",
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	registerLOBTableCleanup(t, db, table)

	blobWant := []byte{0, 1, 127, 128, 254, 255}
	clobWant := "CLOB café 🙂"
	nclobWant := "NCLOB नमस्ते 🌏"
	insert := fmt.Sprintf("INSERT INTO %s (id, bin, txt, ntxt) VALUES (:1, :2, :3, :4)", table)
	if _, err := db.ExecContext(ctx, insert, int64(1), BindBlob(blobWant), BindClob(clobWant), BindNClob(nclobWant)); err != nil {
		t.Fatalf("insert typed LOBs: %v", err)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("get dedicated connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	var blob datatypeLOB.Blob
	var clob datatypeLOB.Clob
	var nclob datatypeLOB.NClob
	t.Cleanup(func() { _ = blob.Close() })
	t.Cleanup(func() { _ = clob.Close() })
	t.Cleanup(func() { _ = nclob.Close() })
	query := fmt.Sprintf("SELECT bin, txt, ntxt FROM %s WHERE id = :1", table)
	if err := conn.QueryRowContext(ctx, query, int64(1)).Scan(&blob, &clob, &nclob); err != nil {
		t.Fatalf("QueryRow.Scan typed LOBs: %v", err)
	}
	if !blob.Valid() || blob.Kind() != datatypeLOB.KindBLOB ||
		!clob.Valid() || clob.Kind() != datatypeLOB.KindCLOB ||
		!nclob.Valid() || nclob.Kind() != datatypeLOB.KindNCLOB {
		t.Fatal("QueryRow.Scan did not adopt the expected typed LOB values")
	}
	if got := readTypedLOBContents(t, ctx, &blob); string(got) != string(blobWant) {
		t.Fatalf("QueryRow BLOB = %x, want %x", got, blobWant)
	}
	if got := string(readTypedLOBContents(t, ctx, &clob)); got != clobWant {
		t.Fatalf("QueryRow CLOB = %q, want %q", got, clobWant)
	}
	if got := string(readTypedLOBContents(t, ctx, &nclob)); got != nclobWant {
		t.Fatalf("QueryRow NCLOB = %q, want %q", got, nclobWant)
	}
}
