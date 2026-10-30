/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
 */

package oracle

import (
	"context"
	"testing"

	datatypeLOB "github.com/oracle/go-oracledb/v26/oracle/datatype/lob"
	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
)

// TestDriver_TypedLOBArgumentValidation checks stable errors for invalid
// temporary-LOB constructor and operation arguments.
func TestDriver_TypedLOBArgumentValidation(t *testing.T) {
	t.Parallel()
	if TestingConfig == nil {
		t.Skip("No configuration available")
	}
	if _, err := datatypeLOB.CreateTemporaryBlob(context.Background(), nil); err == nil {
		t.Fatal("CreateTemporaryBlob accepted a nil connection")
	} else {
		requireLOBErrorCode(t, oracleErrors.InvalidConnection, err)
	}

	db, err := openTestDBWithConfig(TestingConfig)
	if err != nil {
		t.Fatalf("open test DB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("open dedicated connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	blob, err := datatypeLOB.CreateTemporaryBlob(ctx, conn)
	if err != nil {
		t.Fatalf("create temporary BLOB: %v", err)
	}
	t.Cleanup(func() { _ = blob.FreeContext(ctx) })
	if _, err := blob.WriteContext(ctx, []byte("payload")); err != nil {
		t.Fatalf("write temporary BLOB: %v", err)
	}
	if err := blob.TrimContext(ctx, -1); err == nil {
		t.Fatal("TrimContext accepted a negative length")
	} else {
		requireLOBErrorCode(t, oracleErrors.InvalidLOBBuffer, err)
	}
	if _, err := blob.WriteToContext(ctx, nil); err == nil {
		t.Fatal("WriteToContext accepted a nil writer")
	} else {
		requireLOBErrorCode(t, oracleErrors.InvalidLOBBuffer, err)
	}

	clob, err := datatypeLOB.CreateTemporaryClob(ctx, conn)
	if err != nil {
		t.Fatalf("create temporary CLOB: %v", err)
	}
	t.Cleanup(func() { _ = clob.FreeContext(ctx) })
	if _, err := clob.WriteContext(ctx, []byte{0xff}); err == nil {
		t.Fatal("CLOB WriteContext accepted invalid UTF-8")
	} else {
		requireLOBErrorCode(t, oracleErrors.InvalidLobInput, err)
	}
}
