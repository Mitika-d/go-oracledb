/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
 */

package oracle

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	datatypeLOB "github.com/oracle/go-oracledb/v26/oracle/datatype/lob"
)

// TestDriver_TypedLOBPersistentWriteTo verifies multi-chunk reads from
// persistent typed locators after the source Rows are closed.
func TestDriver_TypedLOBPersistentWriteTo(t *testing.T) {
	t.Parallel()
	if TestingConfig == nil {
		t.Skip("No configuration available")
	}
	dsn := TestingConfig.GetConnectionString()
	separator := "?"
	if strings.Contains(dsn, "?") {
		separator = "&"
	}
	dsn += separator + "oracle.go.DriverProperties.DefaultLobPrefetchSize=8"
	db, err := sql.Open(TestingConfig.Driver.Name, dsn)
	if err != nil {
		t.Fatalf("open test DB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping test DB: %v", err)
	}

	table := createObjectName("lob_persistent")
	if err := createTable(ctx, db, table, map[string]string{
		"id": "NUMBER PRIMARY KEY", "bin": "BLOB", "txt": "CLOB", "ntxt": "NCLOB",
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	registerLOBTableCleanup(t, db, table)

	blobWant := bytes.Repeat([]byte{0, 1, 127, 128, 254, 255}, 20*1024)
	clobWant := strings.Repeat("CLOB café 中 🙂 ", 8*1024)
	nclobWant := strings.Repeat("NCLOB नमस्ते 🌏 ", 8*1024)
	insert := fmt.Sprintf("INSERT INTO %s (id, bin, txt, ntxt) VALUES (:1, :2, :3, :4)", table)
	if _, err := db.ExecContext(ctx, insert, int64(1), BindBlob(blobWant), BindClob(clobWant), BindNClob(nclobWant)); err != nil {
		t.Fatalf("insert persistent LOBs: %v", err)
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

	rows, err := conn.QueryContext(ctx, fmt.Sprintf("SELECT bin, txt, ntxt FROM %s WHERE id = :1", table), int64(1))
	if err != nil {
		t.Fatalf("query persistent LOBs: %v", err)
	}
	if !rows.Next() {
		_ = rows.Close()
		t.Fatalf("query returned no row: %v", rows.Err())
	}
	if err := rows.Scan(&blob, &clob, &nclob); err != nil {
		_ = rows.Close()
		t.Fatalf("scan persistent LOBs: %v", err)
	}
	if rows.Next() {
		_ = rows.Close()
		t.Fatal("query returned more than one row")
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatalf("iterate persistent LOB rows: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close source rows: %v", err)
	}

	var blobGot, clobGot, nclobGot bytes.Buffer
	if n, err := blob.WriteToContext(ctx, &blobGot); err != nil || n != int64(len(blobWant)) {
		t.Fatalf("BLOB WriteTo = (%d, %v), want (%d, nil)", n, err, len(blobWant))
	}
	if n, err := clob.WriteToContext(ctx, &clobGot); err != nil || n != int64(len(clobWant)) {
		t.Fatalf("CLOB WriteTo = (%d, %v), want (%d, nil)", n, err, len(clobWant))
	}
	if n, err := nclob.WriteToContext(ctx, &nclobGot); err != nil || n != int64(len(nclobWant)) {
		t.Fatalf("NCLOB WriteTo = (%d, %v), want (%d, nil)", n, err, len(nclobWant))
	}
	if !bytes.Equal(blobGot.Bytes(), blobWant) || clobGot.String() != clobWant || nclobGot.String() != nclobWant {
		t.Fatal("persistent typed LOB payload mismatch after Rows.Close")
	}
}
