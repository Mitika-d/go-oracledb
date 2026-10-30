/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
 */

package oracle

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	lobpkg "github.com/oracle/go-oracledb/v26/oracle/datatype/lob"
)

// TestDriver_LOBBindLargeAndEmptyValues checks large BLOB, CLOB, and NCLOB
// marker binds, then verifies that empty marker values remain empty LOBs rather
// than becoming SQL NULL.
func TestDriver_LOBBindLargeAndEmptyValues(t *testing.T) {
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
	table := createObjectName("lob_bind")
	if err := createTable(ctx, db, table, map[string]string{
		"id": "NUMBER PRIMARY KEY", "bin": "BLOB", "txt": "CLOB", "ntxt": "NCLOB",
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	registerLOBTableCleanup(t, db, table)

	blobWant := bytes.Repeat([]byte{0x00, 0x41, 0x80, 0xff}, 32*1024+1)
	clobWant := strings.Repeat("CLOB Aé中🙂 ", 16*1024)
	nclobWant := strings.Repeat("NCLOB Aé中🙂 ", 16*1024)
	insert := fmt.Sprintf("INSERT INTO %s (id, bin, txt, ntxt) VALUES (:1, :2, :3, :4)", table)
	if _, err := db.ExecContext(ctx, insert, int64(1), BindBlob(blobWant), BindClob(clobWant), BindNClob(nclobWant)); err != nil {
		t.Fatalf("insert large marker values: %v", err)
	}

	var blobGot lobpkg.Bytes
	var clobGot, nclobGot lobpkg.Text
	selectValues := fmt.Sprintf("SELECT bin, txt, ntxt FROM %s WHERE id = :1", table)
	if err := db.QueryRowContext(ctx, selectValues, int64(1)).Scan(&blobGot, &clobGot, &nclobGot); err != nil {
		t.Fatalf("scan inserted values: %v", err)
	}
	if !bytes.Equal(blobGot, blobWant) || string(clobGot) != clobWant || string(nclobGot) != nclobWant {
		t.Fatal("large marker bind round trip mismatch")
	}

	update := fmt.Sprintf("UPDATE %s SET bin = :1, txt = :2, ntxt = :3 WHERE id = :4", table)
	if _, err := db.ExecContext(ctx, update, BindBlob([]byte{}), BindClob(""), BindNClob(""), int64(1)); err != nil {
		t.Fatalf("update empty marker values: %v", err)
	}
	var binNull, txtNull, nclobNull int
	var binLength, txtLength, nclobLength int64
	emptyState := fmt.Sprintf(`SELECT
		CASE WHEN bin IS NULL THEN 1 ELSE 0 END, DBMS_LOB.GETLENGTH(bin),
		CASE WHEN txt IS NULL THEN 1 ELSE 0 END, DBMS_LOB.GETLENGTH(txt),
		CASE WHEN ntxt IS NULL THEN 1 ELSE 0 END, DBMS_LOB.GETLENGTH(ntxt)
		FROM %s WHERE id = :1`, table)
	if err := db.QueryRowContext(ctx, emptyState, int64(1)).Scan(
		&binNull, &binLength, &txtNull, &txtLength, &nclobNull, &nclobLength,
	); err != nil {
		t.Fatalf("query empty LOB state: %v", err)
	}
	if binNull != 0 || txtNull != 0 || nclobNull != 0 || binLength != 0 || txtLength != 0 || nclobLength != 0 {
		t.Fatalf("empty LOB state = nulls(%d,%d,%d), lengths(%d,%d,%d); want all zero",
			binNull, txtNull, nclobNull, binLength, txtLength, nclobLength)
	}
	if err := db.QueryRowContext(ctx, selectValues, int64(1)).Scan(&blobGot, &clobGot, &nclobGot); err != nil {
		t.Fatalf("scan empty marker values: %v", err)
	}
	if len(blobGot) != 0 || clobGot != "" || nclobGot != "" {
		t.Fatalf("empty marker values = (%x, %q, %q)", blobGot, clobGot, nclobGot)
	}
}
