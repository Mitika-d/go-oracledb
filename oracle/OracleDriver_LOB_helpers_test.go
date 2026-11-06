/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
 */

package oracle

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
)

func registerLOBTableCleanup(t *testing.T, db *sql.DB, table string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := dropTable(ctx, db, table); err != nil {
			t.Errorf("drop table %s: %v", table, err)
		}
	})
}

func requireLOBErrorCode(t *testing.T, want oracleErrors.ErrorCode, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error code %s, got nil", want)
	}
	var sqlErr oracleErrors.SQLError
	if !errors.As(err, &sqlErr) {
		t.Fatalf("expected SQL error %s, got %T: %v", want, err, err)
	}
	if got := sqlErr.ErrorCode(); got != string(want) {
		t.Fatalf("error code = %s, want %s (error: %v)", got, want, err)
	}
}
