/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
 */

package main

import (
	"context"
	"database/sql"
	"fmt"

	lob "github.com/oracle/go-oracledb/v26/oracle/datatype/lob"
)

// readMultipleRows uses *sql.DB and materializes each LOB before advancing to
// the next row, because Rows does not reserve its physical connection after
// Close when obtained from the pool.
func readMultipleRows(ctx context.Context, db *sql.DB, table string) error {
	rows, err := db.QueryContext(ctx,
		"SELECT id, bin, txt, ntxt FROM "+table+" ORDER BY id")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var blob lob.Blob
		var clob lob.Clob
		var nclob lob.NClob
		if err := rows.Scan(&id, &blob, &clob, &nclob); err != nil {
			return err
		}
		// Read before the next rows.Next call can move the pooled connection.
		blobBytes, err := blob.ReadAllContext(ctx)
		if err != nil {
			return err
		}
		clobText, err := clob.ReadAllContext(ctx)
		if err != nil {
			return err
		}
		nclobText, err := nclob.ReadAllContext(ctx)
		if err != nil {
			return err
		}
		_ = blob.Close()
		_ = clob.Close()
		_ = nclob.Close()
		fmt.Printf("Pooled query row %d: BLOB %d bytes, CLOB %d UTF-8 bytes, NCLOB %d UTF-8 bytes\n",
			id, len(blobBytes), len(clobText), len(nclobText))
	}
	return rows.Err()
}
