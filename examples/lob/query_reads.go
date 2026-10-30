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

// readPersistentLOBs scans locators on a dedicated connection, closes Rows,
// then reads from the still-live physical session.
func readPersistentLOBs(ctx context.Context, db *sql.DB, table string, id int64) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	rows, err := conn.QueryContext(ctx,
		"SELECT bin, txt, ntxt FROM "+table+" WHERE id = :1", id)
	if err != nil {
		return err
	}
	if !rows.Next() {
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		return fmt.Errorf("no LOB row found for id %d", id)
	}

	var blob lob.Blob
	var clob lob.Clob
	var nclob lob.NClob
	if err := rows.Scan(&blob, &clob, &nclob); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	defer blob.Close()
	defer clob.Close()
	defer nclob.Close()

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
	fmt.Printf("Dedicated connection: BLOB %d bytes, CLOB %d UTF-8 bytes, NCLOB %d UTF-8 bytes\n",
		len(blobBytes), len(clobText), len(nclobText))
	return nil
}
