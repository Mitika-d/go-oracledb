/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
 */

package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	lob "github.com/oracle/go-oracledb/v26/oracle/datatype/lob"
)

// writeTemporaryLOBs creates each public temporary LOB type, writes data, and
// frees the Oracle temporary storage before returning the connection.
func writeTemporaryLOBs(ctx context.Context, db *sql.DB) (retErr error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	blob, err := lob.CreateTemporaryBlob(ctx, conn)
	if err != nil {
		return err
	}
	defer func() {
		if err := blob.FreeContext(ctx); err != nil {
			retErr = errors.Join(retErr, err)
		}
	}()
	if _, err := blob.WriteContext(ctx, []byte("temporary BLOB bytes")); err != nil {
		return err
	}
	blobSize, err := blob.SizeContext(ctx)
	if err != nil {
		return err
	}

	clob, err := lob.CreateTemporaryClob(ctx, conn)
	if err != nil {
		return err
	}
	defer func() {
		if err := clob.FreeContext(ctx); err != nil {
			retErr = errors.Join(retErr, err)
		}
	}()
	if _, err := clob.WriteStringContext(ctx, "temporary CLOB café 😀"); err != nil {
		return err
	}
	clobSize, err := clob.SizeContext(ctx)
	if err != nil {
		return err
	}

	nclob, err := lob.CreateTemporaryNClob(ctx, conn)
	if err != nil {
		return err
	}
	defer func() {
		if err := nclob.FreeContext(ctx); err != nil {
			retErr = errors.Join(retErr, err)
		}
	}()
	if _, err := nclob.WriteStringContext(ctx, "temporary NCLOB नमस्ते 🌏"); err != nil {
		return err
	}
	nclobSize, err := nclob.SizeContext(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("Temporary LOBs: BLOB %d bytes, CLOB %d UTF-16 units, NCLOB %d UTF-16 units\n",
		blobSize, clobSize, nclobSize)
	return nil
}
