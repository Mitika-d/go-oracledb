/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
 */

// Package main demonstrates typed persistent and temporary Oracle LOB values.
package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/oracle/go-oracledb/v26/oracle"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	dsn := os.Getenv("ORACLE_DSN")
	if dsn == "" {
		return fmt.Errorf("set ORACLE_DSN, for example: user/password@localhost:1521/freepdb1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	// Use a short prefetch to make the examples exercise locator reads after
	// the initial row prefix.
	config := oracle.NewOracleDriverConfig()
	config.ConnectDescriptor = dsn
	config.DriverProperties.DefaultLobPrefetchSize = 8
	connector, err := oracle.NewOracleConnector(config)
	if err != nil {
		return err
	}
	db := sql.OpenDB(connector)
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return err
	}

	// The identifier is generated locally and contains only characters valid
	// in an unquoted Oracle identifier.
	table := fmt.Sprintf("GO_LOB_%d", time.Now().UnixNano())
	if _, err := db.ExecContext(ctx, "CREATE TABLE "+table+" (id NUMBER PRIMARY KEY, bin BLOB, txt CLOB, ntxt NCLOB)"); err != nil {
		return err
	}
	defer func() { _, _ = db.ExecContext(context.Background(), "DROP TABLE "+table+" PURGE") }()

	blobData := bytes.Repeat([]byte("blob-data-"), 10_000)
	clobText := strings.Repeat("CLOB café 😀 ", 4_000)
	nclobText := strings.Repeat("NCLOB नमस्ते 🌏 ", 3_000)
	for id := int64(1); id <= 2; id++ {
		if _, err := db.ExecContext(ctx,
			"INSERT INTO "+table+" (id, bin, txt, ntxt) VALUES (:1, :2, :3, :4)",
			id,
			oracle.BindBlob(blobData),
			oracle.BindClob(clobText),
			oracle.BindNClob(nclobText),
		); err != nil {
			return err
		}
	}

	if err := readPersistentLOBs(ctx, db, table, 1); err != nil {
		return err
	}
	if err := readMultipleRows(ctx, db, table); err != nil {
		return err
	}
	return writeTemporaryLOBs(ctx, db)
}
