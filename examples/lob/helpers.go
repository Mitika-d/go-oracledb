/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
**
** Subject to the condition set forth below, permission is hereby granted to any
** person obtaining a copy of this software, associated documentation and/or data
** (collectively, the "Software"), free of charge and under any and all
** copyright and patent rights owned or licensable by each licensor hereunder
** covering the Software, to deal in the Software without restriction, including
** without limitation the rights to use, copy, modify, merge, publish,
** distribute, sublicense, and/or sell copies of the Software, and to permit
** persons to whom the Software is furnished to do so, subject to the following
** conditions:
**
** The above copyright notice and this permission notice shall be included in all
** copies or substantial portions of the Software.
**
** THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
** IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
** FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
** AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
** LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
** OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
** SOFTWARE.
 */

package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/oracle/go-oracledb/v26/oracle"
	lob "github.com/oracle/go-oracledb/v26/oracle/datatype/lob"
)

type exampleLOB interface {
	Kind() lob.Kind
	Valid() bool
	IsTemporary() bool
	ReadContext(context.Context, []byte) (int, error)
	Write([]byte) (int, error)
	WriteToContext(context.Context, io.Writer) (int64, error)
	SizeContext(context.Context) (int64, error)
	ChunkSizeContext(context.Context) (int64, error)
	OpenContext(context.Context) error
	CloseServerContext(context.Context) error
	IsOpenContext(context.Context) (bool, error)
	TrimContext(context.Context, int64) error
	Close() error
	FreeContext(context.Context) error
}

const (
	boundaryPrefetchCount32K       = 32 * 1024
	boundaryPrefetchCount40M       = 40 * 1024 * 1024
	multiplePrefetchDefaultCount8M = 8 * 1024 * 1024
	boundaryPayloadExtraCount64K   = 64 * 1024
	streamingPrefetchCount64K      = 64 * 1024
	basicPayloadSize               = 1024
	lobExampleTimeout              = 30 * time.Minute
)

func defaultLOBPrefetchCount() int {
	return oracle.NewOracleDriverConfig().DriverProperties.DefaultLobPrefetchSize
}

func parseMultiplePrefetchCount(value string) (int, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return multiplePrefetchDefaultCount8M, nil
	}
	prefetch, err := strconv.ParseInt(value, 10, 32)
	if err != nil || prefetch <= 0 {
		return 0, fmt.Errorf("invalid multiple prefetch count %q; use a positive signed 32-bit number", value)
	}
	return int(prefetch), nil
}

func formatPrefetchCount(count int) string {
	return fmt.Sprintf("%d (bytes for BLOB; characters for CLOB/NCLOB)", count)
}

func openLOBDatabase(ctx context.Context, dsn string, prefetchSize int) (*sql.DB, error) {
	cfg := oracle.NewOracleDriverConfig()
	cfg.ConnectDescriptor = dsn
	cfg.DriverProperties.DefaultLobPrefetchSize = prefetchSize

	connector, err := oracle.NewOracleConnector(cfg)
	if err != nil {
		return nil, fmt.Errorf("create Oracle connector: %w", err)
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("connect to Oracle: %w", err)
	}
	return db, nil
}

// binaryPayload returns deterministic binary data of exactly size bytes.
func binaryPayload(size int) []byte {
	payload := make([]byte, size)
	for index := range payload {
		payload[index] = byte(index*31 + 7)
	}
	return payload
}

// textPayload creates at least minCharacters ASCII characters followed by
// multi-byte and supplementary characters for CLOB and NCLOB checks.
func textPayload(minCharacters int) string {
	const unicodeSuffix = "Go Oracle LOB example: Aé中🙂\n"
	return strings.Repeat("A", minCharacters) + unicodeSuffix
}

// createTable creates a uniquely named table for a database-pool example.
func createTable(ctx context.Context, db *sql.DB, columns string) (string, error) {
	// The name is generated locally and never contains user input, so it can be
	// embedded in the DDL and DML used by this example.
	table := fmt.Sprintf("GOLOB_%d", time.Now().UnixNano())
	if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (%s)", table, columns)); err != nil {
		return "", fmt.Errorf("create table %s: %w", table, err)
	}
	return table, nil
}

// dropTable removes a table created by a database-pool example.
func dropTable(ctx context.Context, db *sql.DB, table string) error {
	if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP TABLE %s PURGE", table)); err != nil {
		return fmt.Errorf("drop table %s: %w", table, err)
	}
	return nil
}

// createTableConn creates a uniquely named table on a dedicated connection.
func createTableConn(ctx context.Context, conn *sql.Conn, columns string) (string, error) {
	table := fmt.Sprintf("GOLOB_%d", time.Now().UnixNano())
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (%s)", table, columns)); err != nil {
		return "", fmt.Errorf("create table %s: %w", table, err)
	}
	return table, nil
}

// dropTableConn removes a table created on a dedicated connection.
func dropTableConn(ctx context.Context, conn *sql.Conn, table string) error {
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE %s PURGE", table)); err != nil {
		return fmt.Errorf("drop table %s: %w", table, err)
	}
	return nil
}

// cleanupTable drops a database-pool example table with a cleanup timeout.
func cleanupTable(db *sql.DB, table string) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := dropTable(cleanupCtx, db, table); err != nil {
		fmt.Printf("cleanup warning: %v\n", err)
	}
}

// cleanupTableConn drops a dedicated-connection scenario table with a cleanup
// timeout.
func cleanupTableConn(conn *sql.Conn, table string) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := dropTableConn(cleanupCtx, conn, table); err != nil {
		fmt.Printf("cleanup warning: %v\n", err)
	}
}
