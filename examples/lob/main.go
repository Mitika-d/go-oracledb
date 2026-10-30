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

// Package main demonstrates public materialized and streaming LOB APIs.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
)

func main() {
	caseName := flag.String("case", "basic", "example to run: basic, streaming, temporary, persistent, multiple, multiple-low, boundary-low, boundary-high, or all")
	multiplePrefetch := flag.String("multiple-prefetch", os.Getenv("ORACLE_LOB_MULTIPLE_PREFETCH_SIZE"), "prefetch count for the multiple-column example")
	flag.Parse()

	dsn := os.Getenv("ORACLE_DSN")
	if dsn == "" {
		log.Fatal("set ORACLE_DSN, for example: user/password@host:port/service")
	}
	prefetchCount, err := parseMultiplePrefetchCount(*multiplePrefetch)
	if err != nil {
		log.Fatal(err)
	}
	if err := runLOBExample(dsn, *caseName, prefetchCount); err != nil {
		log.Fatal(err)
	}
}

func runLOBExample(dsn, caseName string, multiplePrefetch int) error {
	switch strings.ToLower(strings.TrimSpace(caseName)) {
	case "basic":
		return runConfiguredLOBExample(dsn, "basic BLOB/CLOB bind and read", defaultLOBPrefetchCount(), runBasicBindReadExample)
	case "streaming":
		return runConfiguredLOBExample(dsn, "streaming BLOB/CLOB read", streamingPrefetchCount64K, runStreamingReadExample)
	case "temporary":
		return runConfiguredLOBExample(dsn, "temporary typed LOBs", defaultLOBPrefetchCount(), runTemporaryLOBExample)
	case "persistent":
		return runConfiguredLOBExample(dsn, "persistent typed LOBs", defaultLOBPrefetchCount(), runPersistentLOBExample)
	case "multiple":
		return runConfiguredLOBExample(dsn, "multiple LOB columns", multiplePrefetch, func(ctx context.Context, db *sql.DB) error {
			return runMultipleColumnsExample(ctx, db, multiplePrefetch)
		})
	case "multiple-low":
		return runConfiguredLOBExample(dsn, "multiple LOB columns with low prefetch", boundaryPrefetchCount32K, func(ctx context.Context, db *sql.DB) error {
			return runMultipleColumnsExample(ctx, db, boundaryPrefetchCount32K)
		})
	case "boundary-low":
		return runConfiguredLOBExample(dsn, "prefetch boundary below default", boundaryPrefetchCount32K, func(ctx context.Context, db *sql.DB) error {
			return runPrefetchBoundaryExample(ctx, db, boundaryPrefetchCount32K)
		})
	case "boundary-high":
		return runConfiguredLOBExample(dsn, "prefetch boundary above default", boundaryPrefetchCount40M, func(ctx context.Context, db *sql.DB) error {
			return runPrefetchBoundaryExample(ctx, db, boundaryPrefetchCount40M)
		})
	case "all":
		for _, name := range []string{"basic", "streaming", "temporary", "persistent", "multiple-low", "multiple", "boundary-low", "boundary-high"} {
			if err := runLOBExample(dsn, name, multiplePrefetch); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unknown -case %q; choose basic, streaming, temporary, persistent, multiple, multiple-low, boundary-low, boundary-high, or all", caseName)
	}
}

func runConfiguredLOBExample(dsn, name string, prefetchSize int, run func(context.Context, *sql.DB) error) (retErr error) {
	fmt.Printf("\n--- %s (LOB prefetch count: %s) ---\n", name, formatPrefetchCount(prefetchSize))
	ctx, cancel := context.WithTimeout(context.Background(), lobExampleTimeout)
	defer cancel()

	db, err := openLOBDatabase(ctx, dsn, prefetchSize)
	if err != nil {
		return err
	}
	defer func() {
		if err := db.Close(); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("close database after %s: %w", name, err))
		}
	}()
	if err := run(ctx, db); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	fmt.Printf("%s: ok\n", name)
	return nil
}
