/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
**
** Subject to the condition set forth below, permission is hereby granted to any
** person obtaining a copy of this software, associated documentation and/or data
** (collectively the "Software"), free of charge and under any and all copyright
** rights in the Software, and under any and all patent rights owned or freely
** licensable by each licensor hereunder covering either (i) the unmodified
** Software as contributed to or provided by such licensor, or (ii) the Larger
** Works (as defined below) to deal in both
**
** (a) the Software, and
** (b) any piece of software and/or hardware listed in the lrgrwrks.txt file if
** one is included with the Software (each a "Larger Work" to which the Software
** is contributed by such licensors),
**
** without restriction, including without limitation the rights to copy, create
** derivative works of, display, perform, and distribute the Software and make,
** use, sell, offer for sale, import, export, have made, and have sold the Software
** and the Larger Work(s), and to sublicense the foregoing rights on either these
** or other terms.
**
** This license is subject to the following condition:
** The above copyright notice and either this complete permission notice or at
** a minimum a reference to the UPL must be included in all copies or substantial
** portions of the Software.
**
** THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
** IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
** FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
** AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
** LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
** OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
** SOFTWARE.
 */

package oracle

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/oracle/go-oracledb/v26/oracle/datatype/lob"
	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
)

// deferTableCleanup schedules a table drop when the functional test finishes.
// The timeout bounds cleanup if the database call stalls.
func deferTableCleanup(t *testing.T, db *sql.DB, table string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := dropTable(ctx, db, table); err != nil {
			t.Errorf("drop table %s: %v", table, err)
		}
	})
}

// assertLOBErrorCode asserts that err is the requested Oracle error.
func assertLOBErrorCode(t *testing.T, want oracleErrors.ErrorCode, err error) {
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

const lobTestReadBufferSize = 32 * 1024

// readTypedLOBContents reads a typed LOB through successive 32 KiB buffers.
func readTypedLOBContents(t *testing.T, ctx context.Context, read func(context.Context, []byte) (int, error)) []byte {
	t.Helper()
	var output bytes.Buffer
	buffer := make([]byte, lobTestReadBufferSize)
	for {
		n, err := read(ctx, buffer)
		if n > 0 {
			_, _ = output.Write(buffer[:n])
		}
		if err == io.EOF {
			return output.Bytes()
		}
		if err != nil {
			t.Fatalf("read typed LOB: %v", err)
		}
	}
}

// compareTypedLOBContents compares a streamed LOB with an expected reader
// without materializing the full result.
func compareTypedLOBContents(t *testing.T, ctx context.Context, read func(context.Context, []byte) (int, error), expected io.Reader) error {
	t.Helper()
	gotBuffer := make([]byte, lobTestReadBufferSize)
	wantBuffer := make([]byte, len(gotBuffer))
	var offset int
	for {
		count, readErr := read(ctx, gotBuffer)
		if count > len(wantBuffer) {
			return fmt.Errorf("LOB read returned %d bytes into a %d-byte buffer", count, len(wantBuffer))
		}
		wantCount, wantErr := io.ReadFull(expected, wantBuffer[:count])
		if wantCount != count || wantErr != nil || !bytes.Equal(gotBuffer[:count], wantBuffer[:wantCount]) {
			return fmt.Errorf("LOB content mismatch at byte %d", offset+wantCount)
		}
		if readErr == io.EOF {
			var extra [1]byte
			extraCount, extraErr := expected.Read(extra[:])
			if extraCount != 0 {
				return fmt.Errorf("LOB ended early at byte %d", offset+count)
			}
			if extraErr != io.EOF {
				if extraErr == nil {
					return fmt.Errorf("read expected LOB: %w", io.ErrNoProgress)
				}
				return fmt.Errorf("read expected LOB: %w", extraErr)
			}
			return nil
		}
		offset += count
		if readErr != nil {
			return fmt.Errorf("read LOB: %w", readErr)
		}
		if count == 0 {
			return fmt.Errorf("read LOB: %w", io.ErrNoProgress)
		}
	}
}

// openLOBFunctionalDBWithPrefetch opens a functional *sql.DB with the requested
// LOB prefetch size and limits its pool to one open connection.
func openLOBFunctionalDBWithPrefetch(t *testing.T, lobPrefetchSize int) *sql.DB {
	t.Helper()
	if TestingConfig == nil {
		t.Skip("No configuration available")
	}
	dsn := TestingConfig.GetConnectionString()
	separator := "?"
	if strings.Contains(dsn, "?") {
		separator = "&"
	}
	dsn += fmt.Sprintf("%soracle.go.DriverProperties.DefaultLobPrefetchSize=%d", separator, lobPrefetchSize)
	db, err := sql.Open(TestingConfig.Driver.Name, dsn)
	if err != nil {
		t.Fatalf("open test DB: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("ping test DB: %v", err)
	}
	return db
}

// functionalLOBBytes builds deterministic binary test data.
func functionalLOBBytes(size int) []byte {
	payload := make([]byte, size)
	for index := range payload {
		payload[index] = byte(index*31 + 7)
	}
	return payload
}

// functionalLOBText builds UTF-8 text with single- and multi-unit characters.
func functionalLOBText(minCharacters int) string {
	return strings.Repeat("A", minCharacters) + "Go Oracle LOB functional test: Aé中🙂\n"
}

// functionalLOBTextSize returns the Oracle UTF-16 logical length of value.
func functionalLOBTextSize(value string) int64 {
	var size int64
	for _, character := range value {
		if character > 0xffff {
			size += 2
		} else {
			size++
		}
	}
	return size
}

// writeTemporaryBlob creates a temporary BLOB and writes payload through its
// public WriteContext API. It registers fallback cleanup; conn must remain open
// until the value is freed.
func writeTemporaryBlob(t *testing.T, ctx context.Context, conn *sql.Conn, payload []byte) *lob.Blob {
	t.Helper()
	value, err := lob.CreateTemporaryBlob(ctx, conn)
	if err != nil {
		t.Fatalf("create temporary BLOB: %v", err)
	}
	t.Cleanup(func() {
		if err := value.FreeContext(ctx); err != nil {
			t.Errorf("free temporary BLOB: %v", err)
		}
	})
	if n, err := value.WriteContext(ctx, payload); err != nil || n != len(payload) {
		t.Fatalf("write temporary BLOB = (%d, %v), want (%d, nil)", n, err, len(payload))
	}
	return value
}

// writeTemporaryClob creates a temporary CLOB and writes UTF-8 text through
// its public WriteStringContext API. It registers fallback cleanup; conn must
// remain open until the value is freed.
func writeTemporaryClob(t *testing.T, ctx context.Context, conn *sql.Conn, payload string) *lob.Clob {
	t.Helper()
	value, err := lob.CreateTemporaryClob(ctx, conn)
	if err != nil {
		t.Fatalf("create temporary CLOB: %v", err)
	}
	t.Cleanup(func() {
		if err := value.FreeContext(ctx); err != nil {
			t.Errorf("free temporary CLOB: %v", err)
		}
	})
	if n, err := value.WriteStringContext(ctx, payload); err != nil || n != len(payload) {
		t.Fatalf("write temporary CLOB = (%d, %v), want (%d, nil)", n, err, len(payload))
	}
	return value
}

// writeTemporaryNClob creates a temporary NCLOB and writes UTF-8 text through
// its public WriteStringContext API. It registers fallback cleanup; conn must
// remain open until the value is freed.
func writeTemporaryNClob(t *testing.T, ctx context.Context, conn *sql.Conn, payload string) *lob.NClob {
	t.Helper()
	value, err := lob.CreateTemporaryNClob(ctx, conn)
	if err != nil {
		t.Fatalf("create temporary NCLOB: %v", err)
	}
	t.Cleanup(func() {
		if err := value.FreeContext(ctx); err != nil {
			t.Errorf("free temporary NCLOB: %v", err)
		}
	})
	if n, err := value.WriteStringContext(ctx, payload); err != nil || n != len(payload) {
		t.Fatalf("write temporary NCLOB = (%d, %v), want (%d, nil)", n, err, len(payload))
	}
	return value
}
