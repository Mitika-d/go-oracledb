/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
**
** Subject to the condition set forth below, permission is hereby granted to any
** person obtaining a copy of this software, associated documentation and/or data
** (collectively the "Software"), free of charge and under any and all copyright
** rights in the Software, and any and all patent rights owned or freely
** licensable by each licensor hereunder covering either (i) the unmodified
** Software as contributed to or provided by such licensor, or (ii) the Larger
** Works (as defined below), to deal in both
**
** (a) the Software, and
** (b) any piece of software and/or hardware listed in the lrgrwrks.txt file if
** one is included with the Software (each a "Larger Work" to which the Software
** is contributed by such licensors),
**
** without restriction, including without limitation the rights to copy, create
** derivative works of, display, perform, and distribute the Software and make,
** use, sell, offer for sale, import, export, have made, and have sold the
** Software and the Larger Work(s), and to sublicense the foregoing rights on
** either these or other terms.
**
** This license is subject to the following condition:
** The above copyright notice and either this complete permission notice or at
** a minimum a reference to the UPL must be included in all copies or
** substantial portions of the Software.
**
** THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
** IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
** FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
** AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
** LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
** OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
** SOFTWARE.
 */

package lob

import (
	"errors"
	"fmt"
	"io"
	"testing"

	internallob "github.com/oracle/go-oracledb/v26/internal/lob"
)

type scanTestSource struct {
	kind       Kind
	data       []byte
	offset     int
	closed     bool
	readErr    error
	closeErr   error
	noProgress bool
}

func (source *scanTestSource) Read(dst []byte) (int, error) {
	if source.noProgress {
		return 0, nil
	}
	if source.readErr != nil {
		return 0, source.readErr
	}
	if source.offset == len(source.data) {
		return 0, io.EOF
	}
	n := copy(dst, source.data[source.offset:])
	source.offset += n
	return n, nil
}

func (source *scanTestSource) WriteTo(writer io.Writer) (int64, error) {
	n, err := writer.Write(source.data[source.offset:])
	source.offset += n
	return int64(n), err
}

func (source *scanTestSource) Close() error              { source.closed = true; return source.closeErr }
func (source *scanTestSource) Size() (int64, error)      { return int64(len(source.data)), nil }
func (source *scanTestSource) ChunkSize() (int64, error) { return 1, nil }
func (source *scanTestSource) Kind() internallob.Kind    { return source.kind }

// TestLOBScan_BytesReadsAndCloses verifies Bytes materializes BLOB data and
// closes its source.
func TestLOBScan_BytesReadsAndCloses(t *testing.T) {
	source := &scanTestSource{kind: BLOB, data: []byte{0, 1, 2, 255}}
	var value Bytes
	if err := value.Scan(source); err != nil {
		t.Fatalf("Bytes.Scan: %v", err)
	}
	if string(value) != string(source.data) {
		t.Fatalf("Bytes = %x, want %x", value, source.data)
	}
	if !source.closed {
		t.Fatal("Bytes.Scan did not close source")
	}
}

// TestLOBScan_TextReadsCharacterKindsAndCloses verifies Text materializes
// CLOB and NCLOB data and closes their sources.
func TestLOBScan_TextReadsCharacterKindsAndCloses(t *testing.T) {
	for _, kind := range []Kind{CLOB, NCLOB} {
		t.Run(fmt.Sprintf("kind-%d", kind), func(t *testing.T) {
			source := &scanTestSource{kind: kind, data: []byte("Aé中🙂")}
			var value Text
			if err := value.Scan(source); err != nil {
				t.Fatalf("Text.Scan: %v", err)
			}
			if string(value) != "Aé中🙂" {
				t.Fatalf("Text = %q", value)
			}
			if !source.closed {
				t.Fatal("Text.Scan did not close source")
			}
		})
	}
}

// TestLOBScan_RejectsNullAndWrongKind verifies scan destinations reject NULL
// and incompatible LOB kinds.
func TestLOBScan_RejectsNullAndWrongKind(t *testing.T) {
	var binary Bytes
	if err := binary.Scan(nil); err == nil {
		t.Fatal("Bytes.Scan(nil) succeeded")
	}
	characterSource := &scanTestSource{kind: CLOB, data: []byte("text")}
	if err := binary.Scan(characterSource); err == nil {
		t.Fatal("Bytes.Scan(CLOB) succeeded")
	}
	if !characterSource.closed {
		t.Fatal("wrong-kind source was not closed")
	}

	var text Text
	if err := text.Scan(&scanTestSource{kind: BLOB, data: []byte("data")}); err == nil {
		t.Fatal("Text.Scan(BLOB) succeeded")
	}
}

// TestLOBScan_ReadAllErrors verifies _readAll reports source read, close, and
// no-progress failures while always closing the source.
func TestLOBScan_ReadAllErrors(t *testing.T) {
	t.Parallel()

	readErr := errors.New("read failed")
	closeErr := errors.New("close failed")
	tests := []struct {
		name    string
		source  *scanTestSource
		wantErr []error
	}{
		{
			name:    "read failure",
			source:  &scanTestSource{kind: BLOB, readErr: readErr},
			wantErr: []error{readErr},
		},
		{
			name:    "close failure after EOF",
			source:  &scanTestSource{kind: BLOB, data: []byte("data"), closeErr: closeErr},
			wantErr: []error{closeErr},
		},
		{
			name:    "no progress",
			source:  &scanTestSource{kind: BLOB, noProgress: true},
			wantErr: []error{io.ErrNoProgress},
		},
		{
			name:    "read and close failure",
			source:  &scanTestSource{kind: BLOB, readErr: readErr, closeErr: closeErr},
			wantErr: []error{readErr, closeErr},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := _readAll(test.source)
			for _, wantErr := range test.wantErr {
				if !errors.Is(err, wantErr) {
					t.Fatalf("error = %v, want %v", err, wantErr)
				}
			}
			if !test.source.closed {
				t.Fatal("_readAll did not close source")
			}
		})
	}
}
