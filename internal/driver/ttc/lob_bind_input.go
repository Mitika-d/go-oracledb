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

package ttc

import (
	"bytes"
	"database/sql/driver"
	"io"
	"strings"

	"github.com/oracle/go-oracledb/v26/internal/common"
	internallob "github.com/oracle/go-oracledb/v26/internal/lob"
	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
)

// lobBindInput carries the normalized source, LOB kind, and declared byte size
// for one streamed bind. Its size is measured in source bytes; the bind pipeline
// validates it before reading and derives Oracle character units for CLOBs.
type lobBindInput struct {
	// source provides the bytes that the statement pipeline streams to Oracle.
	source io.Reader
	// kind selects the BLOB, CLOB, or NCLOB creation and write path.
	kind internallob.Kind
	// byteSize is the exact number of source bytes the pipeline may consume.
	byteSize int64
}

// newLobBindInput creates a streamed bind input without reading or validating
// its source. The statement pipeline validates it before consumption.
//
// Parameters:
//   - source: reader that supplies the bind bytes.
//   - kind: BLOB, CLOB, or NCLOB bind kind.
//   - size: exact number of source bytes to consume.
//
// Returns:
//   - lobBindInput: unvalidated streamed bind input.
func newLobBindInput(source io.Reader, kind internallob.Kind, size int64) lobBindInput {
	return lobBindInput{source: source, kind: kind, byteSize: size}
}

// validate checks that the source is present, the LOB kind is supported, and
// the declared byte size is non-negative.
//
// Returns:
//   - error: InvalidLobInput when the source metadata is invalid; otherwise nil.
func (input lobBindInput) validate() error {
	switch {
	case input.source == nil:
		return common.NewOracleError(oracleErrors.InvalidLobInput, nil, "reader")
	case !internallob.ValidKind(input.kind):
		return common.NewOracleError(oracleErrors.InvalidLobInput, nil, "LOB kind")
	case input.byteSize < 0:
		return common.NewOracleError(oracleErrors.InvalidLobInput, nil, "size marker")
	default:
		return nil
	}
}

// normalizeLobBindArgs replaces typed LOB markers with private streamed
// inputs. It returns the original argument slice when no marker needs conversion.
//
// BLOB markers retain their byte-slice backing storage and declared byte length.
// CLOB and NCLOB markers use their UTF-8 byte lengths; the stream pipeline checks
// the text and derives Oracle character units before sending it.
//
// Parameters:
//   - args: statement arguments that may contain typed LOB markers.
//
// Returns:
//   - []driver.NamedValue: original arguments when no marker is present, or a
//     copy with markers converted to streamed inputs.
func normalizeLobBindArgs(args []driver.NamedValue) []driver.NamedValue {
	for index := range args {
		input, ok := lobBindInputFromValue(args[index].Value)
		if !ok {
			continue
		}
		normalized := append([]driver.NamedValue(nil), args...)
		normalized[index].Value = input
		for valueIndex := index + 1; valueIndex < len(normalized); valueIndex++ {
			input, ok := lobBindInputFromValue(normalized[valueIndex].Value)
			if ok {
				normalized[valueIndex].Value = input
			}
		}
		return normalized
	}
	return args
}

// lobBindInputFromValue converts one typed LOB marker into a private streamed
// input with an exact declared size. It reports false for ordinary bind values.
//
// Parameters:
//   - value: candidate bind value.
//
// Returns:
//   - lobBindInput: converted input, or the zero value when value is not a marker.
//   - bool: true when value was a supported LOB marker.
func lobBindInputFromValue(value any) (lobBindInput, bool) {
	switch value := value.(type) {
	case internallob.BindBlob:
		return newLobBindInput(bytes.NewReader(value), internallob.BLOB, int64(len(value))), true
	case internallob.BindClob:
		text := string(value)
		return newLobBindInput(strings.NewReader(text), internallob.CLOB, int64(len(text))), true
	case internallob.BindNClob:
		text := string(value)
		return newLobBindInput(strings.NewReader(text), internallob.NCLOB, int64(len(text))), true
	default:
		return lobBindInput{}, false
	}
}

// validateLobBindArgs validates streamed inputs without consuming a source. It
// returns the first LOB bind argument index, or -1 when none is present.
//
// Parameters:
//   - args: normalized statement arguments to validate.
//
// Returns:
//   - int: index of the first streamed input, or -1 when there is none.
//   - error: validation error for the first invalid input; otherwise nil.
func validateLobBindArgs(args []driver.NamedValue) (int, error) {
	firstLobBindIndex := -1
	for index := range args {
		input, ok := args[index].Value.(lobBindInput)
		if !ok {
			continue
		}
		if firstLobBindIndex < 0 {
			firstLobBindIndex = index
		}
		// Validate every carrier before preparation can consume any reader. This
		// keeps an invalid later bind from leaving an earlier reader partially used.
		if err := input.validate(); err != nil {
			return -1, err
		}
	}
	return firstLobBindIndex, nil
}

// normalizeAndValidateLobBindArgs converts typed markers and validates
// all inputs before streamed bind preparation consumes any source.
//
// Parameters:
//   - args: statement arguments that may contain typed LOB markers.
//
// Returns:
//   - []driver.NamedValue: normalized arguments.
//   - int: index of the first streamed input, or -1 when there is none.
//   - error: validation error for the first invalid input; otherwise nil.
func normalizeAndValidateLobBindArgs(args []driver.NamedValue) ([]driver.NamedValue, int, error) {
	normalized := normalizeLobBindArgs(args)
	firstLobBindIndex, err := validateLobBindArgs(normalized)
	return normalized, firstLobBindIndex, err
}

// sizedLobReader bounds source reads to the declared byte size and exposes the
// remaining count so the stream pipeline can detect a short source.
//
// Parameters:
//   - input: validated streamed bind input.
//
// Returns:
//   - *io.LimitedReader: reader limited to the declared source byte size.
func sizedLobReader(input lobBindInput) *io.LimitedReader {
	return &io.LimitedReader{R: input.source, N: input.byteSize}
}
