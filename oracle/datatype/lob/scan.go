/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
 */

package lob

import (
	"bytes"
	"database/sql"
	"errors"
	"io"
	"math"
	"reflect"

	"github.com/oracle/go-oracledb/v26/internal/common"
	internallob "github.com/oracle/go-oracledb/v26/internal/lob"
	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
)

// Scan implements sql.Scanner for Blob. SQL NULL resets the value to invalid.
// Non-NULL values must be persistent BLOB locators produced by this driver.
func (blob *Blob) Scan(src any) error {
	if blob == nil {
		return invalidSource("nil *Blob destination")
	}
	return blob.LOB.scan(src, internallob.BLOB)
}

// Scan implements sql.Scanner for Clob. SQL NULL resets the value to invalid.
// Non-NULL values must be persistent CLOB locators produced by this driver.
func (clob *Clob) Scan(src any) error {
	if clob == nil {
		return invalidSource("nil *Clob destination")
	}
	return clob.LOB.scan(src, internallob.CLOB)
}

// Scan implements sql.Scanner for NClob. SQL NULL resets the value to invalid.
// Non-NULL values must be persistent NCLOB locators produced by this driver.
func (clob *NClob) Scan(src any) error {
	if clob == nil {
		return invalidSource("nil *NClob destination")
	}
	return clob.LOB.scan(src, internallob.NCLOB)
}

func (lob *LOB) scan(src any, kind internallob.Kind) error {
	if lob == nil {
		return invalidSource("nil *LOB destination")
	}
	if src == nil {
		lob.reset(kind)
		return nil
	}
	adopter, ok := src.(internallob.PersistentLocatorAdopter)
	if !ok {
		return invalidSource("persistent locator source required")
	}
	adoption, err := adopter.AdoptPersistentLocator(kind)
	if err != nil {
		return err
	}
	if adoption.Session == nil || len(adoption.Locator) == 0 {
		return invalidSource("incomplete locator adoption")
	}
	lob.handle = &lobHandle{
		session: adoption.Session,
		kind:    kind,
		locator: append([]byte(nil), adoption.Locator...),
		offset:  adoption.Offset,
		length:  adoption.Length,
		pending: append([]byte(nil), adoption.Prefix...),
	}
	if lob.handle.offset == 0 {
		lob.handle.offset = 1
	}
	lob.kind = kind
	return nil
}

// reset clears the local value and leaves it ready for another scan.
func (lob *LOB) reset(kind internallob.Kind) {
	lob.handle = nil
	lob.kind = kind
}

// Kind reports the statically declared family of this LOB value.
func (lob *LOB) Kind() Kind {
	if lob == nil {
		return KindUnknown
	}
	switch lob.kind {
	case internallob.BLOB:
		return KindBLOB
	case internallob.CLOB:
		return KindCLOB
	case internallob.NCLOB:
		return KindNCLOB
	default:
		return KindUnknown
	}
}

// Kind reports BLOB for every Blob value, including its zero value.
func (blob *Blob) Kind() Kind { return KindBLOB }

// Kind reports CLOB for every Clob value, including its zero value.
func (clob *Clob) Kind() Kind { return KindCLOB }

// Kind reports NCLOB for every NClob value, including its zero value.
func (clob *NClob) Kind() Kind { return KindNCLOB }

// Valid reports whether the value still has a live connection-scoped locator.
func (lob *LOB) Valid() bool {
	if lob == nil || lob.handle == nil {
		return false
	}
	h := lob.handle
	h.mu.Lock()
	defer h.mu.Unlock()
	return !h.closed && h.session != nil && h.session.ValidateLOBLease() == nil
}

// IsTemporary reports whether this handle refers to a session-duration
// temporary LOB.
func (lob *LOB) IsTemporary() bool {
	if lob == nil || lob.handle == nil {
		return false
	}
	lob.handle.mu.Lock()
	defer lob.handle.mu.Unlock()
	return !lob.handle.closed && lob.handle.temporary
}

func invalidSource(detail string) error {
	return common.NewOracleError(oracleErrors.InvalidLobSource, nil, detail)
}

var (
	_ sql.Scanner = (*Blob)(nil)
	_ sql.Scanner = (*Clob)(nil)
	_ sql.Scanner = (*NClob)(nil)
	_ sql.Scanner = (*Bytes)(nil)
	_ sql.Scanner = (*Text)(nil)
)

// Bytes is a BLOB scan destination. Its underlying type is []byte. Scan fully
// reads the BLOB into memory and releases the query LOB. SQL NULL is rejected.
type Bytes []byte

// Text is a CLOB or NCLOB scan destination. Its underlying type is string and
// its contents are UTF-8. Scan fully reads the LOB into memory and releases the
// query LOB. SQL NULL is rejected.
type Text string

// Scan implements sql.Scanner for a BLOB query column returned by go-oracledb.
// It reads the locator-backed source completely and closes it before returning.
func (value *Bytes) Scan(input any) error {
	if value == nil {
		return invalidSource("nil *Bytes destination")
	}
	source, err := _scanSource(input, internallob.BLOB)
	if err != nil {
		return err
	}
	data, err := _readAll(source)
	if err != nil {
		return err
	}
	*value = Bytes(data)
	return nil
}

// Scan implements sql.Scanner for a CLOB or NCLOB query column returned by
// go-oracledb. It reads the locator-backed source completely and closes it
// before returning.
func (value *Text) Scan(input any) error {
	if value == nil {
		return invalidSource("nil *Text destination")
	}
	source, err := _scanSource(input, internallob.CLOB, internallob.NCLOB)
	if err != nil {
		return err
	}
	data, err := _readAll(source)
	if err != nil {
		return err
	}
	*value = Text(string(data))
	return nil
}

// _scanSource validates input as a non-nil locator source of one of allowed
// kinds. It closes an incompatible source before returning an error.
func _scanSource(input any, allowed ...internallob.Kind) (internallob.LOBSource, error) {
	if input == nil {
		return nil, common.NewOracleError(oracleErrors.InvalidLobSource, nil, "NULL LOB value")
	}
	source, ok := input.(internallob.LOBSource)
	if !ok || _isNilSource(source) {
		return nil, common.NewOracleError(oracleErrors.InvalidLobSource, nil, "source type")
	}
	for _, kind := range allowed {
		if source.Kind() == kind {
			return source, nil
		}
	}
	kindErr := common.NewOracleError(oracleErrors.InvalidLobSource, nil, "LOB kind")
	return nil, errors.Join(kindErr, source.Close())
}

func _isNilSource(source internallob.LOBSource) bool {
	if source == nil {
		return true
	}
	value := reflect.ValueOf(source)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// _readAll consumes source with bounded reads, enforces the in-memory size
// limit, and closes source before returning its bytes or an error.
func _readAll(source internallob.LOBSource) (data []byte, err error) {
	defer func() {
		if closeErr := source.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()
	if source.Kind() == internallob.BLOB {
		if size, sizeErr := source.Size(); sizeErr != nil {
			return nil, sizeErr
		} else if size > math.MaxInt32 {
			return nil, common.NewOracleError(oracleErrors.InvalidLOBBuffer, nil, "scan", "BLOB", "value exceeds MaxInt32")
		}
	}
	var output bytes.Buffer
	buffer := make([]byte, 32*1024)
	for {
		n, err := source.Read(buffer)
		if n != 0 {
			if int64(output.Len())+int64(n) > math.MaxInt32 {
				return nil, common.NewOracleError(oracleErrors.InvalidLOBBuffer, nil, "scan", "LOB", "value exceeds MaxInt32")
			}
			_, _ = output.Write(buffer[:n])
		}
		if err == io.EOF {
			return output.Bytes(), nil
		}
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return nil, io.ErrNoProgress
		}
	}
}
