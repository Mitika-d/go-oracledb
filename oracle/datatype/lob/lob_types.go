/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
 */

package lob

import internallob "github.com/oracle/go-oracledb/v26/internal/lob"

// LOB is the shared behavior of the typed Blob, Clob, and NClob values. Its
// zero value is invalid and can be used as a Scan destination. Do not copy a
// LOB value after first use.
type LOB struct {
	handle *lobHandle
	kind   internallob.Kind
}

// Blob is a typed Oracle BLOB value. Its zero value can be used with
// database/sql Rows.Scan.
type Blob struct{ LOB }

// Clob is a typed Oracle CLOB value. Text is read and written as UTF-8.
type Clob struct{ LOB }

// NClob is a typed Oracle NCLOB value. Text is read and written as UTF-8.
type NClob struct{ LOB }

// Kind identifies a public Oracle LOB family.
type Kind uint8

const (
	// KindUnknown is the zero value for an untyped LOB.
	KindUnknown Kind = iota
	// KindBLOB identifies binary LOB data.
	KindBLOB
	// KindCLOB identifies database-character-set LOB data.
	KindCLOB
	// KindNCLOB identifies national-character-set LOB data.
	KindNCLOB
)
