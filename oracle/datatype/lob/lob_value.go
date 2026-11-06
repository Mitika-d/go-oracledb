/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
 */

package lob

import (
	"context"
	"database/sql"
	"errors"
	"io"

	"github.com/oracle/go-oracledb/v26/internal/common"
	internallob "github.com/oracle/go-oracledb/v26/internal/lob"
	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
)

// ReadAll reads the remaining BLOB bytes.
func (blob *Blob) ReadAll() ([]byte, error) { return blob.ReadAllContext(context.Background()) }

// ReadAllContext reads the remaining BLOB bytes.
func (blob *Blob) ReadAllContext(ctx context.Context) ([]byte, error) {
	return readAll(ctx, &blob.LOB)
}

// ReadAll reads the remaining CLOB text as UTF-8.
func (clob *Clob) ReadAll() (string, error) { return clob.ReadAllContext(context.Background()) }

// ReadAllContext reads the remaining CLOB text as UTF-8.
func (clob *Clob) ReadAllContext(ctx context.Context) (string, error) {
	data, err := readAll(ctx, &clob.LOB)
	return string(data), err
}

// ReadAll reads the remaining NCLOB text as UTF-8.
func (clob *NClob) ReadAll() (string, error) { return clob.ReadAllContext(context.Background()) }

// ReadAllContext reads the remaining NCLOB text as UTF-8.
func (clob *NClob) ReadAllContext(ctx context.Context) (string, error) {
	data, err := readAll(ctx, &clob.LOB)
	return string(data), err
}

func readAll(ctx context.Context, lob *LOB) ([]byte, error) {
	buf := make([]byte, int(defaultChunkBytes))
	var result []byte
	for {
		n, err := lob.ReadContext(ctx, buf)
		if n > 0 {
			result = append(result, buf[:n]...)
		}
		if errors.Is(err, io.EOF) {
			return result, nil
		}
		if err != nil {
			return result, err
		}
	}
}

// WriteString writes UTF-8 text to a CLOB using a background context.
func (clob *Clob) WriteString(text string) (int, error) {
	return clob.WriteStringContext(context.Background(), text)
}

// WriteStringContext writes UTF-8 text to a CLOB.
func (clob *Clob) WriteStringContext(ctx context.Context, text string) (int, error) {
	return clob.WriteContext(ctx, []byte(text))
}

// WriteString writes UTF-8 text to an NCLOB using a background context.
func (clob *NClob) WriteString(text string) (int, error) {
	return clob.WriteStringContext(context.Background(), text)
}

// WriteStringContext writes UTF-8 text to an NCLOB.
func (clob *NClob) WriteStringContext(ctx context.Context, text string) (int, error) {
	return clob.WriteContext(ctx, []byte(text))
}

// CreateTemporaryBlob creates a session-duration temporary BLOB on connection.
// The caller must keep the dedicated *sql.Conn open and Free the value before
// closing that connection.
func CreateTemporaryBlob(ctx context.Context, connection *sql.Conn) (*Blob, error) {
	session, err := connectionLOBSession(connection)
	if err != nil {
		return nil, err
	}
	locator, err := createTemporaryLocator(ctx, session, internallob.BLOB)
	if err != nil {
		return nil, err
	}
	return &Blob{LOB{kind: internallob.BLOB, handle: newHandle(session, internallob.BLOB, locator, true)}}, nil
}

// CreateTemporaryClob creates a session-duration temporary CLOB on connection.
// The caller must keep the dedicated *sql.Conn open and Free the value before
// closing that connection.
func CreateTemporaryClob(ctx context.Context, connection *sql.Conn) (*Clob, error) {
	session, err := connectionLOBSession(connection)
	if err != nil {
		return nil, err
	}
	locator, err := createTemporaryLocator(ctx, session, internallob.CLOB)
	if err != nil {
		return nil, err
	}
	return &Clob{LOB{kind: internallob.CLOB, handle: newHandle(session, internallob.CLOB, locator, true)}}, nil
}

// CreateTemporaryNClob creates a session-duration temporary NCLOB on connection.
// The caller must keep the dedicated *sql.Conn open and Free the value before
// closing that connection.
func CreateTemporaryNClob(ctx context.Context, connection *sql.Conn) (*NClob, error) {
	session, err := connectionLOBSession(connection)
	if err != nil {
		return nil, err
	}
	locator, err := createTemporaryLocator(ctx, session, internallob.NCLOB)
	if err != nil {
		return nil, err
	}
	return &NClob{LOB{kind: internallob.NCLOB, handle: newHandle(session, internallob.NCLOB, locator, true)}}, nil
}

func connectionLOBSession(connection *sql.Conn) (internallob.LOBSession, error) {
	if connection == nil {
		return nil, common.NewOracleError(oracleErrors.InvalidConnection, nil)
	}
	var session internallob.LOBSession
	err := connection.Raw(func(raw any) error {
		provider, ok := raw.(internallob.LOBHandleProvider)
		if !ok {
			return common.NewOracleError(oracleErrors.InvalidConnection, nil, "Oracle LOB connection required")
		}
		var err error
		session, err = provider.NewLOBSession()
		return err
	})
	if err == nil && session == nil {
		return nil, common.NewOracleError(oracleErrors.InvalidConnection, nil, "LOB session capability unavailable")
	}
	return session, err
}

func createTemporaryLocator(ctx context.Context, session internallob.LOBSession, kind internallob.Kind) ([]byte, error) {
	release, err := session.AcquireLOBLease()
	if err != nil {
		return nil, err
	}
	defer release()
	locator, err := session.LobCreate(ctx, kind)
	if err != nil {
		return nil, err
	}
	if len(locator) == 0 {
		return nil, common.NewOracleError(oracleErrors.InvalidLOBBuffer, nil, "empty temporary LOB locator")
	}
	return locator, nil
}
