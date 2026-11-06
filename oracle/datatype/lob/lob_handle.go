/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
 */

// Package lob provides materializing scan destinations and typed,
// connection-scoped Oracle BLOB, CLOB, and NCLOB values. Scan BLOBs into Bytes
// and CLOBs or NCLOBs into Text to read complete values into memory. Scan into
// Blob, Clob, or NClob to retain a locator; operations after Rows.Close require
// the dedicated *sql.Conn that produced the value. CLOB and NCLOB data is
// exposed as UTF-8; their sizes and offsets use Oracle UTF-16 logical units.
package lob

import (
	"context"
	"errors"
	"io"
	"math"
	"sync"
	"unicode/utf8"

	"github.com/oracle/go-oracledb/v26/internal/common"
	internallob "github.com/oracle/go-oracledb/v26/internal/lob"
	oracleErrors "github.com/oracle/go-oracledb/v26/oracle/errors"
)

const (
	defaultChunkBytes = uint64(internallob.DefaultBlobLobChunkBytes)
	openReadWrite     = uint8(internallob.LOBOpenReadWrite)
)

// lobHandle owns a locator and its sequential logical cursor. A session
// capability is captured from the dedicated database/sql connection and
// expires when database/sql returns that connection to its pool.
type lobHandle struct {
	mu        sync.Mutex
	session   internallob.LOBSession
	kind      internallob.Kind
	locator   []byte
	offset    uint64 // next 1-based Oracle logical unit
	length    uint64
	pending   []byte
	temporary bool
	opened    bool
	closed    bool
}

// Read implements io.Reader using a background context.
func (lob *LOB) Read(dst []byte) (int, error) { return lob.ReadContext(context.Background(), dst) }

// ReadContext reads the next bytes or UTF-8 text from the LOB.
func (lob *LOB) ReadContext(ctx context.Context, dst []byte) (int, error) {
	h, err := lob.liveHandle()
	if err != nil {
		return 0, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	release, err := h.acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer release()
	if len(dst) == 0 {
		return 0, nil
	}
	if len(h.pending) != 0 {
		n := copy(dst, h.pending)
		h.pending = h.pending[n:]
		if len(h.pending) == 0 {
			h.pending = nil
		}
		return n, nil
	}
	if h.offset == 0 || h.offset-1 >= h.length {
		return 0, io.EOF
	}
	amount := defaultChunkBytes
	if h.kind != internallob.BLOB {
		amount = uint64(internallob.DefaultCharacterLobChunkUnits)
	}
	if remaining := h.length - (h.offset - 1); amount > remaining {
		amount = remaining
	}
	data, logical, err := h.session.LobRead(ctx, h.kind, h.locator, h.offset, amount)
	if err != nil {
		return 0, err
	}
	if len(data) == 0 || logical == 0 {
		return 0, io.ErrUnexpectedEOF
	}
	h.offset += logical
	n := copy(dst, data)
	if n < len(data) {
		h.pending = append(h.pending[:0], data[n:]...)
	}
	return n, nil
}

// ReadAt reads BLOB bytes at the zero-based byte offset without changing the
// sequential cursor. It implements io.ReaderAt short-read behavior.
func (blob *Blob) ReadAt(dst []byte, offset int64) (int, error) {
	return blob.ReadAtContext(context.Background(), dst, offset)
}

// ReadAtContext reads BLOB bytes at the zero-based byte offset without
// changing the sequential cursor.
func (blob *Blob) ReadAtContext(ctx context.Context, dst []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, common.NewOracleError(oracleErrors.InvalidLOBBuffer, nil, "negative BLOB read offset")
	}
	h, err := blob.liveHandle()
	if err != nil {
		return 0, err
	}
	if h.kind != internallob.BLOB {
		return 0, common.NewOracleError(oracleErrors.UnsupportedLobOperation, nil, "ReadAt requires BLOB")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	release, err := h.acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer release()
	if len(dst) == 0 {
		return 0, nil
	}
	data, _, err := h.session.LobRead(ctx, h.kind, h.locator, uint64(offset)+1, uint64(len(dst)))
	if err != nil {
		return 0, err
	}
	n := copy(dst, data)
	if n < len(dst) {
		return n, io.EOF
	}
	return n, nil
}

// ReadTextAt reads up to amount UTF-16 logical units from a CLOB at the
// zero-based offset and returns UTF-8 bytes, units consumed, and any error.
func (clob *Clob) ReadTextAt(offset int64, amount uint64) ([]byte, uint64, error) {
	return clob.ReadTextAtContext(context.Background(), offset, amount)
}

// ReadTextAtContext reads up to amount UTF-16 logical units from a CLOB at the
// zero-based offset without changing the sequential cursor.
func (clob *Clob) ReadTextAtContext(ctx context.Context, offset int64, amount uint64) ([]byte, uint64, error) {
	return readTextAt(ctx, &clob.LOB, internallob.CLOB, offset, amount)
}

// ReadTextAt reads up to amount UTF-16 logical units from an NCLOB at the
// zero-based offset and returns UTF-8 bytes, units consumed, and any error.
func (clob *NClob) ReadTextAt(offset int64, amount uint64) ([]byte, uint64, error) {
	return clob.ReadTextAtContext(context.Background(), offset, amount)
}

// ReadTextAtContext reads up to amount UTF-16 logical units from an NCLOB at
// the zero-based offset without changing the sequential cursor.
func (clob *NClob) ReadTextAtContext(ctx context.Context, offset int64, amount uint64) ([]byte, uint64, error) {
	return readTextAt(ctx, &clob.LOB, internallob.NCLOB, offset, amount)
}

func readTextAt(ctx context.Context, lob *LOB, kind internallob.Kind, offset int64, amount uint64) ([]byte, uint64, error) {
	if offset < 0 {
		return nil, 0, common.NewOracleError(oracleErrors.InvalidLOBBuffer, nil, "negative text LOB read offset")
	}
	h, err := lob.liveHandle()
	if err != nil {
		return nil, 0, err
	}
	if h.kind != kind {
		return nil, 0, common.NewOracleError(oracleErrors.UnsupportedLobOperation, nil, "text read kind mismatch")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	release, err := h.acquire(ctx)
	if err != nil {
		return nil, 0, err
	}
	defer release()
	if amount == 0 {
		return nil, 0, nil
	}
	return h.session.LobRead(ctx, kind, h.locator, uint64(offset)+1, amount)
}

// Write writes bytes or UTF-8 text at the current logical cursor.
func (lob *LOB) Write(data []byte) (int, error) {
	return lob.WriteContext(context.Background(), data)
}

// WriteContext writes bytes or UTF-8 text at the current logical cursor.
func (lob *LOB) WriteContext(ctx context.Context, data []byte) (int, error) {
	h, err := lob.liveHandle()
	if err != nil {
		return 0, err
	}
	if h.kind != internallob.BLOB && !utf8.Valid(data) {
		return 0, common.NewOracleError(oracleErrors.InvalidLobInput, nil, "invalid UTF-8")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	release, err := h.acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer release()
	h.pending = nil
	writtenBytes := 0
	for writtenBytes < len(data) {
		end := writtenBytes + int(defaultChunkBytes)
		if h.kind != internallob.BLOB {
			// Move to a UTF-8 rune boundary before issuing a character-LOB call.
			for end < len(data) && !utf8.RuneStart(data[end]) {
				end--
			}
		}
		if end > len(data) {
			end = len(data)
		}
		if end <= writtenBytes {
			return writtenBytes, common.NewOracleError(oracleErrors.InvalidLobInput, nil, "invalid UTF-8 chunk")
		}
		part := data[writtenBytes:end]
		expectedLogical := uint64(len(part))
		if h.kind != internallob.BLOB {
			expectedLogical = utf16LogicalLength(part)
		}
		logical, writeErr := h.session.LobWrite(ctx, h.kind, h.locator, h.offset, part)
		if writeErr != nil {
			return writtenBytes, writeErr
		}
		if logical == 0 || logical > expectedLogical {
			return writtenBytes, io.ErrShortWrite
		}
		h.offset += logical
		if h.offset-1 > h.length {
			h.length = h.offset - 1
		}
		if logical != expectedLogical {
			acceptedBytes := int(logical)
			if h.kind != internallob.BLOB {
				acceptedBytes = utf8BytesForLogicalPrefix(part, logical)
			}
			return writtenBytes + acceptedBytes, io.ErrShortWrite
		}
		writtenBytes += len(part)
	}
	return writtenBytes, nil
}

// WriteTo copies the remaining value to writer using a background context.
func (lob *LOB) WriteTo(writer io.Writer) (int64, error) {
	return lob.WriteToContext(context.Background(), writer)
}

// WriteToContext copies the remaining value to writer.
func (lob *LOB) WriteToContext(ctx context.Context, writer io.Writer) (int64, error) {
	if writer == nil {
		return 0, common.NewOracleError(oracleErrors.InvalidLOBBuffer, nil, "nil LOB writer")
	}
	var total int64
	buf := make([]byte, int(defaultChunkBytes))
	for {
		n, readErr := lob.ReadContext(ctx, buf)
		if n > 0 {
			written, writeErr := writer.Write(buf[:n])
			total += int64(written)
			if writeErr != nil {
				return total, writeErr
			}
			if written != n {
				return total, io.ErrShortWrite
			}
		}
		if errors.Is(readErr, io.EOF) {
			return total, nil
		}
		if readErr != nil {
			return total, readErr
		}
	}
}

// Size returns bytes for BLOBs and UTF-16 logical units for CLOBs and NCLOBs.
func (lob *LOB) Size() (int64, error) { return lob.SizeContext(context.Background()) }

// SizeContext returns the current logical LOB length.
func (lob *LOB) SizeContext(ctx context.Context) (int64, error) {
	h, err := lob.liveHandle()
	if err != nil {
		return 0, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	release, err := h.acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer release()
	length, err := h.session.LobLength(ctx, h.kind, h.locator)
	if err != nil {
		return 0, err
	}
	h.length = length
	if length > math.MaxInt64 {
		return 0, common.NewOracleError(oracleErrors.InvalidLOBBuffer, nil, "LOB length overflow")
	}
	return int64(length), nil
}

// ChunkSize returns Oracle's storage chunk size in bytes.
func (lob *LOB) ChunkSize() (int64, error) { return lob.ChunkSizeContext(context.Background()) }

// ChunkSizeContext returns Oracle's storage chunk size in bytes.
func (lob *LOB) ChunkSizeContext(ctx context.Context) (int64, error) {
	h, err := lob.liveHandle()
	if err != nil {
		return 0, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	release, err := h.acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer release()
	size, err := h.session.LobChunkSize(ctx, h.kind, h.locator)
	if err != nil {
		return 0, err
	}
	if size > math.MaxInt64 {
		return 0, common.NewOracleError(oracleErrors.InvalidLOBBuffer, nil, "LOB chunk size overflow")
	}
	return int64(size), nil
}

// Trim changes the server-side LOB length using a background context.
func (lob *LOB) Trim(length int64) error { return lob.TrimContext(context.Background(), length) }

// TrimContext changes the server-side LOB length. Length is bytes for BLOBs
// and UTF-16 logical units for CLOBs and NCLOBs.
func (lob *LOB) TrimContext(ctx context.Context, length int64) error {
	if length < 0 {
		return common.NewOracleError(oracleErrors.InvalidLOBBuffer, nil, "negative LOB length")
	}
	h, err := lob.liveHandle()
	if err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	release, err := h.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	result, err := h.session.LobTrim(ctx, h.kind, h.locator, uint64(length))
	if err != nil {
		return err
	}
	h.length = result
	if h.offset > result+1 {
		h.offset = result + 1
	}
	h.pending = nil
	return nil
}

// Open opens the locator for read and write access using a background context.
func (lob *LOB) Open() error { return lob.OpenContext(context.Background()) }

// OpenContext opens the locator for read and write access.
func (lob *LOB) OpenContext(ctx context.Context) error {
	h, err := lob.liveHandle()
	if err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	release, err := h.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	opened, locator, err := h.session.LobOpen(ctx, h.kind, h.locator, openReadWrite)
	if err != nil {
		return err
	}
	if len(locator) != 0 {
		h.locator = append(h.locator[:0], locator...)
	}
	h.opened = h.opened || opened
	return nil
}

// CloseServer closes an explicitly opened locator on the Oracle server.
func (lob *LOB) CloseServer() error { return lob.CloseServerContext(context.Background()) }

// CloseServerContext closes an explicitly opened locator on the Oracle server.
func (lob *LOB) CloseServerContext(ctx context.Context) error {
	h, err := lob.liveHandle()
	if err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	release, err := h.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	locator, err := h.session.LobClose(ctx, h.kind, h.locator)
	if err != nil {
		return err
	}
	if len(locator) != 0 {
		h.locator = append(h.locator[:0], locator...)
	}
	h.opened = false
	return nil
}

// IsOpen reports the Oracle server-side open state using a background context.
func (lob *LOB) IsOpen() (bool, error) { return lob.IsOpenContext(context.Background()) }

// IsOpenContext reports the Oracle server-side open state.
func (lob *LOB) IsOpenContext(ctx context.Context) (bool, error) {
	h, err := lob.liveHandle()
	if err != nil {
		return false, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	release, err := h.acquire(ctx)
	if err != nil {
		return false, err
	}
	defer release()
	return h.session.LobIsOpen(ctx, h.kind, h.locator)
}

// Close releases local Go state. If Open succeeded, call CloseServer first.
func (lob *LOB) Close() error {
	if lob == nil || lob.handle == nil {
		return nil
	}
	h := lob.handle
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.opened && h.session != nil && h.session.ValidateLOBLease() == nil {
		return common.NewOracleError(oracleErrors.UnsupportedLobOperation, nil, "close server-open LOB before local Close")
	}
	h.closed = true
	h.locator = nil
	h.pending = nil
	return nil
}

// Free releases a temporary locator or the local state for a persistent
// locator using a background context. It is safe to call more than once.
func (lob *LOB) Free() error { return lob.FreeContext(context.Background()) }

// FreeContext releases a temporary locator or the local state for a persistent
// locator. Persistent data stored in Oracle is never deleted by this method.
func (lob *LOB) FreeContext(ctx context.Context) error {
	if lob == nil || lob.handle == nil {
		return nil
	}
	h := lob.handle
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil
	}
	if h.opened && h.session != nil && h.session.ValidateLOBLease() == nil {
		return common.NewOracleError(oracleErrors.UnsupportedLobOperation, nil, "close server-open LOB before Free")
	}
	if !h.temporary {
		h.closed = true
		h.locator = nil
		h.pending = nil
		return nil
	}
	release, err := h.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	if err := h.session.LobFree(ctx, h.locator); err != nil {
		return err
	}
	h.closed = true
	h.locator = nil
	h.pending = nil
	return nil
}

func newHandle(session internallob.LOBSession, kind internallob.Kind, locator []byte, temporary bool) *lobHandle {
	return &lobHandle{session: session, kind: kind, locator: append([]byte(nil), locator...), offset: 1, temporary: temporary}
}

func (lob *LOB) liveHandle() (*lobHandle, error) {
	if lob == nil || lob.handle == nil {
		return nil, common.NewOracleError(oracleErrors.LobValueInvalidated, nil, "zero-value LOB")
	}
	h := lob.handle
	if err := h.session.ValidateLOBLease(); err != nil {
		return nil, err
	}
	return h, nil
}

func (h *lobHandle) acquire(ctx context.Context) (func(), error) {
	if h.closed {
		return nil, common.NewOracleError(oracleErrors.LobValueClosed, nil, "after Close or Free")
	}
	if h.session == nil {
		return nil, common.NewOracleError(oracleErrors.LobValueInvalidated, nil, "missing connection capability")
	}
	return h.session.AcquireLOBLease()
}

func utf16LogicalLength(data []byte) uint64 {
	var length uint64
	for _, r := range string(data) {
		length++
		if r > 0xFFFF {
			length++
		}
	}
	return length
}

func utf8BytesForLogicalPrefix(data []byte, logical uint64) int {
	var units uint64
	bytes := 0
	for _, r := range string(data) {
		n := uint64(1)
		if r > 0xFFFF {
			n++
		}
		if units+n > logical {
			break
		}
		units += n
		bytes += utf8.RuneLen(r)
	}
	return bytes
}

var (
	_ io.Reader = (*LOB)(nil)
	_ io.Writer = (*LOB)(nil)
	_ io.Closer = (*LOB)(nil)
)
