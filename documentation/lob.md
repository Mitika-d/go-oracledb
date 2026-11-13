# Typed Oracle LOB values

The `oracle/datatype/lob` package provides `lob.Blob`, `lob.Clob`, and
`lob.NClob` values for locator-backed operations. Zero values can be used as
`database/sql` scan destinations. BLOB data is exposed as bytes; CLOB and NCLOB
text is exposed as UTF-8. Text LOB sizes, offsets, and trim lengths use Oracle
UTF-16 logical units.

For a complete in-memory result, scan BLOBs into `lob.Bytes` and CLOBs or
NCLOBs into `lob.Text` (from `oracle/datatype/lob`). These scanners read the
entire locator while `Rows` is active and close it before returning. Plain
`[]byte` and `string` destinations do not accept the driver's locator-backed
query values. Materialized scans reject SQL `NULL`; typed `Blob`, `Clob`, and
`NClob` scans preserve NULL as an invalid value.

```go
var binary lob.Bytes
var text lob.Text
if err := db.QueryRowContext(ctx,
	"SELECT payload, body FROM documents WHERE id = :1", id,
).Scan(&binary, &text); err != nil {
	return err
}
```

## Scan a persistent LOB

Keep a dedicated `*sql.Conn` open while using a scanned locator. This lets the
value remain usable after `Rows.Close` without allowing Oracle locators to move
between pooled physical sessions.

```go
conn, err := db.Conn(ctx)
if err != nil {
	return err
}
defer conn.Close()

rows, err := conn.QueryContext(ctx, "SELECT payload FROM documents WHERE id = :1", id)
if err != nil {
	return err
}

var payload lob.Blob
if rows.Next() {
	if err := rows.Scan(&payload); err != nil {
		rows.Close()
		return err
	}
}
if err := rows.Err(); err != nil {
	rows.Close()
	return err
}
if err := rows.Close(); err != nil {
	return err
}

data, err := payload.ReadAllContext(ctx)
if err != nil {
	return err
}
_ = data
return payload.Close()
```

`Clob.ReadAllContext` and `NClob.ReadAllContext` return UTF-8 strings. The
`Read`, `Write`, `WriteTo`, `Size`, `ChunkSize`, `Trim`, `Open`, `CloseServer`,
`IsOpen`, and `Free` methods have context-aware variants as well as
background-context convenience methods. Reads and writes use a sequential
cursor. `ReadAll` consumes the remaining value.
`Blob.ReadAt` reads at a zero-based byte offset without changing that cursor.
`Clob.ReadTextAt` and `NClob.ReadTextAt` read at a zero-based UTF-16 logical
offset and return both UTF-8 bytes and the number of logical units consumed.

For queries made through `*sql.DB`, closing `Rows` can return its physical
connection to the pool and expire a live typed locator. If only a Go value is
needed, read it before advancing the result or closing `Rows`:

```go
var payload lob.Blob
if err := rows.Scan(&payload); err != nil {
	return err
}
data, err := payload.ReadAllContext(ctx)
if err != nil {
	return err
}
```

The returned bytes or text are independent of the connection. A live locator
cannot be used after its owning connection is closed or returned to the pool.

SQL `NULL` scans leave a typed value invalid. A non-NULL scan must match its
declared LOB family and contain a persistent table locator; temporary and
abstract locators are rejected. Use a temporary-LOB constructor for new
temporary values.

## Create and write a temporary LOB

Temporary LOBs belong to the session. Free them before closing their dedicated
connection.

```go
conn, err := db.Conn(ctx)
if err != nil {
	return err
}
defer conn.Close()

payload, err := lob.CreateTemporaryBlob(ctx, conn)
if err != nil {
	return err
}
defer payload.FreeContext(ctx)

if _, err := payload.WriteContext(ctx, []byte("payload")); err != nil {
	return err
}
return nil
```

Use `CreateTemporaryClob` or `CreateTemporaryNClob` for text values. Their
`Write` methods accept valid UTF-8 bytes, and `WriteString` is provided for
convenience. `Close` drops local Go state; `Free` queues cleanup of a temporary
LOB. For persistent query values, `Free` drops only local state and does not
delete database content. If `Open` succeeds, call `CloseServer` before local
`Close` or `Free`.

The driver currently validates text LOB behavior with database character set
`AL32UTF8` and national character set `AL16UTF16`.

Query prefetch size is controlled by
`oracle.OracleDriverProperties.DefaultLobPrefetchSize`. The default is 32 MiB;
the setting controls prefetched LOB data in bytes. A typed scanner retains the
prefetched bytes as the initial part of its sequential read.
