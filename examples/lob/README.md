# Oracle LOB example

This runnable example demonstrates BLOB, CLOB, and NCLOB support through
`database/sql`:

- bind large values with `oracle.BindBlob`, `oracle.BindClob`, and
  `oracle.BindNClob`;
- read a complete value with `lob.Bytes` or `lob.Text`;
- stream a query result with `lob.LOB`; and
- create, modify, and promote LOBs with `lob.DirectLOB`.

The program creates its own tables, verifies deterministic data, and drops the
tables when each test finishes.

## Requirements

- Go and an Oracle Database user that can create and drop tables.
- Database character set `AL32UTF8` and national character set `AL16UTF16` for
  the CLOB and NCLOB tests.

## Run

From the repository root:

```shell
export ORACLE_DSN='user/password@host:port/service'
go run ./examples/lob
```

By default, the example runs all tests:

1. a boundary read with 32 KiB LOB prefetch;
2. a boundary read with 40 MiB LOB prefetch;
3. six independent LOB columns with 32 KiB prefetch;
4. the same six-column test with 8 MiB prefetch; and
5. temporary and persistent `DirectLOB` operations for BLOB and CLOB values.

To run one test category, set `ORACLE_LOB_TEST`:

| Value | Category |
| --- | --- |
| `all` | All three test categories (the default). |
| `boundary` | Two boundary checks: 32 KiB and 40 MiB prefetch. |
| `multiple` | Two six-column checks: 32 KiB and 8 MiB prefetch. |
| `direct` | Temporary and persistent BLOB/CLOB direct-LOB operations. |

For example:

```shell
ORACLE_LOB_TEST=boundary go run ./examples/lob
```

To change the prefetch used by the multiple-default test:

```shell
ORACLE_LOB_TEST=multiple \
ORACLE_LOB_MULTIPLE_PREFETCH_SIZE=4194304 \
go run ./examples/lob
```

`ORACLE_LOB_MULTIPLE_PREFETCH_SIZE` is a positive byte count. The boundary
tests use 32 KiB and 40 MiB and can allocate large values. For a smaller
local run, reduce `boundaryPrefetch32KiB`, `boundaryPrefetch40MiB`, and
`boundaryPayloadExtra64KiB` in [main.go](main.go).

## Public API patterns

### Bind LOB values

Use the marker matching the database column:

```go
_, err := db.ExecContext(ctx, query,
    oracle.BindBlob(binaryData),
    oracle.BindClob(clobText),
    oracle.BindNClob(nclobText),
)
```

### Materialize a query result

`lob.Bytes` is for BLOBs. `lob.Text` is for CLOBs and NCLOBs. These scanners
consume the value during `Scan`, so `QueryRowContext` is appropriate.

```go
var blob lob.Bytes
var text lob.Text
err := db.QueryRowContext(ctx, query).Scan(&blob, &text)
```

Materialization keeps the complete value in memory.

### Stream a query result

Scan into `lob.LOB` with `QueryContext`, and read it while its `Rows` remains
open:

```go
rows, err := db.QueryContext(ctx, query)
if err != nil {
    return err
}
defer rows.Close()

if !rows.Next() {
    if err := rows.Err(); err != nil {
        return err
    }
    return sql.ErrNoRows
}

var value lob.LOB
if err := rows.Scan(&value); err != nil {
    return err
}
defer value.Close()

_, err = value.WriteTo(destination)
```

Do not scan a streaming LOB into `lob.LOB` with `QueryRowContext`; the statement
may be closed as soon as `Scan` returns. `LOB.Size` and `LOB.ChunkSize`
report Oracle logical units: bytes for BLOBs and UTF-16 code units for CLOBs
and NCLOBs.

### Create a direct LOB

Direct LOBs remain tied to the connection used to create or open them, so use a
dedicated `*sql.Conn`:

```go
conn, err := db.Conn(ctx)
if err != nil {
    return err
}
defer conn.Close()

value, err := lob.CreateTemporary(ctx, conn, lob.CLOB)
if err != nil {
    return err
}
defer value.Free(ctx)

_, err = value.WriteContext(ctx, []byte("temporary text"))
```

Use `Read`, `Write`, `WriteTo`, `Trim`, `Open`, `CloseServer`, and `Size` as
needed. Call `Free` for temporary storage before closing the owning
connection.

`lob.OpenPersistent` transfers an unread query LOB to a `DirectLOB` on the same
`*sql.Conn`. Keep the rows open during the transfer; after it succeeds, the
rows may close and the direct LOB remains usable. Use a row-locking query such
as `FOR UPDATE` when the persistent value will be modified.

The direct-LOB test uses BLOB and CLOB to keep the lifecycle example small;
NCLOB is covered by the multiple-column test.

## Source files

- [main.go](main.go) — configuration and test-category selection.
- [boundary.go](boundary.go) — boundary scenario with materialized and streaming reads.
- [streaming.go](streaming.go) — streaming reads with `lob.LOB`.
- [multiple.go](multiple.go) — independent LOBs from one row.
- [direct_lob.go](direct_lob.go) — temporary and persistent `DirectLOB`s.
