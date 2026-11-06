# Typed LOB example

This runnable example uses the public typed LOB API with BLOB, CLOB, and
NCLOB columns. It creates a temporary table, writes values with the public bind
markers, reads persistent LOBs after closing their query rows, reads multiple
rows from a pooled `*sql.DB`, and creates and frees temporary LOBs.

## Run

From the repository root, provide an Oracle connection string and run:

```sh
ORACLE_DSN='user/password@host:port/service' go run ./examples/lob
```

The database user needs permission to create and drop the example table. CLOB
and NCLOB use the database character sets AL32UTF8 and AL16UTF16 respectively.
The example sets a small LOB prefetch size to exercise reads beyond the
prefetched row data.

## Source files

- [main.go](main.go) configures the connection, creates sample rows, and runs
  each scenario.
- [query_reads.go](query_reads.go) scans and reads persistent LOBs after
  `Rows.Close` while keeping a dedicated `*sql.Conn` open.
- [multiple.go](multiple.go) reads each LOB before advancing a pooled query to
  its next row.
- [typed_lob.go](typed_lob.go) writes and frees temporary BLOB, CLOB, and NCLOB
  values.

See the [LOB guide](../../documentation/lob.md) for API details, ownership,
character lengths, and cleanup requirements.
