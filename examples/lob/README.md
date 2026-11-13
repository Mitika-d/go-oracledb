# Oracle LOB examples

This directory contains small, focused examples of the public LOB APIs. Each
case is selected independently so a user can follow one flow at a time. The
default case is the basic BLOB/CLOB bind and read.

For API details, ownership rules, text lengths, and cleanup, see the
[LOB guide](../../documentation/lob.md).

## Requirements

- Go and an Oracle Database user that can create and drop tables.
- Database character set `AL32UTF8` and national character set `AL16UTF16` for
  the CLOB and NCLOB scenarios.
- Set `ORACLE_DSN` to `user/password@host:port/service`.

## Run one case

From the repository root:

```shell
export ORACLE_DSN='user/password@host:port/service'
go run ./examples/lob
```

That runs the basic case. Choose another case with `-case`:

| Case | What it demonstrates |
| --- | --- |
| `basic` | Bind a small BLOB and CLOB, then scan them into materialized values. This is the default. |
| `streaming` | Scan typed BLOB/CLOB locators, close the source rows, then stream the values. |
| `temporary` | Create, write, inspect, open, close, trim, and free temporary BLOB/CLOB values. |
| `persistent` | Scan persistent BLOB/CLOB locators and read them while their dedicated connection stays open. |
| `multiple` | Bind and scan six columns together: two BLOBs, two CLOBs, and two NCLOBs. Uses an 8 MiB prefetch count by default. |
| `multiple-low` | Run the same six-column case with a 32 KiB prefetch count. |
| `boundary-low` | Compare materialized and streaming reads with a 32 KiB prefetch count. |
| `boundary-high` | Run the boundary scenario with a 40 MiB prefetch count. This large case is intended for focused testing. |
| `all` | Run every case, including the large boundary case. This can take a long time and allocate large payloads. |

For example, run the streaming case by itself:

```shell
go run ./examples/lob -case streaming
```

Run temporary and persistent LOB flows separately:

```shell
go run ./examples/lob -case temporary
go run ./examples/lob -case persistent
```

The six-column case accepts a prefetch override. Oracle interprets the count as
bytes for BLOBs and characters for CLOBs and NCLOBs:

```shell
go run ./examples/lob -case multiple -multiple-prefetch 4194304
```

The `ORACLE_LOB_MULTIPLE_PREFETCH_SIZE` environment variable is also accepted
as the default for `-multiple-prefetch`. Payloads exceed the selected count by
at least 65,536 units, so large overrides require substantial memory.

## Source files

- [main.go](main.go) — command-line case selection and scenario dispatch.
- [helpers.go](helpers.go) — database/table setup and shared payload helpers.
- [basic_bind_read.go](basic_bind_read.go) — basic materialized bind and read.
- [streaming_read.go](streaming_read.go) — typed locator streaming after row close.
- [temporary_lob.go](temporary_lob.go) — temporary LOB lifecycle.
- [persistent_lob.go](persistent_lob.go) — persistent typed LOB query flow.
- [multiple_columns.go](multiple_columns.go) — six LOB columns in one row.
- [prefetch_boundary.go](prefetch_boundary.go) — larger prefetch boundary scenario.
