# Dactyl's SQLite Connector Boundary

## Summary

Dactyl's local route is a real SQLite database. `DatastoreRoute::sqlite(path)`
opens that path through the optional `sqlite` feature, and existing SQLite
files remain ordinary readable and writable databases. Dactyl does not define
a replacement file format, import step, SQL parser, journal, or query planner.

The driver boundary is deliberately small:

```text
application -> Dactyl facade -> SQLite file
                         \-> Neon/Propodus HTTP route
```

Dactyl owns route selection, parameter binding, access mode, atomic batches,
row normalization, generated-key results, schema projection, and stable error
categories. Decapod owns schema and migration policy. Propodus owns hosting,
authentication, tenancy, authorization, retries, and idempotency policy.

## Opening and durability

A read/write local route uses SQLite's normal read/write/create flags and
creates the requested parent directory when needed. A read-only route uses
SQLite's read-only flag and refuses a missing file with a typed
`missing_database` error. There is no Dactyl sidecar lock or journal.

Each connection enables foreign-key enforcement and configures SQLite's busy
timeout from `OpenOptions::lock_timeout`. SQLite remains responsible for its
pager, locking, rollback journal/WAL mode, crash recovery, and file
compatibility. Dactyl maps native outcomes such as busy, locked, read-only,
constraint, malformed database, and I/O failure to `AdapterErrorKind` values
without exposing SQLite loader or C-ABI types through the public API.

## Shared operation contract

The local and Neon adapters implement the same facade:

- `read(sql, params)` returns owned `Rows`.
- `write(sql, params)` returns affected rows; `write_result` also returns an
  integer generated key for a rowid insert when SQLite reports one.
- `atomic(&[Operation])` runs a mutating batch inside one SQLite transaction
  and rolls it back if any operation fails. Empty batches are no-ops.
- `OpenOptions { access_mode: ReadOnly, .. }` rejects writes before execution.
- `Parameter` supports NULL, boolean, integer, REAL, text, and blob values.
  SQLite booleans bind as integer `0`/`1`; blobs are normalized to JSON byte
  arrays in owned row projections and restored by `Row::get_blob`.

The public API never returns a backend handle. A caller can use SQLite SQL
supported by the linked SQLite version, while Dactyl still controls the
operation kind and does not add migration, retry, or business semantics.

## Schema projection

`Connection::inspect_schema()` converts SQLite's local catalog into the
backend-neutral `StoreSchema` type. It reports user tables, columns,
nullability, defaults, primary/unique keys, indexes, foreign keys, delete
actions, and row counts. SQLite catalog queries are private implementation
details; callers do not need to depend on `sqlite_schema` or PRAGMA response
layouts. Neon intentionally returns `unsupported_schema_inspection` because
hosted catalog policy belongs to the remote service boundary.

## Compatibility proof

`tests/sqlite_existing.rs` copies a checked-in Decapod SQLite fixture and
proves that it opens unchanged, exposes the expected catalog and values,
accepts a parameterized update, and reopens with the update intact. The same
test file covers NULL/text/integer/REAL/blob values, generated keys,
read-only/missing-path failures, and the actual SQLite magic header.

`tests/storage_contract.rs` covers transaction rollback, schema changes,
foreign-key cascades, typed constraints, separate connections, and a bounded
native SQLite lock timeout. `tests/storage_fixtures.rs` retains the
backend-neutral local matrix; its live Propodus row remains `unavailable`
unless an external service proof is explicitly supplied.

The connector is optional and isolated behind the `sqlite` feature. It uses a
small generic dynamic-loader dependency and resolves the host SQLite runtime
only when a local route is opened:

```text
sqlite feature -> libloading -> host shared SQLite library
```

No `rusqlite`, `libsqlite3-sys`, SQLite amalgamation, or bundled SQLite is
compiled into Dactyl or a consumer such as Decapod. Linux uses the host
`libsqlite3.so`, macOS uses the system `libsqlite3.dylib`, and Windows uses a
`sqlite3.dll` supplied by the deployment. `DACTYL_SQLITE_LIBRARY` can provide
an explicit library path when the host loader search path is non-standard.
If no compatible runtime is present, opening a local route fails with the
typed `sqlite_runtime_unavailable` outcome. The normal no-feature build
remains free of the local adapter.
