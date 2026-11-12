# Bounded snapshot inspection

Dactyl owns physical execution. Decapod owns the semantic inventory requested
by Decapod #1630 and Dactyl #101. This API adds no application tables, event
format, schema migration, retry policy or garbage collection.

Open a dedicated connection with `AccessMode::ReadOnly`, then call
`Connection::inspect_snapshot(SnapshotLimits::default(), callback)`.
`SnapshotReader::visit` delivers one owned legacy `Row` at a time. Return
`false` to stop deliberately: `ScanOutcome.complete` will be false.
`read` retains the owned `Rows` compatibility representation. Visitors must
not reenter the connection or perform unbounded work.

SQLite holds one deferred read transaction. The first read fixes the snapshot;
later writes on another connection are invisible. Returning, failing, stopping
or unwinding releases statements, rolls back the read transaction, unregisters
the native progress callback and restores connection-local limits.

| Budget | Default | Enforcement |
| --- | ---: | --- |
| Duration | 5 seconds | VM callback and admission/return checks |
| Approximate SQLite VM instructions | 100,000,000 | Callback every 1,000 instructions |
| Queries | 4,096 | Before preparation |
| Returned rows | 1,000,000 | Before row decoding |
| Conservative decoded result charge | 64 MiB | Before allocation; includes names, row metadata and byte-array BLOB expansion |
| Cell/SQLite row | 8 MiB | Native LENGTH limit and pre-decode cell checks |
| Columns | 128 | Native COLUMN limit and pre-decode check |
| SQL | 64 KiB | Before preparation plus native SQL_LENGTH limit |

Limits are shared across the callback. Positive explicit budgets may be selected;
duration cannot exceed 300 seconds. Exhaustion is
`Quota/inspection_budget_exceeded`, never successful truncation. Invalid
budgets fail before a transaction. SQL is limited to SELECT/WITH and the native
statement must be read-only. PRAGMA is rejected because preparation itself can
change settings. Use read-only pragma table-valued SELECTs.

These limits are not process-wide RSS, allocation or hard wall-time guarantees.
SQLite may allocate internal execution state; progress callbacks cannot interrupt
arbitrary blocked filesystem calls or caller callback work. The existing lock
timeout still bounds SQLite lock waits. JSON byte-array BLOBs retain their legacy
representation and are conservatively charged at 32 bytes per raw byte before
decoding. No compression or remote streaming support is claimed.

Read-write handles return `inspection_requires_read_only`. Neon/Supabase return
`Capability/unsupported_snapshot_inspection` without network I/O or local
fallback. This capability does not alter existing read-only batch consistency,
the public `Row`/`Rows` layout, or ordinary connection behavior.

## Verification and repository readiness

`tests/sqlite_inspection.rs` covers a stable snapshot across concurrent WAL
writes, mutation and PRAGMA rejection, missing-file refusal, scoped row/byte/VM
budgets, early-stop cleanup and empty text/BLOB/NULL/signed-integer values.

The accepted repository-local proof contract runs `tools/proof/check.py` in an
immutable snapshot: formatting, all-feature tests, all seven optional feature
profiles, explicit refusal of an unavailable requested PostgreSQL fixture, and
clippy, the Rust 1.75 MSRV/clean consumer and package verification. It uses the
reviewed Linux ARM64 Rust 1.96.1/Python 3.11/SQLite toolchain
and a PATH containing only declared tools. Its tool locations are validation
requirements, not installed credentials or captured machine state. Native
library tests prove the executing local engine and synthetic HTTP contracts;
they do not attest a deployed PostgreSQL service. Hosted Pages and release
publication are explicit CI-only limitations, not passed checks.
