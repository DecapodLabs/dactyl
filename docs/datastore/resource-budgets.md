# Resource Budgets, Streaming Reads, and Failure Semantics

Dactyl bounds per-operation and per-batch resource consumption across SQLite, Neon, and Supabase through an additive, versioned [`ResourceBudget`] contract (`RESOURCE_BUDGET_VERSION = 1`), incremental row visits (`Connection::visit_rows`), targeted SQLite catalog predicates (`Connection::has_table`, `Connection::has_column`), and explicit error-origin / mutation-state classifiers (`DactylError::failure_origin`, `DactylError::mutation_outcome`).

Existing `OpenOptions { access_mode, lock_timeout }` struct literals, `Rows` / `Row` callers, `SnapshotLimits` / `Connection::inspect_snapshot`, `DactylError` / `AdapterErrorKind` variants, and Neon / Supabase HTTP and PostgREST RPC (`dactyl_query_v1`, `dactyl_batch_v1`, `dactyl_read_batch_v1`) wire envelopes remain unchanged.

## Measured Dogfooding Inventory (`docs/datastore/dogfooding-inventory.json`)

Before choosing default bounds and maintenance optimizations, `/Users/arx/src/dactyl/.decapod/data/decapod.db` was measured via `decapod data database inspect --scope dactyl --rows-per-table 25000 --format json` over a read-only snapshot (`files_before == files_after`, zero bytes mutated):

- **Physical store footprint**: `13,344,768` apparent bytes (`3,258` pages × `4,096`-byte page size, `89` freelist pages, `WAL` journal mode).
- **Dominant growth table**: `events` holds `18,941` rows across `2,752` table B-tree pages (`11,272,192` allocated bytes, `10,351,232` payload bytes) plus `138` index pages (`idx_events_stream_seq_unique`: `364,544` bytes; `idx_events_subject`: `200,704` bytes), representing **`94.3%`** of live allocated B-tree bytes.
- **Secondary tables**: `agent_prompts` (`155,648` table bytes + `24,576` index bytes across `940` physical cells) and `authoring_bindings` (`16,384` table bytes across `24` cells); remaining governance tables occupy 1 page (`4,096` bytes) each.
- **Measurement boundary**: This inventory characterizes one local developer repository's append-heavy governance ledger. It is not evidence of multi-tenant production cloud traffic, hosted Supabase pooler behavior, or live Propodus workload distribution.

## `ResourceBudget` (Version 1)

`Connection::open_with_budget(route, options, context, budget)` validates `budget.validate()` before opening an adapter (requiring `version == 1` and non-zero `connect_timeout` / `request_timeout`). Existing constructors (`open`, `open_with_options`, `open_with_context`, `open_with_options_and_context`) apply `ResourceBudget::v1()`:

| Field | Default (`v1`) | Unit & Enforcement Seam | Typed Error (`kind` / `code`) |
|---|---|---|---|
| `max_context_bytes` | `65,536` (64 KiB) | Bounded JSON writer over `StorageContext` at `Connection` construction | `Quota` / `context_budget_exceeded` |
| `max_sql_bytes` | `262,144` (256 KiB) | UTF-8 byte length of `sql` checked before local prepare or HTTP serialization | `Quota` / `sql_budget_exceeded` |
| `max_cell_bytes` | `4,194,304` (4 MiB) | Per bound parameter (`Text`/`Blob` byte length; fixed scalar width) and per decoded cell before `Value` allocation | `Quota` / `cell_budget_exceeded` |
| `max_param_bytes` | `4,194,304` (4 MiB) | Checked `u64` sum of parameter bytes per statement and across all operations in `atomic` | `Quota` / `parameter_budget_exceeded` |
| `max_batch_operations` | `1,024` | Operation slice length in `Connection::atomic` before local `BEGIN` or HTTP send | `Quota` / `batch_budget_exceeded` |
| `max_wire_bytes` | `4,194,304` (4 MiB) | Streaming `BoundedVecWriter` on HTTP requests (`Quota` / `payload_too_large`) and chunked `read_bounded_body` (`max_wire_bytes + 1`) on HTTP responses without trusting `Content-Length` | `Quota` / `payload_too_large` (request), `Protocol` / `response_too_large` (response) |
| `max_columns` | `256` | Result column count checked once per query before row decoding | `Quota` / `column_budget_exceeded` |
| `max_rows` | `100,000` | Checked `u64` row count across `read`, `visit_rows`, or all read results inside `atomic` | `Quota` / `row_budget_exceeded` |
| `max_result_bytes` | `67,108,864` (64 MiB) | Checked `u64` conservative decoded-result tracker across column headers, row maps, scalars, strings, and `32x` BLOB JSON-array expansion | `Quota` / `result_budget_exceeded` |
| `connect_timeout` | `5s` | Per-request HTTP TCP/TLS connect deadline on Neon and Supabase worker threads | `Timeout` |
| `request_timeout` | `15s` | Per-request end-to-end HTTP header + body read deadline on Neon and Supabase worker threads | `Timeout` |

### Per-Call Scope and Decoded Memory Accounting

- **Per-operation / per-batch scope**: `ResourceBudget` bounds a single call (`read`, `visit_rows`, `write`, `write_result`, or `atomic`) on one `Connection`. Dactyl does not run a process-wide memory allocator or cross-connection scheduler; callers that open `K` concurrent connections must bound `K` to cap aggregate process RSS.
- **Pre-allocation BLOB accounting**: Normalized `Row` values represent SQLite and wire BLOBs as `serde_json::Value::Array` of byte numbers, which expands in memory by ~`24-32x` relative to raw bytes. `DecodedResultTracker::charge_raw_cell` charges `raw_bytes * 32 + 32` using checked `u64` arithmetic *before* `sqlite::Statement::value` or remote cell cloning allocates the decoded `Row`.
- **Bounded JSON nesting**: Remote JSON responses are checked against `MAX_JSON_DEPTH = 64` (`Protocol` / `malformed_response`) so deeply nested payloads fail predictably.

## Streaming Scans (`Connection::visit_rows`)

`Connection::visit_rows(sql, params, visitor)` visits normalized `Row` values incrementally across SQLite, Neon, and Supabase:

- Returning `Ok(true)` advances to the next row while enforcing `max_columns`, `max_rows`, `max_cell_bytes`, and `max_result_bytes`.
- Returning `Ok(false)` stops deliberately, returning `Ok(ScanOutcome { rows, complete: false })`. On SQLite, `Statement` drops deterministically before `visit_rows` returns so the statement lock is released immediately (subsequent `DROP TABLE` or schema writes on the same connection do not fail with `SQLITE_LOCKED`).
- Exceeding any `ResourceBudget` limit returns `Err(DactylError)` (`AdapterErrorKind::Quota`), never `Ok(ScanOutcome { complete: false })`.
- Note on HTTP adapters: Neon and Supabase JSON endpoints return one bounded response body (`<= max_wire_bytes`) per request, so `visit_rows` bounds decoded `Row` construction and callback termination over that bounded envelope rather than opening a server cursor.

## `FailureOrigin` and `MutationOutcome` Classification

Callers inspect `error.failure_origin()` and `error.mutation_outcome()` without parsing error text:

| Scenario | `FailureOrigin` | `MutationOutcome` | Caller Action |
|---|---|---|---|
| Local preflight limit (`sql`, `context`, `cell`, `param`, `batch`, `payload_too_large`), `ReadOnly`, or unsupported local/remote capability (`has_table` on remote) | `LocalBudget` (or `Other` for capability/read-only) | `NotAttempted` | Safe to fix caller input/route; no physical state changed |
| Local SQLite write/constraint failure, or local `atomic` batch failing on a later read-result budget (`row_budget_exceeded` / `result_budget_exceeded`) | `LocalBudget` / `Other` | `RolledBack` | SQLite rolled back the transaction (`0` rows persisted) |
| Remote explicit policy denial before commit (`401`/`403` authz, `429` rate limit, remote `Quota`) | `RemotePolicy` | `RolledBack` | Do not retry without updated credentials, quota, or backoff |
| Remote oversized response (`response_too_large`), invalid JSON, depth > 64, or mismatched batch result count on a read-only call | `MalformedResponse` | `RolledBack` (or `NotAttempted`) | Treat remote endpoint/version as incompatible |
| Post-send transport error, request/body timeout, HTTP `408`/`502`/`504`, `response_too_large`, malformed batch response, or post-response read-result budget failure on a mutating remote `write` or `atomic` batch | `Other` / `MalformedResponse` / `LocalBudget` | **`Uncertain`** | **Never replay blindly**: remote PostgreSQL may have committed before response delivery failed; reconcile by caller idempotency key or state query first |

## SQLite Recovery Amplification Reduction

`src/adapter/sqlite/recovery.rs` previously copied each recovered table row by decoding columns into `serde_json::Value` (incurring `32x` BLOB array allocation) and preparing a fresh `INSERT` statement per row under autocommit. Logical dump/reload now:

1. Keeps `PRAGMA journal_mode = PERSIST` on the private `0600` staging file, wraps table data restoration in a single `BEGIN IMMEDIATE` ... `COMMIT` transaction before `PRAGMA integrity_check` and final `DELETE`-mode activation, and preserves all umask, symlink, inode, and POSIX ACL/MAC fail-closed checks.
2. Prepares one parameterized `INSERT` statement per table and reuses it across rows via `sqlite3_reset` + `sqlite3_clear_bindings`, binding raw SQLite C-ABI integer/double/text/blob pointers directly from the source cursor without intermediate `Row` or `serde_json::Value` allocation. A multi-row large-BLOB store therefore recovers cleanly even when the handle's `max_result_bytes` is smaller than `32 * blob_len`.

## Cross-Repository Compatibility & Evidence Boundaries

- **Caller-owned component references**: `tests/storage_fixtures.rs` and `tests/resource_budgets.rs` exercise the compact component reference tuple `(component_kind, codec_version, byte_len, content_sha256)` alongside content-addressed blobs as ordinary caller SQL tables/parameters. Dactyl remains schema-agnostic and takes no ownership of Decapod or Propodus domain schemas.
- **Catalog predicates**: `Connection::has_table` and `Connection::has_column` prepare a single parameterized catalog query on SQLite (versus 161 statements for `inspect_schema` on a 32-table indexed fixture; see `docs/datastore/catalog-predicates.md`) and fail closed locally (`Capability` / `unsupported_schema_inspection`, `NotAttempted`) on Neon and Supabase without issuing network requests.

| Evidence Label | Scope | Status in Repository Suite |
|---|---|---|
| `local_sqlite` | Real host SQLite C-ABI execution, budgets, `visit_rows`, `has_table`/`has_column`, atomic rollback, online backup, and C-ABI streaming recovery | **Executed & proved** (`tests/resource_budgets.rs`, `tests/catalog_predicates.rs`, `tests/storage_fixtures.rs`, `tests/sqlite_maintenance.rs`, `src/adapter/sqlite/recovery/tests.rs`) |
| `synthetic_http` | Loopback HTTP & PostgREST RPC wire bounds, lying `Content-Length`, JSON depth, timeouts, `visit_rows`, and `MutationOutcome::Uncertain` | **Executed & proved** (`tests/resource_budgets.rs`, `tests/supabase_contract.rs`, `tests/supabase_rpc.rs`, `tests/supabase_service.rs`) |
| `neon_mock` | Executing loopback `/query` and `/batch` mock forwarding into SQLite | **Executed & proved** (`tests/storage_fixtures.rs`, `tests/conformance.rs`, `tests/catalog_capabilities.rs`) |
| `local_postgres` | Disposable external PostgreSQL service conformance (`DACTYL_SUPABASE_FIXTURE`) | **`unavailable`** (opt-in external prerequisite; missing fixture fails closed when explicitly invoked) |
| `hosted_supabase` | Protected hosted Supabase Auth/PostgREST/pooler conformance (`DACTYL_SUPABASE_HOSTED_PROOF`) | **`unavailable`** (opt-in external prerequisite) |
| `live_propodus` | Deployed Propodus / Vercel Neon service (`DACTYL_LIVE_PROPODUS_ROUTE`) | **`unavailable`** (explicitly recorded as `unavailable`, never `passed`) |
