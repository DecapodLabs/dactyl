# Backend capability and evidence matrix

This is the issue 79 contract inventory for the accepted 0.16.0 source. A shared
Rust method does not imply identical SQL support or independent service proof.
Dactyl owns physical execution and normalization; callers own schemas, domain
policy and retries. Propodus owns effective hosted grants and private SQL.

| Capability | SQLite | Neon HTTP | Supabase service HTTP | Supabase native RPC |
| --- | --- | --- | --- | --- |
| Explicit construction | `DatastoreRoute::sqlite` | `DatastoreRoute::neon` | `DatastoreRoute::supabase` | `DatastoreRoute::supabase_rpc` |
| Logical connector | `Backend::Local`, file target | Explicit compatibility route | Explicit compatibility route | `Backend::Cloud`, discovered public profile |
| Single read/write | Local engine | `/query` service contract | `/query` service contract | `dactyl_query_v1`, service SQL allowlist |
| Ordered atomic operations | Physical transaction | One `/batch` request | One `/batch` request | One `dactyl_batch_v1` request |
| Read-only atomic operations | Local read-only enforcement | Same batch route, read-only envelope | Same batch route, read-only envelope | Separate `dactyl_read_batch_v1` route |
| Values/counts/explicit keys | Local normalization | Service result normalization | Service result normalization | Versioned RPC result normalization |
| `inspect_schema`, `has_table`, `has_column` | Supported | Explicit capability error | Explicit capability error | Explicit capability error |
| `inspect_snapshot` | Dedicated read-only snapshot | Explicit capability error | Explicit capability error | Explicit capability error |
| Integrity/online backup/recovery | Explicit local operations | Explicit capability error | Explicit capability error | Explicit capability error |
| `visit_rows`, resource budgets | Incremental engine reads | Bounded materialized HTTP response | Bounded materialized HTTP response | Bounded materialized HTTP response |
| Managed Auth/account transport | Cloud-only capability error | Separate from this route | Separate from this route | `CloudConnector` / cloud `LogicalConnector` |

Remote catalog and maintenance rejection occurs without a request. Remote
`visit_rows` is bounded visitation of an already decoded response; it is not
server-side streaming or a cross-request consistent snapshot. Empty atomic
batches are local no-ops. Generated keys are explicit service results, never an
assumption that PostgreSQL emulates SQLite integer-primary-key generation.

## Selection and compatibility

Prefer `LogicalTarget` / `LogicalConnector` for independent per-workspace local
and cloud access. They resolve the selected target without changing process
environment variables. Cloud discovers the public configuration, uses the
current user's managed bearer and never falls back to a local write on failure.
Disabled feature paths fail explicitly. Portable connector/session/account types
remain available across feature combinations; transport execution still needs
its optional feature.

`Connection::from_env` is a separate explicit ambient compatibility API:
`DATASTORE` selects `sqlite`, `neon` or `supabase`; `DATASTORE_ROUTE` supplies its
file/service route and `DATASTORE_TOKEN` the remote bearer. See the README's
environment contract and `src/lib.rs` for required/malformed input handling.
An explicit constructor supplies its own route rather than merging ambient
configuration. `DATASTORE=supabase` denotes the legacy service, not native RPC.
Never infer a new wire profile from a hostname or translate a PostgreSQL DSN into
an HTTP endpoint.

The local connector dynamically loads the host SQLite runtime through private
`src/adapter/sqlite/ffi.rs`. The former pure-Rust importer proposal is not the
current implementation requirement. Preserve real SQLite file compatibility;
do not restore an obsolete importer as a parity prerequisite.

## Evidence map and remaining qualification

| Source | Evidence scope | Remaining external obligation |
| --- | --- | --- |
| `tests/storage_fixtures.rs` | Real local SQLite and SQLite-backed executing Neon mock | Actual PostgreSQL/dialect and deployed compatibility |
| `tests/supabase_contract.rs`, `tests/supabase_rpc.rs` | Synthetic request/result/error mapping | Real transactions and effective service grants |
| `tests/logical_connector.rs`, `tests/cloud_connector.rs`, `tests/supabase_disabled.rs` | Local/synthetic routing, managed-auth envelopes, feature refusal | Managed OAuth/refresh and deployed discovery |
| `tests/catalog_capabilities.rs`, `tests/catalog_predicates.rs`, `tests/sqlite_inspection.rs` | Local catalog/snapshot and remote capability boundaries | No remote schema parity claim |
| `tests/resource_budgets.rs` | Local/synthetic bounds and outcome classification | Actual service resource/concurrency limits |
| `tests/native_service.rs` | Opt-in local read-only native batch client | Independent physical server enforcement and hosted proof |
| `tests/supabase_service.rs` | Opt-in generic `/query` and `/batch` fixture | Compatible service/operator fixture and independent observation |

These are executable proof surfaces, not a claim they were run in this document
change. Issue 70 owns protected happy-path execution and issue 85 owns adversarial
execution. Each external result needs exact client/service/profile identities,
observations and cleanup; unavailable and unsupported remain distinct.

The native task profile is deliberately narrower than arbitrary SQL or every
Decapod store. Extending it requires service-owned SQL/grants and a reviewed wire
profile, not adding domain semantics to Dactyl. Retries, idempotency and isolation
level selection remain caller/service contracts. A zero-row physical CAS is not
automatically a Dactyl conflict or a publication judgment.
