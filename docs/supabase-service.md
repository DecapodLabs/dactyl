# Supabase-backed authenticated service contract

## Provider, transport, and activation

`supabase` is an opt-in Dactyl Cargo feature. `Datastore::Supabase` selects the
private HTTP adapter at connection construction. Its route constructor is
`DatastoreRoute::supabase(endpoint, Option<String>)`; ambient equivalents are
`DATASTORE=supabase`, `DATASTORE_ROUTE`, and `DATASTORE_TOKEN`. The token is a
user/session bearer for that service, never a shared Postgres password or
Supabase service-role key. Consumers must supply `StorageContext` through the
existing context-aware opening or operation API.

`DatastoreTransport::{Local,Http}` distinguishes transport from provider. No
Vercel deployment is implied. No default provider changes and no local fallback
occurs when authentication, configuration, capabilities or transport fail.

This is a client integration foundation. The service must separately opt in to
its Supabase backend. Selecting the client provider does not provision a project,
change a service's backend, authorize tenants, or migrate existing data.

`DatastoreRoute::validate()` checks the Supabase endpoint policy without any
network I/O, token or context. Consumers should call it before forwarding
credentials to a configured service's onboarding or session-refresh routes.
Opening also checks the bearer and context locally before the first request.

## Wire contract

The base URL exposes `POST /query` and `POST /batch`. Each request carries
`Authorization: Bearer <user-session-token>` and `Content-Type: application/json`.
Context is the existing opaque envelope `{ "version": 1, "payload": { ... } }`.
Dactyl validates its shape and preserves its content. Only the authenticated
service may decide whether the caller is allowed to use it.

A query request contains:

- `sql`: SQL string with positional `$1`, `$2`, ... bind placeholders
- `params`: ordered JSON scalar/byte-array parameters
- `kind`: `read` or `write` (additive to the existing query envelope)
- `access_mode`: `read_only` for every `read` call, otherwise the opened mode
- `context`: unmodified versioned context

A batch request contains `operations`, `access_mode`, and `context`. Each
operation retains the existing `kind` (`read`, `write`, `schema`), `sql`, and
`params`. The service must execute the entire list in one physical transaction,
in order, and roll back on any execution, authorization or normalization error.
It must enforce each `read` operation as read-only even inside a read-write batch.
An unsupported schema operation is a capability error, not a silently skipped
migration. The client sends one batch request and never emulates atomicity with
separate requests. An empty batch sends no request.

Every successful result contains `columns` (ordered strings), `rows` (objects
keyed by those columns), and explicit unsigned `affected_rows`. Writes normally
return empty columns/rows. Optional `generated_keys` contains only explicitly
returned integer/text keys; there is no ambient last-insert-id inference. Batch
responses contain `results` in operation order, exactly one per operation.
Duplicate column aliases cannot be represented by object rows and are rejected;
use unique aliases. Missing columns/counts are protocol errors, not implicit NULL
or guessed affected counts. A stale caller-owned CAS may return zero affected
rows; Dactyl does not invent domain conflict semantics.

A rejected request returns a non-2xx status with an optional
`{"error":{"code":"stable_code","message":"..."}}` envelope. Known codes
map to Dactyl error kinds. Unknown codes and all server messages are redacted;
status provides the fallback category. A failed or timed-out mutation has an
unknown physical outcome until the service/caller verifies it. Dactyl does not
retry writes or batches automatically.

## Portable SQL and type profile

The wire supports NULL, bool, signed 64-bit integers, finite real numbers, text,
and blobs encoded as JSON byte arrays (`[0, 255]`), not base64 text. Strings remain
strings. Returned JSON objects, non-byte arrays, unsigned integers above i64,
and other database types are capability errors; callers should explicitly cast
to supported values only if that preserves their intended meaning. NaN/infinity
parameters fail before transmission rather than becoming NULL.

PostgreSQL service implementations must bind parameters, not substitute strings,
normalize boolean/integer/real/text/bytea/NULL values, and use explicit keys or
`RETURNING` for generated keys. IDs should be supplied by callers or created with
PostgreSQL-compatible DDL. SQLite `INTEGER PRIMARY KEY`, AUTOINCREMENT, PRAGMA,
sqlite_master/catalog queries and SQLite-only maintenance are not portable.
Dactyl does not rewrite arbitrary SQL in this adapter. The service must expose
and enforce an explicit constrained SQL/schema profile and return
`unsupported_query`, `unsupported_schema`, or `unsupported_type` for unsupported
capabilities. A task-only service is not a full arbitrary Decapod datastore.

`inspect_schema`, `verify_integrity`, backup and recovery remain local SQLite
capabilities and fail explicitly on Supabase routes. Read-only handles reject
write/schema operations before transport; the service must also enforce
read-only execution in PostgreSQL, including writes disguised as reads.

## Supabase connection modes

Dactyl's hosted client has no PostgreSQL handle, credentials, prepared-statement
cache, or direct/session/transaction-pooler selector. These belong to the trusted
service/operator deployment and must not be confused with HTTP hosting.

Supabase documents prepared statements on direct and session connections, and
requires disabling them for transaction-pooler mode. A service must explicitly
configure its mode, driver and statement behavior and test that mode against
actual PostgreSQL/Supavisor before claiming support. This HTTP-only feature
makes no native direct/session/transaction-pooler compatibility claim.

Sources:
- https://supabase.com/docs/guides/database/connecting-to-postgres
- https://supabase.com/docs/guides/troubleshooting/disabling-prepared-statements-qL8lEL

## Security and bounded failure

Production endpoints require HTTPS certificate validation. Literal loopback
HTTP (`127.0.0.0/8` or `::1`) is accepted only to support local testing; do not
send production credentials to a local untrusted listener. `localhost` and other
DNS names are not an HTTP exception. URL credentials, query strings and fragments
are rejected. Redirects never forward a bearer/context to another endpoint.

Connection establishment is bounded at 5 seconds and each full request at 15
seconds. JSON request and response sizes are bounded at 4 MiB. Caller
`OpenOptions::lock_timeout` retains its existing local-lock meaning; it does not
configure the HTTP network budget. Debug/error output cannot echo endpoint URLs,
bearers, SQL, bind values or raw remote errors. Route/token accessors remain
explicit caller-owned secret access and must not be logged.

The facade remains synchronous. Each request owns its blocking HTTP client on a
dedicated worker thread so constructing, using and dropping a connection inside
a Tokio runtime does not panic. Callers should still use `spawn_blocking` when
they need to keep an async executor responsive. No worker survives its request.

## Evidence boundaries

The runnable [real-service conformance profile](supabase-conformance.md) uses
the public `Connection` API with an externally provisioned protected fixture.
Its local-PostgreSQL and hosted-Supabase tests are ignored by default. Selecting
either without the required fixture fails explicitly. Network-free fixture and
receipt tests establish harness readiness only; a successful client receipt
must be paired with service-owned independent database and cleanup evidence.

`tests/supabase_contract.rs` exercises real loopback HTTP with synthetic JSON
responses: bearer/context forwarding, scalar/blob projection, explicit counts,
zero-row CAS responses, ordered batch envelopes, denial/error mapping,
configuration, read-only client enforcement, redaction, protocol failures,
unsupported cells, redirects, request/response size bounds, deadlines before
headers and during body reads, and synchronous lifecycle safety inside both Tokio
runtime flavors. It does not execute SQL or
prove physical commit/rollback, RLS, prepared statements or live Supabase.

`tests/storage_fixtures.rs` continues to prove local SQLite and the old Neon
executing mock. That mock executes SQLite and is not PostgreSQL evidence.

Before production activation, a protected service suite must prove actual
PostgreSQL parameterized reads/writes, counts, zero-row CAS, all scalar/blob/NULL
types, ordered transaction commit/rollback, constraint/conflict errors and
physical read-only enforcement. It must also prove authentication, cross-scope
authorization denial, and the configured pooler's statement behavior. Use
random namespaced/disposable records and deterministic cleanup. Missing endpoint
or credentials is **unavailable**, never passed; neither synthetic fixtures nor
a successful client build satisfies this service gate. Full Decapod catalog and
schema compatibility remains separately unproven.
