# Real-service adapter conformance

## Current native cloud qualification (issue 70)

The application path is logical `Backend::Cloud` → `LogicalConnector` → native
Supabase RPC, with the current managed user bearer and opaque `StorageContext`.
Propodus supplies the private SQL, Auth/onboarding infrastructure and effective
authorization. Explicit Neon and Supabase HTTP service routes remain compatibility
surfaces. Their `/query` and `/batch` evidence does not qualify native RPC.

Keep the following evidence profiles separate:

| Profile | Existing executable evidence surface | What it cannot establish |
| --- | --- | --- |
| `local_sqlite` | `tests/storage_fixtures.rs`, real local files | PostgreSQL, RLS or hosted execution |
| `neon_mock` | `tests/storage_fixtures.rs`, HTTP mock executing SQLite | Neon/PostgreSQL dialect or deployment parity |
| `synthetic_http` | `tests/supabase_rpc.rs`, `tests/cloud_connector.rs`, `tests/logical_connector.rs` | Physical transactions, managed OAuth or deployed authorization |
| Local native service | `tests/native_service.rs::local_native_read_only_contract` | Hosted execution, independent authorization or server-side mutation denial |
| Generic PostgreSQL service | The `dactyl_service_v1` fixture below | A task-only native RPC service or Supabase pooler modes |
| Protected native scenarios | `tests/native_service.rs::{local_native_scenarios,protected_hosted_native_scenarios}` | Client scenarios only; hosted/database qualification still requires actual execution and independent observations |

The local native test requires a private `DACTYL_NATIVE_FIXTURE` and a disposable
literal-loopback HTTP origin. Invoke it explicitly from the candidate checkout:

```sh
cargo test --locked --features supabase --test native_service \
  local_native_read_only_contract -- --ignored --exact
```

Supply `DACTYL_NATIVE_FIXTURE` through private environment configuration. The
test issues two reads in a read-only batch; its write rejection happens in the
client. It does **not** send a disguised mutation to prove physical PostgreSQL
read-only enforcement. Do not substitute a hosted URL: this fixture rejects it.
The normal proof suite tests refusal without the fixture, not a database pass.

### Protected native scenario fixture (version 2)

The separate v2 runner accepts operations and exact expectations from the service
owner. It does not install generic SQL in Propodus's private statement catalog.
Use a fresh disposable fixture for each run; the scenarios may mutate its data.
The v1 loopback interface above remains compatible with Propodus's existing
exact-source consumer test.

Keep the fixture in a trusted private directory outside Git, as a regular file
with mode `0600`. The loader rejects symlinks, changed file identity, group/other
permissions and input larger than 64 KiB. Diagnostics never echo JSON/OS errors.
The fixture owner must protect the directory against concurrent replacement.
Only ordinary user bearers and a publishable key enter this client.

The following shape uses synthetic placeholders; the operator supplies the exact
reviewed client/service commit IDs and statements supported by that service:

```json
{
  "version": 2,
  "profile": "local_native",
  "disposable": true,
  "independent_principals": true,
  "endpoint": "http://127.0.0.1:8080",
  "publishable_key": "sb_publishable_fixture",
  "schema": "dactyl_api",
  "client_revision": "1111111111111111111111111111111111111111",
  "service_revision": "2222222222222222222222222222222222222222",
  "principals": [
    {"token": "synthetic-one", "context": {"version": 1, "payload": {"service_owned": "scope"}}},
    {"token": "synthetic-two", "context": {"version": 1, "payload": {"service_owned": "scope"}}}
  ],
  "scenarios": [{
    "principal": 0,
    "access_mode": "read_only",
    "operations": [{"kind": "read", "sql": "SERVICE_OWNED_SQL", "params": []}],
    "expected": {"outcome": "success", "results": [{"Rows": {"rows": []}}]}
  }],
  "receipt_path": "/private/disposable/new-native-receipt.json"
}
```

Every scenario runs once through `Connection::atomic`, preserving operation and
result order. `principal` indexes 2–8 distinct fixture bearers. There are at most
32 scenarios, each with 1–16 read/write operations and at most 64 bound parameters
per operation; schema operations are excluded. The SDK's normal request/result
bounds and deadlines remain active. Each connection has explicit access mode,
native RPC route and opaque context, without ambient backend selection.

Success results use the public `OperationResult` serialization: rows are
`{"Rows":{"rows":[{"columns":["name"],"values":["value"]}]}}`; writes are
`{"Write":{"affected_rows":1,"generated_keys":["returned-id"]}}`. Types, column
order, row order, counts and keys must match exactly. An error expectation is
`{"outcome":"error","kind":"Authorization","origin":"RemotePolicy","mutation":"Uncertain"}`.
These are the public enum variant names. Match the actual SDK boundary: a remote
failed mutation may remain `Uncertain` even when an independent observer later
establishes rollback. A client-side `ReadOnly` refusal with `NotAttempted` cannot
stand in for server-side enforcement.

Supply the private path in `DACTYL_NATIVE_FIXTURE`, then run the local profile:

```sh
cargo test --locked --features supabase --test native_service \
  local_native_scenarios -- --ignored --exact
```

For an authorized disposable hosted fixture, set `profile` to `hosted_native`,
use an HTTPS origin, and additionally opt in:

```sh
DACTYL_NATIVE_HOSTED_PROOF=1 cargo test --locked --features supabase \
  --test native_service protected_hosted_native_scenarios -- --ignored --exact
```

Both profiles reject missing fixtures. An exclusive `0600` receipt is created
before requests; existing files and symlinks are never overwritten. It records
SDK/wire versions, operator-declared source revisions and completed scenario
count, with no endpoint, token, context, SQL, parameters, row data or remote error
text. A failure leaves an incomplete receipt identifying the next private
scenario index. A killed process or receipt-write failure cannot become a
complete pass. No request or failed mutation is automatically replayed.

`client_scenarios_passed` means only the supplied expectations matched. It does
not certify that the scenario inventory covers an issue's full acceptance matrix
or that the declared revisions were independently verified. For issue 70, the
service operator must supply typed roundtrips, returned counts/keys, cross-user
visibility, ordered commit/rollback and read-only probes; issue 85 additionally
requires controlled failures, independent concurrent callers and observations of
actual outcomes. A sequential scenario runner cannot establish concurrent CAS
winners by itself. Use read-kind mutation probes where the service supports
them, and retain independent evidence distinguishing catalog rejection from
physical PostgreSQL `READ ONLY`. This runner does not add fixture-only SQL to
production policy, orchestrate faults, provision principals or destroy resources.

The receipt always leaves independent DB/physical transaction evidence,
authorization provisioning, connection mode and cleanup pending. The operator
must bind its own observations and verified teardown to that exact run, including
on failure, and separately verify source revisions, registry checksum/consumer
provenance, database version and pooler mode. Hosted access is unavailable until
such a fixture is independently supplied and exercised.

Issue 70 owns the remaining hosted happy-path qualification: released SDK and
actual Decapod consumer, a disposable authorized scope, normalized typed values
and affected counts, ordered commit/rollback, independent database observations,
and verified cleanup. Record immutable client/service revisions, SDK version,
wire/profile version, database version and connection mode, commands and outcomes.
Attach sanitized artifact digests and distinguish `passed`, `failed`, `unavailable`
and deliberately unsupported cases. A receipt needs actual observations; source
presence, a GitHub release, HTTP 200 and skipped tests are insufficient.

Issue 79 owns the capability matrix; issue 85 owns fault/concurrency and uncertain
outcomes; issue 91 owns provider qualification; issue 94 owns registry handoff.
Propodus's `docs/proofs/cloud-integration.md` records historical local PostgreSQL/
PostgREST consumer evidence with a source-patched Dactyl. Preserve those revision
limits: they do not establish current registry-consumer or hosted acceptance.

## Existing generic HTTP service profile

This is the runnable client profile for [issue #93](https://github.com/DecapodLabs/dactyl/issues/93).
It tests the public `Connection` read/write/atomic API against an authenticated
HTTP service that executes real PostgreSQL. It does not implement that service,
provision Supabase, or send database passwords or service-role keys to Dactyl.

The profile is test-ready; no PostgreSQL or hosted pass is implied by adding it.
The normal Rust Contract workflow runs only network-free readiness tests and
the existing synthetic HTTP regressions. It also proves that explicitly asking
for the real profile without a fixture fails with `unavailable is not a pass`.

## Who supplies what

The trusted service/operator harness owns:

1. A fresh disposable PostgreSQL database and non-owner
   runtime role. It must never point this profile at existing application data
2. The schema in `tests/fixtures/supabase-service-schema.sql`, applied through
   the operator's database connection before starting the client profile
3. An actual authenticated `/query` and `/batch` service implementing the
   [public wire and SQL/type contract](supabase-service.md), explicitly allowing
   the profile's reviewed SQL against only its disposable schema
4. Two independently authenticated principals sharing that fixture, plus the
   service-owned denied-access cases. Opaque contexts select targets; they do
   not grant access. Tokens and contexts must be synthetic/disposable
5. A private fixture file outside the checkout, an absent receipt destination,
   independent database observations, and cleanup on both success and failure

A task-only service with a narrower SQL allowlist cannot run the wider type/key
profile without a service-owned conformance fixture. Do not weaken its
production SQL or authorization policy to make this test pass. An unsupported
request is an explicit incompatibility, not evidence that PostgreSQL itself
is broken. Creating a Supabase project alone is insufficient: the HTTP service,
schema and principals are separate prerequisites.

## Protected fixture

Create a regular file outside Git in an operator-owned private directory, using
restrictive permissions from creation (Unix `0600`, directory `0700`). Do not
paste real tokens into commands or logs. The operator fills this JSON through
its normal secret-handling flow; values below are illustrative, not credentials:

```json
{
  "version": 1,
  "profile": "local_postgres",
  "disposable": true,
  "independent_principals": true,
  "sql_profile": "dactyl_service_v1",
  "endpoint": "http://127.0.0.1:8080",
  "allowed": [
    {"token": "FIRST_DISPOSABLE_USER", "context": {"version": 1, "payload": {"target": "disposable-scope"}}},
    {"token": "SECOND_DISPOSABLE_USER", "context": {"version": 1, "payload": {"target": "disposable-scope"}}}
  ],
  "authentication_denied": {
    "token": "REJECTED_DISPOSABLE_SESSION",
    "context": {"version": 1, "payload": {"target": "disposable-scope"}}
  },
  "authorization_denied": {
    "token": "VALID_BUT_UNAUTHORIZED_DISPOSABLE_USER",
    "context": {"version": 1, "payload": {"target": "disposable-scope"}}
  },
  "receipt_path": "/private/disposable/client-receipt.json"
}
```

For the hosted test use `profile: "hosted_supabase"` and an HTTPS service URL.
Context payload fields belong to the service; `target` is only an example, not
a Dactyl authorization schema. Distinct tokens and `independent_principals`
are fixture prerequisites, not proof of principal independence. The operator
must independently establish their actual identities and grants.

Unknown fields are rejected. Do not add database credentials or shell hooks.
`receipt_path` must be absolute and not already exist; the test creates it
exclusively with restrictive Unix permissions before any service request. Use
a fresh path for every attempt. A partial/failed receipt is not a passing run.
Protected fixture loading currently requires Unix permission checks; other
platforms fail explicitly rather than claiming the file was protected.

The generic profile has one authentication-denied and one authorization-denied
principal/context case; both must deny reads and writes. Run it with fresh
fixtures for service-specific denied-session or denied-scope categories where
that expectation applies. A role that may read but may not write needs separate
service policy coverage and cannot be used as this profile's denied principal.
This profile does not choose or implement a production tenant policy or replace
the service's complete access-control matrix.

## Run the local PostgreSQL profile

No hosted Supabase account is needed for this profile. The operator may use a
disposable local PostgreSQL server and the real service's supported test
launcher. The endpoint must use a literal loopback address with HTTP or HTTPS
(normal certificate validation still applies). Ordinary user-session tokens
are the only credentials the client receives.

From the exact Dactyl checkout being evaluated:

```sh
git rev-parse HEAD
DACTYL_SUPABASE_FIXTURE=/private/disposable/fixture.json \
  cargo test --locked --features supabase --test supabase_service \
  local_postgres_service_contract -- --ignored --exact --nocapture
```

Record the command's exit code. A missing fixture, unsupported SQL, failed
assertion or unavailable service is not a pass. Do not rerun a mutating profile
against its partly used fixture: first collect failure evidence, clean up that
fixture, and provision a fresh one. Dactyl does not retry writes automatically.

## Protected hosted profile

Run this only after an operator approves a dedicated disposable fixture on a
compatible hosted service. It requires HTTPS and a separate explicit opt-in:

```sh
DACTYL_SUPABASE_HOSTED_PROOF=1 \
DACTYL_SUPABASE_FIXTURE=/private/disposable/hosted-fixture.json \
  cargo test --locked --features supabase --test supabase_service \
  protected_hosted_supabase_service_contract -- --ignored --exact --nocapture
```

No hosted endpoint or credential is supplied by this repository. Run and label
each supported service-side connection mode separately. A run against direct
local PostgreSQL cannot establish actual Supabase/Supavisor direct, session
pooler or transaction pooler behavior. The service owner must provide its driver,
prepared-statement configuration and actual mode-specific evidence; untested
or unsupported modes remain explicit.

## Independent database evidence and cleanup

The client checks what it can observe through `Connection`, including a fresh
second principal. Its receipt is deliberately a client result, not a claim
that it independently observed PostgreSQL, RLS or cleanup.

Before removing the disposable resources, the external harness must use a
separate trusted database observer to verify the receipt's expected persisted
rows and counts. Also capture service-side transaction/authorization evidence:

- Ordered batches commit together; a deliberate constraint failure rolls back
  the preceding mutation instead of merely returning an error response
- Zero-row stale CAS changes no row, and denied/disguised-read mutations leave
  the persisted fixture unchanged
- The rejected schema operation leaves the original table definition unchanged,
  including no added `forbidden_column`; named-column row queries alone do not
  establish this
- Each bearer resolves to the intended independently provisioned principal;
  contexts are received intact, and rejected principals/scopes cannot observe
  or mutate another target
- Read-only execution is enforced by the PostgreSQL execution boundary, even
  for writes labeled as reads inside otherwise read-write batches

For `dactyl_service_v1`, the final table must contain exactly two rows:

| id | flag | signed_value | real_value | text_value | blob bytes | nullable_text | version |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 101 | true | -9007199254740993 | 1.25 | committed update | 0, 1, 127, 255 | NULL | 2 |
| 202 | false | 42 | -2.5 | second row | empty | present | 1 |

The independent observer can inspect the complete table using its own trusted
database connection (never the Dactyl endpoint):

```sql
SELECT id, flag, signed_value, real_value, text_value,
       encode(blob_value, 'hex') AS blob_hex, nullable_text, version
FROM dactyl_service_conformance
ORDER BY id;
```

Check both the two rows and the absence of any additional rows. The deliberately
failed batches and denied mutations must not leave extra rows or altered values.
These final-state observations complement, rather than replace, service-side
transaction and read-only enforcement evidence.

Retain the exact Dactyl/service revisions, PostgreSQL version, supported SQL
profile, commands, sanitized results, independent observations and cleanup
outcome. Never retain the protected fixture, tokens, contexts, database DSNs,
service-role keys, raw request logs or unredacted errors as public artifacts.

Cleanup belongs to the external harness, not the client test. Use its normal
bounded teardown/finally mechanism to stop the service and destroy only the
database and roles allocated for this run, on success or failure. Verify the
resources are gone. A killed client or failed assertion must not skip teardown.

Do not claim all of #93, production onboarding, tenant authorization or hosted
acceptance from network-free tests or the client receipt alone. Local service
proof, hosted proof and each pooler mode remain separate results.
