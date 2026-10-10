# Real-service adapter conformance

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
