# Native cloud failure and concurrency qualification

Issue 85 owns this adversarial proof contract. Issue 70 owns hosted happy-path
qualification and issue 79 the supported capability inventory. The primary
cloud route is native Supabase RPC with the current user bearer; explicit
Neon/service HTTP compatibility profiles need their own results.

Dactyl transports bounded operations and opaque context. Propodus enforces
identity, grants and physical SQL transactions. Decapod interprets task revisions,
proof applicability and recovery policy. None of the scenarios below authorizes
an agent to choose a weaker proof profile or assert publication readiness.

## Outcomes and required independent observations

| Injection/operation | Client claim permitted | Required service/database evidence |
| --- | --- | --- |
| Local budget rejects before dispatch | `LocalBudget`, `NotAttempted` where exposed | No request observed at the controlled service |
| Known local atomic failure | `RolledBack` where exposed | Original local state remains unchanged |
| Explicit remote denial or constraint result | Existing typed kind/code and conservative outcome | Effective principal and unchanged state/events; an HTTP error alone is insufficient |
| Remote commit, acknowledgement lost | `Uncertain`; reconcile before replay | Exactly committed state/event transaction, correlated independently |
| Truncated/malformed response or post-send timeout | Preserve uncertainty for mutations | Distinguish actual commit, rollback or unresolved server state |
| Concurrent supported CAS/claim | Physical count/result, not invented domain policy | One winner; observed losers; exact revision and event count |
| Read-only batch containing a disguised mutation | Typed refusal through the real execution boundary | Server transaction cannot persist the mutation |
| Revoked/expired bearer or wrong scope | Typed authentication/authorization denial | Verified principal/grants and no unauthorized reads/writes |
| Unsupported profile/version | Typed protocol/capability refusal | No execution under a silently selected alternative |

Client knowledge and actual database state are separate observations. A network
failure does not prove rollback. `FailureOrigin` and `MutationOutcome` in
`src/error.rs` conservatively classify storage errors; managed Auth/account
requests use the existing `outcome_unknown` code on ambiguous one-time or
mutating calls. Neither mechanism supplies automatic retry, an idempotency key
or a service outcome lookup API. Callers must use a documented service/caller
reconciliation path or preserve unresolved uncertainty.

Fault injection must occur in an operator-owned test service/proxy around a
disposable fixture. Use independent callers and observer connections, bounded
barriers/deadlines, and teardown on success, failure and interruption. Never
probe unrelated data or weaken production SQL/RLS to run a wider fixture.

## Existing tests and missing proof

`tests/resource_budgets.rs` covers local/synthetic budget failures, result bounds,
local rollback and conservative remote write outcomes. `tests/supabase_rpc.rs`
checks native envelopes/results/errors and read-only batch routing.
`tests/cloud_connector.rs` checks managed Auth/account request bounds, denial
mapping, credential separation and absence of automatic replay. Those are
valuable transport regressions; they do not execute real hosted SQL or Auth.

`tests/native_service.rs::local_native_read_only_contract` submits two native
reads, then rejects a write locally. Its passing result cannot establish the
disguised-read physical enforcement row above. A future protected native profile
must exercise that server boundary and independently observe unchanged state.

For each executed scenario retain immutable SDK/consumer/service revisions,
registry version, wire/profile version, fixture category, timeout/byte/row bounds,
sanitized results, independent before/after counts and cleanup outcome. Do not
retain bearer/refresh tokens, SCM credentials, DSNs, opaque tenant context or raw
request logs in Git. Missing fixtures are `unavailable`, deliberate capability
limits are `unsupported`, and a failed assertion is `failed`.

## Opaque governance transport

Coordinate Decapod issues 1038/1052 and Propodus issue 124 using existing opaque
payloads, exact evidence identities/digests and supported task revisions. The
service owns authenticated repository authority; Decapod owns applicability and
acceptance. Dactyl must not parse proof policy, accept agent success assertions,
split a logical atomic operation, discard fencing fields, or create another
evidence datastore. A synthetic payload round-trip does not prove service-level
capture or acceptance.

The contract is preparation for execution. It does not record any new real
PostgreSQL, hosted Supabase, managed OAuth or legacy Neon conformance pass.
