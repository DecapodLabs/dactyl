# Authenticated control RPC

`LogicalConnector::control_request` and `CloudConnector::control_request` send
one opaque JSON argument to a trusted native Supabase function. Enable `supabase`.
This is separate from account/billing routes and the existing SQL query/batch
carrier. Propodus still owns repository identity, accepted policy, effective
owner/delegated authority, expiry, revocation and audit decisions.

```rust,no_run
use dactyl_db::{DactylError, LogicalConnector, OperationKind, ResourceBudget, UserSession};
use serde_json::{json, Value};

fn repository_status(
    connector: &LogicalConnector,
    session: &UserSession, // restored from the application's private managed-session store
    request: &Value,      // the server's versioned repository-status request
) -> Result<Value, DactylError> {
    connector.control_request(
        "propodus_repository_access_v1",
        request,
        Some(&session.access_token),
        OperationKind::Read,
        ResourceBudget::v1().with_max_wire_bytes(16 * 1024),
    )
}
```

Resolve `LogicalTarget::cloud()` or a trusted explicit deployment origin once.
The connector uses the discovered project origin, publishable key and API schema;
`cloud_with_config` is only for independently trusted configuration or fixtures.
Each call accepts a single identifier (ASCII letters/digits/underscore, maximum
63 bytes, first character a letter/underscore), never a URL, path, SQL or schema.
The wire route is `POST /rest/v1/rpc/<function>` with `{"request": <opaque JSON>}`.
`Authorization: Bearer <current managed access token>`, `apikey`, `Content-Profile`,
`Accept-Profile`, JSON headers and `dactyl-contract-version: 1` use the native RPC
contract. No application version is inserted into the opaque request.

The function name and `OperationKind` are trusted application integration inputs,
not values to accept from arbitrary request JSON. `Read` only declares verified
absence of side effects for transport-loss classification; it does not enforce a
read-only PostgreSQL transaction or establish permission. Use `Write` for
`delegate_tier`, `revoke_tier` and **`evaluate_repair_authority`**, which inserts an
audit record, and for any operation with uncertain side effects. The driver does
not interpret action names. A denial returned as successful JSON remains opaque:
consumers must inspect the server's documented decision, not equate HTTP 200
with authorization. Caller-supplied owner labels are never authority.

Wire size and connect/total timeouts come from `ResourceBudget` (defaults 4 MiB,
5 seconds/15 seconds). The bound includes the outgoing `request` envelope and
applies to both successful and failed responses independently of Content-Length.
JSON values are limited to depth 64 by the shared native JSON guard. SQL, row,
cell and decoded-datastore-result limits do not apply to this opaque channel.
Oversized/deep requests fail before dispatch. Use a smaller wire bound where the
service requires it; Propodus currently accepts at most 16 KiB request JSON.

Missing/invalid managed bearer fails locally; expired/revoked bearer and permission
denials remain typed remote errors. Native schema/function errors, rate limits
and timeouts retain their error kinds and allowlisted codes. Diagnostic output
omits origin, bearer, key, payload, remote message and unknown remote code.
The server independently validates the bearer: Dactyl never interprets JWT role
claims as permission. Project configuration rejects secret/service-role keys;
applications must supply a managed user access token, never a service credential.
No SCM credential is forwarded and no local fallback or automatic refresh occurs.

There are no redirects or automatic replays. A lost/truncated/malformed/oversized
mutation acknowledgement or ambiguous gateway/server failure returns
`adapter_code() == Some("outcome_unknown")` and
`mutation_outcome() == MutationOutcome::Uncertain`, preserving the underlying
transport/timeout/protocol category. Reconcile server state through its supported
protocol. `is_retryable()` alone never authorizes replay. Explicit authentication,
authorization and rate denials keep their original codes. Read-only transport
loss has no mutation uncertainty. Existing query/execute/batch and the registered
`SELECT propodus.owner_repair_v1(...)` operation through `Connection::atomic`
retain their contracts; do not replace that repair carrier with a second one.

## Release and consumer migration

The first published release with this API is **dactyl-db 0.17.0** (Rust/Cargo
1.85 or newer). [Release v0.17.0](https://github.com/DecapodLabs/dactyl/releases/tag/v0.17.0)
and [the crates.io package](https://crates.io/crates/dactyl-db/0.17.0) were
published on 2026-11-14 after PRs #124 and #125 merged. The downloaded registry
archive was independently hashed and its `.cargo_vcs_info.json` matches release
commit `a25796172ee300873c76ba27c1ccd4d022b544d0`:

```text
SHA-256 15face02da6f675d5784497a7ddcb885de14c1d09e2173c14df83c0aadd74abd
```

Consumers migrating from 0.16.1 can select the supported registry interface:

```toml
[dependencies]
dactyl-db = { version = "=0.17.0", features = ["supabase"] }
serde_json = "1"
```

Reuse the current managed session's access token in the existing connector;
status is a verified read, while delegation/revocation/evaluation audit writes
must use `OperationKind::Write`. Reconcile an uncertain mutation through server
state. Keep the existing owner-repair batch carrier. A Git/path override is not
a substitute for this registry dependency.

`tools/proof/registry_consumer.py` creates a fresh registry-only consumer, checks
Cargo metadata and the lock checksum against the exact artifact above, hashes
the cached archive, and copies the control wire tests shipped in that release.
Their source digest is
`35c7fddd2fce9204c0c15fd851f53bdb244a731b41c4e5c935b7fcb25f27b9c7`.
`tools/proof/check.py` executes that published test contract with Rust 1.85 during
full validation, alongside the current checkout's original seventeen checks.
Neither the library nor this registry test contract is supplied by a source
overlay. These tests still prove synthetic HTTP compatibility, not hosted
identity or repository authority.

As reviewed on 2026-11-14, accepted Decapod `master` still pins Dactyl 0.16.1.
Its 0.17.0 dependency adoption, repository-access control wiring through its
private managed-session custody, and real consumer qualification remain tracked
in [Decapod #1590](https://github.com/DecapodLabs/decapod/issues/1590).
[Propodus #89](https://github.com/DecapodLabs/propodus/issues/89) owns the service
policy boundary. Registry publication does not imply that either consumer
integration or deployed authorization acceptance has passed.

`tests/control_rpc.rs` exercises the actual exported connector and native HTTP
boundary, including discovery, headers, opaque JSON, rejection, bounds, redaction,
timeouts and lost acknowledgements. These loopback fixtures prove transport only.
Native PostgreSQL/Propodus migration 016 authorization and hosted managed-Auth
acceptance are **unexecuted by this fixture suite** and must be independently
recorded with the deployed revision, SDK release, principals, grants/denials,
expiry/revocation, audits and cleanup. Mocked permission responses do not prove
owner authority or complete cloud owner repair.
