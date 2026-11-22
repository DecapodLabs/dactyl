# Logical backend and managed authentication

Applications select local SQLite (`LogicalTarget::local(path)`) or Decapod Cloud
(`LogicalTarget::cloud()` / `LogicalTarget::cloud_with_origin(origin)`) per
workspace through `LogicalTarget` (`Backend::Local` or `Backend::Cloud`) and
resolve a per-workspace `LogicalConnector` via `LogicalConnector::resolve`,
`LogicalConnector::resolve_with_metadata`, `LogicalConnector::local`,
`LogicalConnector::cloud`, `LogicalConnector::cloud_with_origin`, or
`LogicalConnector::cloud_with_config`. `LogicalTarget`, `LogicalConnector`, and
portable session/account/proof types (`DEFAULT_CLOUD_ORIGIN`, `AccountOperation`,
`AuthScheme`, `RequestProof`, `RequestProofInput`, `RequestProofSigner`,
`CloudResponseMetadata`, `ScmProvider`, `ScmCredential`, `AuthenticationSecrets`,
`AuthenticationStart`, and `UserSession`) are exported unconditionally across all
Cargo feature combinations so consumers never duplicate
`#[cfg(feature = "supabase")]` glue. Dactyl resolves local storage to SQLite and
Decapod Cloud to native Supabase RPC without process-global state or ambient
environment variables; multiple local and cloud workspaces coexist independently
in one process. The default cloud origin is `DEFAULT_CLOUD_ORIGIN`; the Decapod
Cloud service layer (provisioned by legacy Propodus infrastructure) enforces
schema, RLS, managed Auth integration, and onboarding functions. Internal
infrastructure codenames remain strictly for backward-compatible wire routing
during cutover, not as user- or SDK-facing terminology.

`LogicalConnector::route` and `DatastoreRoute::for_backend` take the logical selection and optional
current user bearer; `LogicalConnector::open(options, context, bearer)` opens the
resolved datastore `Connection` directly. A Decapod Cloud connector discovers
version 1 of the public deployment profile at
`/functions/v1/decapod-onboarding/config`, falling back once to legacy
`/functions/v1/propodus-onboarding/config` only when HTTP `404` indicates the
canonical function slug is not yet deployed. Discovery sends neither
the bearer nor a project API key, follows no redirects, and rejects a profile
whose origin differs from the selected origin. Its publishable key and API
schema are validated before constructing the native RPC route. A secret key
cannot substitute for the publishable key. Failed cloud discovery or disabled
`supabase` compilation fails closed with a typed error and never falls back to
local SQLite. Calling managed-auth or account-readiness methods on a local
`LogicalConnector` fails closed with `AdapterErrorKind::Capability`
(`cloud_connector_required`) without network I/O. Existing explicit SQLite, Neon and
Supabase service constructors retain their contracts.

`CloudConnector::default_origin` supplies the production origin; a controlled
test/private deployment can set `DACTYL_CLOUD_ORIGIN`. `from_runtime` also accepts
the Dactyl-owned `DACTYL_CLOUD_PUBLISHABLE_KEY` for an explicitly provisioned
profile. These are connector runtime settings, not per-project Decapod policy.
The project-facing Decapod setting remains `backend = "local" | "cloud"`.

The same connector provides stateless authentication transport and an optional
synchronous caller-supplied proof-of-possession hook:

1. The caller generates `AuthenticationSecrets`, persists them privately and
   starts authentication for its opaque repository scope.
2. The caller opens the validated browser handoff (`begin` beneath either
   `/functions/v1/decapod-onboarding` or legacy `/functions/v1/propodus-onboarding`)
   and polls status. Dactyl sends only PKCE/poll challenges on start; the browser
   never receives the verifier or polling secret.
3. The caller checkpoints an in-flight marker before the one-time code exchange,
   saves the returned code, and checkpoints again before managed token exchange.
4. Managed Auth issues `UserSession`. The caller saves it privately before
   authenticated onboarding completion. Dactyl retains no durable credentials.
5. Subsequent native RPC, `control_request`, and `account_request` calls carry
   the current access token. By default Dactyl sends `Authorization: Bearer <token>`;
   when a caller attaches a synchronous [`RequestProofSigner`] (via
   `with_proof_signer` on `LogicalConnector`, `CloudConnector`, or
   `DatastoreRoute`), Dactyl passes `(method, canonical_url, access_token_sha256,
   optional_nonce)` so the caller can supply fresh RFC 9449 `DPoP` proofs
   (`Authorization: DPoP <token>` + `DPoP: <proof>`) or explicitly select
   `RequestProof::bearer()` compatibility mode. Dactyl never generates keys,
   touches OS keystores, or caches private key material. Server `DPoP-Nonce`
   challenge headers (including on `401` rejections) are surfaced via
   `RequestProofSigner::observe_nonce` and `CloudResponseMetadata` without
   exposing tokens, proofs, nonces, or origins in `Debug` or `DactylError`. When
   a signer is attached and a server rejects a DPoP-signed request with pre-execution
   HTTP `401` `use_dpop_nonce` and a bounded `DPoP-Nonce` header, Dactyl
   performs at most one bounded retry with a freshly signed proof carrying the
   new nonce (never retrying if the signer returns `AuthScheme::Bearer`, never
   retrying a second `use_dpop_nonce`, and never retrying post-send
   `outcome_unknown` failures on mutating calls), returning the final response's
   `CloudResponseMetadata` (including bounded `Retry-After`). Enabling
   `.with_device_gateway()` on `CloudConnector`, `LogicalConnector`, or
   `SupabaseRpcConfig` routes `AuthScheme::DPoP` datastore and control RPCs
   through `/functions/v1/decapod-onboarding/{cloud-query,cloud-batch,cloud-control}`
   (with HTTP `404` fallback on `cloud-control` to legacy
   `/functions/v1/propodus-onboarding/cloud-control` when configured). Refresh
   tokens go only to the configured origin's token or device-refresh endpoint,
   and session revocation uses managed logout or device revocation.

`UserSession::provider_token` preserves an optional credential returned by the
sign-in provider and redacts it in Debug. A refresh response may omit it; callers
own any decision to retain a prior provider credential in private storage.
Sign-in may use Google or another provider. Dactyl never infers an SCM provider
from that token. The account backend independently verifies a fresh linking
credential before granting SCM authority.

`CloudConnector::account_request` (and `account_request_with_metadata`) provides a
separate stateless account and device-onboarding channel.
Authentication completion confirms session handoff only. The caller persists
that session before account setup and requires the backend's explicit
`backend_authorized: true` before declaring cloud access ready. Registration and
status use server-issued project references; pricing and workspace capacities
remain backend-owned. A missing billing adapter returns `billing_not_configured`.

Its `AccountOperation` selects one fixed POST endpoint beneath
`/functions/v1/decapod-onboarding` (with automatic HTTP `404` fallback to legacy
`/functions/v1/propodus-onboarding` during cutover): `Register` uses
`account-register`, `Status` uses `account-status`, `Quote` uses `billing-quote`,
`Checkout` uses `billing-checkout`, `Manage` uses `billing-manage`, `Portal` uses
`billing-portal`, `BillingOveragePolicy` uses `billing-overage-policy`,
`DeviceChallenge` uses `device-challenge`, `DeviceEnroll` uses `device-enroll`,
`DeviceKeys` uses `device-keys`, `DeviceSessions` uses `device-sessions`,
`DeviceSession` uses `device-session`, `DeviceRefresh` uses `device-refresh`,
`DeviceStepUp` uses `device-step-up`, and `DeviceRevoke` uses `device-revoke`.
`AccountOperation::is_mutating` classifies `Register`, `Checkout`, `Manage`,
`Portal`, `BillingOveragePolicy`, `DeviceEnroll`, `DeviceSession`, `DeviceRefresh`,
`DeviceStepUp`, and `DeviceRevoke` as mutating/state-changing and `Status`,
`Quote`, `DeviceChallenge`, `DeviceKeys`, and `DeviceSessions` as non-mutating
calls. With `.with_device_gateway()` enabled, account, billing, `DeviceChallenge`,
`DeviceKeys`, `DeviceSessions`, and `DeviceStepUp` transmit managed
`Authorization: Bearer <token>`, `DeviceEnroll` and `DeviceSession` require both
`Authorization: Bearer <token>` and a `DPoP` proof (`device_proof_required`
when absent), `DeviceRefresh` requires a `DPoP` proof, and `DeviceRevoke`
supports either `Bearer` or `DPoP` proof according to the attached signer. The
JSON request and response remain opaque Decapod Cloud policy. Ordinary
registration, status, device, and paid-seat access use the current user access
token and `None` for the optional SCM credential. An explicit SCM-linking call
may pass `Some(&ScmCredential::github(token)?)` or the corresponding `gitlab`
constructor. Each validates an opaque header-safe token, keeps it private and
redacts Debug. `ScmProvider::{GitHub,GitLab}` labels transport metadata; it does
not establish authorization. The connector sends `X-SCM-Token` together with
`X-SCM-Provider: github` and `X-SCM-Host: github.com`, or the fixed `gitlab`
and `gitlab.com` pair. Caller-selected hosts are unsupported.

These three headers are added only to the selected account request, never to
managed Auth, discovery or native data requests. Dactyl does not persist SCM
credentials. Callers decide consent and handling of an unknown checkout outcome;
Dactyl does not automatically replay mutating failures or retried nonce
challenges more than once. `DEFAULT_CLOUD_ORIGIN` remains unchanged at
`https://goyxpcmxetztjlrrcwew.supabase.co` unless overridden for controlled
testing via `DACTYL_CLOUD_ORIGIN` or `LogicalTarget::cloud_with_origin`.

Auth, account and discovery HTTP requests are bounded to a 5-second connect timeout,
15-second request timeout, 16 KiB bodies, and `MAX_JSON_DEPTH = 64` JSON nesting.
Across `Neon`, `Supabase`, and `CloudConnector` (`with_http_budget` / `open_with_budget`),
`ResourceBudget` explicitly governs transport pooling (`http_max_in_flight = 16` in `1..=64`,
`http_max_idle_per_host = 32` in `0..=64`, `http_idle_timeout = 15s`), backed by a
runtime-aware pool bounded globally at 64 in-flight slots. Synchronous callers reuse pooled
`reqwest::blocking::Client` directly outside Tokio or off-runtime inside Tokio, while native
`*_async` methods on `CloudConnector`, `LogicalConnector`, and `Connection` run directly
on the caller's Tokio runtime using per-runtime pooled `reqwest::Client` instances and
async `Notify` permits without worker-thread handoffs or FD exhaustion. No redirect follows
an error.

Per-operation authorization and effect provenance (`PROVENANCE_CONTRACT_VERSION = 1`)
is carried unconditionally across transport via `OperationAttempt`, `EffectProvenance`,
`EffectStatus` (`NotAttempted`, `Refused`, `CommittedObserved`, `RolledBack`, `Uncertain`,
`UnsupportedProvenance`), `DecisionOutcome` (`Allowed`, `Refused`), `ObservationSource`
(`DirectResponse`, `ServerReadback`, `LocalPreflight`, `TransportFault`), and
`ReconciliationObservation` (`write_with_attempt*`, `atomic_with_attempt*`,
`account_request_with_attempt*`, `control_request_with_attempt*`,
`reconcile_operation*`, and `reconcile_control_attempt*`). Validated server provenance
is bound strictly to the expected origin, authentication scheme, operation kind, caller
`attempt_id`, SHA-256 request digest, and optional `scope_ref`; header vs. JSON envelope
disagreements, wrong versions, redirects, mismatched origin/auth/scope/attempt refs, and
partial or truncated responses fail closed (treating truncated mutating responses as
`Uncertain` / `TransportFault` even when headers claim `committed_observed`). Mutating calls
never auto-retry after post-send socket loss or timeout; callers preserve the same
`OperationAttempt` identity across separate idempotent `reconcile_*` readbacks without
mutating earlier failure records, and same-`attempt_id`/different-digest mismatches surface
typed `AdapterErrorKind::IdempotencyConflict` (`idempotency_conflict` /
`provenance_digest_mismatch`). Unadvertised legacy responses report
`EffectStatus::UnsupportedProvenance` without fabricating receipts, and local SQLite
operations remain unaffected without remote policy requirements. Bounded `Decapod-Cloud-*`
diagnostic headers, `Sunset`, `Deprecation`, `Retry-After`, and `DPoP-Nonce` are captured
in redacted `CloudResponseMetadata` without interpreting application-layer governance or
billing policy, and all credential, proof, digest, origin, and correlation references are
redacted from `Debug` and `Display`. HTTP `409` maps to `AdapterErrorKind::Conflict` (or
`IdempotencyConflict` / `VersionConflict` on matching codes), `413` maps to
`AdapterErrorKind::Quota`, `429` with `quota_exceeded` or `overage_cap_exhausted`
preserves `AdapterErrorKind::Quota` while `429 rate_limited` preserves
`AdapterErrorKind::RateLimited`, and allowlisted public denial/DPoP codes are extracted
across Decapod Cloud (`error.code`), Supabase GoTrue v2 (`error_code` when `code` is
numeric), and OAuth2 (`error`) — including `use_dpop_nonce`, `invalid_dpop_proof`,
`dpop_proof_required`, `dpop_replay`, `dpop_clock_skew`, `dpop_aud_mismatch`,
`expired_credential`, `stolen_bearer`, `device_revoked`, `device_key_expired`,
`refresh_reuse_detected`, `device_not_enrolled`, `mfa_step_up_required`,
`provider_disabled`, `repo_access_denied`, `grant_revoked`, `grant_expired`,
`scope_denied`, `organization_seat_required`, `payment_pending`,
`repository_not_authorized`, `quota_exceeded`, `overage_cap_exhausted`,
`session_expired`, `session_revoked`, `session_not_found`, `bad_jwt`,
`refresh_token_already_used`, `custody_conflict`, `idempotency_conflict`,
`idempotency_in_progress`, and `billing_not_configured` — while remote error messages and
unknown codes are omitted. Post-send `5xx`, redirect, network, or response truncation
failures on one-time auth exchanges, token rotations, revocations, or uncertified mutating
operations attach `code = Some("outcome_unknown")` and `MutationOutcome::Uncertain` unless
certified server provenance explicitly establishes `RolledBack`, `NotAttempted`, or
`Refused`. Credential-bearing types redact Debug, and server error bodies are not exposed
in diagnostics.

Person identity, tenant/project isolation, assigned-seat entitlement and
revocation are backend policy enforced by Decapod Cloud.
Linked SCM identity is separate from code-host permission hierarchies. Dactyl
forwards opaque application context and credentials; neither a local agent label
nor a caller-supplied role creates backend authority. Privileged governance
operations remain separately controlled by the backend.

`tests/logical_connector.rs`, `tests/cloud_connector.rs`, `tests/http_pool_async.rs`,
`tests/effect_provenance.rs`, and `tests/supabase_disabled.rs` prove per-workspace
`LogicalTarget` / `LogicalConnector` isolation across concurrent local SQLite files and
separate loopback origins without ambient environment variables, portable redaction across
feature combinations, runtime-aware async/sync HTTP pool limits and 250-way concurrent
multi-tenant fanout without starvation or FD leaks, canonical `decapod-onboarding` discovery
with legacy 404 fallback, bounded `Decapod-Cloud-*` / `DPoP-Nonce` metadata redaction, DPoP
signer binding and challenge mapping, account/device route and credential separation,
per-operation effect provenance (`NotAttempted`, `Refused`, `CommittedObserved`, `RolledBack`,
`Uncertain`, `UnsupportedProvenance`), the full transport fault matrix (`before_send`,
`accepted_not_committed`, `committed_before_ack`, `partial_response`,
`timeout_with_late_effect`, 2 competing clients + 7 severed-socket retries without
duplicate commit, same-ID/different-digest conflict, wrong DPoP audience/nonce, redirect,
swapped refs, grant revocation), SQLite local compatibility, allowlisted GoTrue/OAuth2/
Decapod Cloud error codes, and async-runtime safety using `local_sqlite` and
`synthetic_http` fixtures. `tests/supabase_rpc.rs` proves native request/result
normalization. Those tests are not `local_postgres`, `hosted_supabase` OAuth, or live
Decapod Cloud deployment evidence (`unavailable` until exercised in deployment).
