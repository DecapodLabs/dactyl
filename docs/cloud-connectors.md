# Logical backend and managed authentication

Applications select `Backend::Local` or `Backend::Cloud` per workspace through
`LogicalTarget` (`LogicalTarget::local(path)`, `LogicalTarget::cloud()`, or
`LogicalTarget::cloud_with_origin(origin)`) and resolve a per-workspace
`LogicalConnector` via `LogicalConnector::resolve`, `LogicalConnector::local`,
`LogicalConnector::cloud`, `LogicalConnector::cloud_with_origin`, or
`LogicalConnector::cloud_with_config`. `LogicalTarget`, `LogicalConnector`, and
portable session/account types (`DEFAULT_CLOUD_ORIGIN`, `AccountOperation`,
`ScmProvider`, `ScmCredential`, `AuthenticationSecrets`, `AuthenticationStart`,
and `UserSession`) are exported unconditionally across all Cargo feature
combinations so consumers never duplicate `#[cfg(feature = "supabase")]`
glue. Dactyl resolves local storage to SQLite and cloud storage to native
Supabase RPC without process-global state or ambient environment variables;
multiple local and cloud workspaces coexist independently in one process. The default cloud
origin is `DEFAULT_CLOUD_ORIGIN`; the Propodus IaC project provisions its schema,
RLS, managed Auth integration and onboarding function. Propodus is the name of
that infrastructure, not an additional application server in the request path.

`LogicalConnector::route` and `DatastoreRoute::for_backend` take the logical selection and optional
current user bearer; `LogicalConnector::open(bearer, options, context)` opens the
resolved datastore `Connection` directly. A cloud connector discovers version 1 of the public deployment
profile at `/functions/v1/propodus-onboarding/config`. Discovery sends neither
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

The same connector provides stateless authentication transport:

1. The caller generates `AuthenticationSecrets`, persists them privately and
   starts authentication for its opaque repository scope.
2. The caller opens the validated browser handoff and polls status. Dactyl sends
   only PKCE/poll challenges on start; the browser never receives the verifier
   or polling secret.
3. The caller checkpoints an in-flight marker before the one-time code exchange,
   saves the returned code, and checkpoints again before managed token exchange.
4. Supabase Auth issues `UserSession`. The caller saves it privately before
   authenticated onboarding completion. Dactyl retains no durable credentials.
5. Subsequent native RPC carries the current access bearer. Refresh tokens go
   only to the same origin's managed Auth token endpoint. Session revocation
   uses managed logout. Neither Dactyl nor Propodus issues a parallel session.

`UserSession::provider_token` preserves an optional credential returned by the
sign-in provider and redacts it in Debug. A refresh response may omit it; callers
own any decision to retain a prior provider credential in private storage.
Sign-in may use Google or another provider. Dactyl never infers an SCM provider
from that token. The account backend independently verifies a fresh linking
credential before granting SCM authority.

`CloudConnector::account_request` provides a separate stateless account channel.
Authentication completion confirms session handoff only. The caller persists
that session before account setup and requires the backend's explicit
`backend_authorized: true` before declaring cloud access ready. Registration and
status use server-issued project references; pricing and workspace capacities
remain backend-owned. A missing billing adapter returns `billing_not_configured`.

Its `AccountOperation` selects one fixed POST endpoint beneath
`/functions/v1/propodus-onboarding`: `Register` uses `account-register`, `Status`
uses `account-status`, `Quote` uses `billing-quote`, `Checkout` uses
`billing-checkout`, `Manage` uses `billing-manage`, and `Portal` uses
`billing-portal`. `AccountOperation::is_mutating` classifies `Register`,
`Checkout`, `Manage`, and `Portal` as mutating/session-creating and `Status`
and `Quote` as read-only calls. The JSON request and response remain opaque
Propodus policy. Ordinary registration, status and paid-seat access use the
current Supabase user bearer and `None` for the optional SCM credential. An
explicit SCM-linking call may pass `Some(&ScmCredential::github(token)?)` or the
corresponding `gitlab` constructor. Each validates an opaque header-safe token,
keeps it private and redacts Debug. `ScmProvider::{GitHub,GitLab}` labels
transport metadata; it does not establish authorization. The connector sends
`X-SCM-Token` together with `X-SCM-Provider: github` and `X-SCM-Host: github.com`,
or the fixed `gitlab` and `gitlab.com` pair. Caller-selected hosts are
unsupported.

These three headers are added only to the selected account request, never to
managed Auth, discovery or native data requests. Dactyl does not persist SCM
credentials. Callers decide consent and handling of an unknown checkout outcome;
Dactyl does not automatically replay account requests.

Auth, account and discovery HTTP requests are bounded to a 5-second connect timeout,
15-second request timeout, 16 KiB bodies, and `MAX_JSON_DEPTH = 64` JSON nesting.
A joined worker owns the complete blocking HTTP lifecycle so synchronous callers
can enter from an async runtime. No redirect or automatic request replay follows
an error. HTTP `409` maps to `AdapterErrorKind::Conflict`, and allowlisted public
denial codes are extracted across Propodus (`error.code`), Supabase GoTrue v2
(`error_code` when `code` is numeric), and OAuth2 (`error`) — including
`organization_seat_required`, `payment_pending`, `repository_not_authorized`,
`session_expired`, `session_revoked`, `session_not_found`, `bad_jwt`,
`refresh_token_already_used`, `custody_conflict`, and `billing_not_configured` —
while remote error messages and unknown codes are omitted. Post-send network or
response truncation failures on one-time auth exchanges, token rotations,
revocations, or mutating account operations attach `code = Some("outcome_unknown")`
so callers reconcile status instead of blindly replaying ambiguous mutations.
Credential-bearing types redact Debug, and server error bodies are not exposed in
diagnostics.

Person identity, tenant/project isolation, assigned-seat entitlement and
revocation are backend policy enforced by the Propodus-managed infrastructure.
Linked SCM identity is separate from code-host permission hierarchies. Dactyl
forwards opaque application context and credentials; neither a local agent label
nor a caller-supplied role creates backend authority. Privileged governance
operations remain separately controlled by the backend.

`tests/logical_connector.rs`, `tests/cloud_connector.rs`, and
`tests/supabase_disabled.rs` prove per-workspace `LogicalTarget` /
`LogicalConnector` isolation across concurrent local SQLite files and separate
loopback cloud origins without ambient environment variables, portable redaction
across feature combinations, discovery, account route and credential separation,
optional provider-token preservation, managed Auth requests, handoff validation,
redirect/replay/`outcome_unknown` behavior, allowlisted GoTrue/OAuth2/Propodus
error codes, and async-runtime safety using `local_sqlite` and `synthetic_http`
fixtures. `tests/supabase_rpc.rs` proves native request/result normalization.
Those tests are not `local_postgres`, `hosted_supabase` OAuth, or `live_propodus`
deployment evidence (`unavailable` until exercised in deployment). The profile
endpoint must be deployed before rolling out discovery-based clients.

The native RPC foundation was reconciled from source commit `24c549c` on the
prior `agent/codex-dactyl-rpc/code_01m71hjy2gceqhq0` branch. Its original worktree
and governance evidence remain untouched; this branch records fresh proof.
