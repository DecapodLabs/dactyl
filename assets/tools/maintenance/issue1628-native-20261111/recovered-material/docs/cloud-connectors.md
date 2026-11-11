# Logical backend and managed authentication

Applications select `Backend::Local` or `Backend::Cloud`. Dactyl resolves local
storage to SQLite and cloud storage to native Supabase RPC. The default cloud
origin is `DEFAULT_CLOUD_ORIGIN`; the Propodus IaC project provisions its schema,
RLS, managed Auth integration and onboarding function. Propodus is the name of
that infrastructure, not an additional application server in the request path.

`DatastoreRoute::for_backend` takes the logical selection, target and optional
current user bearer. A cloud route discovers version 1 of the public deployment
profile at `/functions/v1/propodus-onboarding/config`. Discovery sends neither
the bearer nor a project API key, follows no redirects, and rejects a profile
whose origin differs from the selected origin. Its publishable key and API
schema are validated before constructing the native RPC route. A secret key
cannot substitute for the publishable key. Existing explicit SQLite, Neon and
Supabase service constructors retain their contracts.

`CloudConnector::default_origin` returns the built-in production origin without
reading process environment. `discover(origin)` and `with_config(profile)` are
explicit per-instance seams for controlled test/private deployments; the latter
still validates the entire profile. `from_runtime(origin)` is a compatibility
alias for discovery, with no ambient key or origin overrides. One workspace
cannot change another workspace's connector through process-global routing.
The project-facing Decapod setting remains `backend = "local" | "cloud"`.

The same connector provides stateless authentication transport:

1. The caller generates `AuthenticationSecrets`, persists them privately and
   starts authentication for its opaque repository scope. The caller may also
   supply its existing private verifier and polling secret through validated
   `AuthenticationSecrets::from_parts`, preserving interrupted-flow custody.
2. The caller opens the validated browser handoff and polls status. Dactyl sends
   only PKCE/poll challenges on start; the browser never receives the verifier
   or polling secret. The handoff must use the selected origin, fixed begin path,
   and exactly one matching flow identifier plus one valid one-time ticket.
3. The caller checkpoints an in-flight marker before the one-time code exchange,
   saves the returned code, and checkpoints again before managed token exchange.
4. Supabase Auth issues `UserSession`. The caller saves it privately before
   authenticated onboarding completion. Dactyl retains no durable credentials.
   Access lifetime is positive and bounded at 24 hours; the local expiry uses
   request-start time and never exceeds a supplied earlier server expiry.
5. Subsequent native RPC carries the current access bearer. Refresh tokens go
   only to the same origin's managed Auth token endpoint. Session revocation
   uses managed logout. Neither Dactyl nor Propodus issues a parallel session.

All auth/discovery HTTP requests are bounded to a 5-second connect timeout,
15-second request timeout and 16 KiB bodies. A joined worker owns the complete
blocking HTTP lifecycle so synchronous callers can enter from an async runtime.
No redirect or automatic request replay follows an error. A lost one-time
exchange or refresh response has an unknown outcome; the caller must not replay
it as though the server did nothing. After a dispatched mutation, transport,
body-limit or response-decoding failures carry `outcome_unknown`. A known server
rejection retains an allowlisted code such as `entitlement_required`,
`authorization_denied`, `session_expired` or `session_revoked`; raw codes,
messages, details and hints never enter diagnostics. Credential-bearing types
redact Debug. Explicit caller cancellation before a request sends nothing;
once a request is dispatched, its bounded worker must finish or its outcome
must be treated as uncertain. Dactyl provides no background cancellation daemon.

`authentication_status` returns `AuthenticationStatus` with a typed state,
repository and RFC3339 expiry. `exchange_authentication_code` returns
`AuthenticationExchange` with the code and repository. Authenticated
`complete_authentication` accepts only the service's exact `complete` result
and returns its repository. The caller must compare those repository values and
expiry against its private origin/repository binding before advancing custody.
Browser `ready` means that a code exists, not that an account is authorized.
A printed URL, redirect, token issuance, or service `/readyz` result does not
establish account readiness. `entitlement_required` does not distinguish payment
from seat assignment; the client must not invent that distinction or an account
action URL. No pricing, role inference, or customer-policy engine lives here.

GitHub identities, repository roles, membership, entitlement and revocation are
enforced by the Propodus-managed PostgreSQL functions and RLS. Dactyl forwards
opaque application context and credentials; neither a local agent label nor a
caller-supplied role creates backend authority.

`tests/cloud_connector.rs` proves discovery, secret separation, managed Auth
requests, redirect/replay behavior and async-runtime safety using local HTTP
fixtures. `tests/supabase_rpc.rs` proves native request/result normalization.
Those tests are not hosted OAuth, real PostgreSQL or deployment evidence. The
profile endpoint must be deployed before rolling out discovery-based clients.

The native RPC and connector foundation was reconciled from source commit
`bef1a565819c67f5403dd24ed6a35163a3a788eb`, which incorporated the native RPC
work at `24c549cbd5390c6dba8735f9a43675c0cb624723`. Their original branches,
worktrees and governance evidence remain untouched. This task requires fresh
exact-candidate proof; earlier source or receipts do not establish readiness.
