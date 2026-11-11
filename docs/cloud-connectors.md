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

All auth/discovery HTTP requests are bounded to a 5-second connect timeout,
15-second request timeout and 16 KiB bodies. A joined worker owns the complete
blocking HTTP lifecycle so synchronous callers can enter from an async runtime.
No redirect or automatic request replay follows an error. A lost one-time
exchange or refresh response has an unknown outcome; the caller must not replay
it as though the server did nothing. Credential-bearing types redact Debug,
and server error bodies are not exposed in diagnostics.

GitHub identities, repository roles, membership, entitlement and revocation are
enforced by the Propodus-managed PostgreSQL functions and RLS. Dactyl forwards
opaque application context and credentials; neither a local agent label nor a
caller-supplied role creates backend authority.

`tests/cloud_connector.rs` proves discovery, secret separation, managed Auth
requests, redirect/replay behavior and async-runtime safety using local HTTP
fixtures. `tests/supabase_rpc.rs` proves native request/result normalization.
Those tests are not hosted OAuth, real PostgreSQL or deployment evidence. The
profile endpoint must be deployed before rolling out discovery-based clients.

The native RPC foundation was reconciled from source commit `24c549c` on the
prior `agent/codex-dactyl-rpc/code_01m71hjy2gceqhq0` branch. Its original worktree
and governance evidence remain untouched; this branch records fresh proof.
