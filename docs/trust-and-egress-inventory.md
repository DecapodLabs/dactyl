# Dactyl Trust & Zero-Egress Inventory (`#1944`)

This document is the authoritative security, trust-boundary, and outbound-network inventory for `dactyl-db`. It is verified by automated tests in `tests/trust_and_egress.rs`, which assert that every transport and environment-inspection callsite in `src/` is documented here, that local `sqlite` mode performs zero network egress, and that remote HTTP routes fail closed on untrusted origins, non-HTTPS endpoints, or redirects.

---

## 1. Local SQLite Zero-Egress Guarantee

When configured with `Backend::Local`, `Datastore::Sqlite`, `LogicalTarget::local(path)`, `LogicalConnector::local(path)`, `DatastoreRoute::sqlite(path)`, or `DATASTORE=sqlite`:

- **Transport mode**: `DatastoreRoute::transport()` returns `DatastoreTransport::Local`.
- **Zero network sockets or DNS**: `src/adapter/sqlite/mod.rs`, `src/adapter/sqlite/ffi.rs`, `src/adapter/sqlite/inspection.rs`, and `src/adapter/sqlite/recovery.rs` contain zero networking code (`reqwest`, `std::net`, `tokio`, `http_pool`, `cloud`, `supabase_rpc` are absent from the SQLite adapter tree).
- **Compile-time isolation**: Under `--no-default-features --features sqlite`, Cargo does not compile or link `reqwest`, `hyper-rustls`, `hyper-util`, `tokio`, `url`, `quinn-proto`, or `quinn-udp`.
- **No ambient cloud discovery or credential harvesting**:
  - Local SQLite never reads `SUPABASE_*`, `DATABASE_URL`, `POSTGRES_*`, `NEON_*`, or user home directory credential files (`~/.decapod/*`, `~/.config/*`).
  - `DatastoreRoute::for_backend(Backend::Local, target, token)` and `LogicalConnector::route` / `LogicalConnector::open` reject any supplied `token` with `DactylError::Config` before opening a database.
  - `LogicalConnector::local` rejects cloud auth, onboarding, control RPC, and reconciliation methods locally with `AdapterErrorKind::Capability` (`cloud_connector_required`) without touching the network.
  - `Connection::open_with_context` ignores `StorageContext` for SQLite operations; no tenant/context fields trigger remote calls.
- **No silent fallback**: Failed `Backend::Cloud` discovery, missing `supabase` / `neon` features, or unreachable remote routes fail closed and never silently degrade to local SQLite. Conversely, local SQLite never falls back to a remote endpoint.

---

## 2. Optional Remote Backends (`neon`, `supabase`)

Remote HTTP transport is strictly opt-in via Cargo features (`neon`, `supabase`; `default = []`) and explicit caller selection (`DatastoreRoute::neon`, `DatastoreRoute::supabase`, `DatastoreRoute::supabase_rpc`, `LogicalTarget::cloud*`, or `DATASTORE=neon|supabase`):

| Backend / Channel | Feature Gate | Entry Points | Endpoints / Paths | Auth Material |
| --- | --- | --- | --- | --- |
| **Neon SQL-over-HTTP** | `neon` | `DatastoreRoute::neon`, `NeonAdapter` (`src/adapter/neon/mod.rs`) | `<origin>/query`, `<origin>/batch` | Optional caller `Bearer` token + mandatory `StorageContext` |
| **Supabase Service HTTP** | `supabase` | `DatastoreRoute::supabase`, `SupabaseAdapter` (`src/adapter/supabase.rs`) | `<origin>/query`, `<origin>/batch` | Mandatory caller `Bearer` (or `DPoP`) token + `StorageContext` |
| **Supabase Native RPC** | `supabase` | `DatastoreRoute::supabase_rpc`, `SupabaseAdapter` (`src/adapter/supabase.rs`) | `/rest/v1/rpc/{dactyl_query_v1,dactyl_batch_v1,dactyl_read_batch_v1}` or Edge gateway `/functions/v1/decapod-onboarding/{cloud-query,cloud-batch}` | Mandatory `apikey` (publishable key only) + caller `Bearer` / `DPoP` + `StorageContext` |
| **Decapod Cloud Discovery / Auth / Onboarding / Control** | `supabase` | `LogicalConnector::cloud*`, `CloudConnector` (`src/cloud.rs`) | `/functions/v1/decapod-onboarding/*` (with bounded 404 fallback to `/functions/v1/propodus-onboarding/*`), `/auth/v1/token`, `/auth/v1/logout`, `/rest/v1/rpc/<fn>`, `/functions/v1/decapod-onboarding/cloud-control` | Discovery sends no user token or key; Auth/Account/Control send only explicit caller credentials (`Bearer` / `DPoP` / optional `ScmCredential`) |

---

## 3. Allowed Origins & Endpoint Validation

- **Built-in origins (`src/connector.rs`)**:
  - `DEFAULT_CLOUD_ORIGIN`: `https://goyxpcmxetztjlrrcwew.supabase.co`
  - `FIRST_PARTY_CLOUD_ORIGIN`: `https://api.decapodlabs.ai` (opt-in via `LogicalTarget::first_party_cloud()` / `LogicalConnector::first_party_cloud()`)
- **Pre-dispatch URL validation (`src/http_pool.rs::validate_endpoint`, `src/adapter/supabase.rs::validate_rpc`, `src/cloud.rs::validate_origin`, `src/lib.rs::DatastoreRoute::validate`)**:
  - **HTTPS-only**: Non-loopback endpoints MUST use `https://`. Plain `http://` is permitted **only** when the host parses as a literal loopback IP address (`IpAddr::is_loopback()`, e.g., `127.0.0.1` or `[::1]`) for offline test fixtures. DNS hostnames such as `http://localhost` or suffix tricks such as `http://127.0.0.1.evil.test` are rejected without DNS resolution.
  - **No embedded credentials, queries, or fragments**: URLs containing `user:pass@`, `?query`, or `#fragment` are rejected fail-closed (`DactylError::Config`) before any socket is created.
  - **Origin & key pinning (`validate_rpc`, `validate_origin`)**: Native RPC and Cloud origins require a root path (`/`), a `sb_publishable_*` publishable key (never secret or service-role keys), and a valid SQL schema identifier.
  - **Discovery profile pinning (`CloudConnector::discover_with_metadata*`)**: Rejects `DeploymentProfile` payloads where `version != 1` or `profile.project_origin` does not match the selected origin.
  - **Browser handoff validation (`CloudConnector::validate_start`)**: Rejects `browser_url` values whose origin differs from the selected origin, whose path is not `/functions/v1/decapod-onboarding/begin` or `/functions/v1/propodus-onboarding/begin`, or that carry userinfo/fragments or invalid `flow_id`/`ticket` parameters.
  - **Canonical route helpers (`canonical_auth_url`, `canonical_repository_url`)**: Pin `/auth` and `/decapod/<org>/<repo>` strictly to the verified origin and reject traversal (`..`), slashes, or non-identifier characters via `validate_canonical_path_segment`.

---

## 4. Fail-Closed Redirect & Origin Checks

- **Redirects disabled at HTTP client layer (`src/http_pool.rs`)**: Both `SharedHttpClient::blocking_client` and `SharedHttpClient::async_client` configure `reqwest::redirect::Policy::none()` and `reqwest::retry::never()`.
- **Response-level origin & status guards**:
  - `src/cloud.rs::handle_cloud_response`, `src/adapter/supabase.rs::handle_supabase_response`, `src/http_pool.rs::bind_response_provenance`, and `src/adapter/neon/mod.rs` (`verify_response_origin`) explicitly reject all `300..400` redirect statuses and any response where `response.url().origin() != expected_origin.origin()` with `AdapterErrorKind::Protocol` (`provenance_origin_mismatch`).
  - Mutating calls encountering a redirect or origin mismatch mark `MutationOutcome::Uncertain` / `EffectStatus::Uncertain` without following the `Location` header or forwarding `Authorization` / `DPoP` / `apikey` / `X-SCM-Token` headers.

---

## 5. Credential Isolation, No Fingerprinting & Privacy Guarantees

- **Zero durable credential storage**: Dactyl stores no token files, SQLite credential caches, or OS keystore entries. `AuthenticationSecrets` and `UserSession` custody belongs strictly to the caller.
- **Caller-owned key custody (`RequestProofSigner`)**: Dactyl never generates asymmetric keys or accesses OS keychains. `RequestProofSigner` receives `(method, canonical_url, access_token_sha256, nonce)` synchronously and returns a short-lived `RequestProof`.
- **Debug & error redaction**: `DatastoreRoute`, `SupabaseRpcConfig`, `AuthenticationSecrets`, `AuthenticationStart`, `UserSession`, `ScmCredential`, `LogicalTarget`, `LogicalConnector`, `CloudConnector`, `RequestProofInput`, `RequestProof`, `OperationAttempt`, `EffectProvenance`, `ReconciliationObservation`, and `CloudResponseMetadata` redact secrets, tokens, nonces, origins, digests, and payloads in `Debug`. HTTP request headers containing secrets call `HeaderValue::set_sensitive(true)`. Remote error messages and non-allowlisted error codes are stripped (`allowlisted_cloud_code`).
- **No identity fingerprinting or telemetry**: Dactyl sends no machine IDs, hostnames, MAC addresses, OS usernames, filesystem paths, git repository URLs, or telemetry/analytics beacons.
- **SCM credential isolation**: `ScmCredential` (`X-SCM-Provider`, `X-SCM-Host`, `X-SCM-Token`) is restricted to fixed `github.com` / `gitlab.com` labels and sent solely on explicit `CloudConnector::account_request*` calls, never on auth, discovery, or data calls.
- **Inactive HTTP/3 dependencies**: `quinn-proto` (`>=0.11.19, <0.12`) and `quinn-udp` (`>=0.5.16, <0.6`) are constrained under `[target.'cfg(any())'.dependencies]` solely so Cargo's resolver selects patched manifests for `reqwest`'s inactive HTTP/3 graph; HTTP/3 is never compiled or enabled.

---

## 6. Exhaustive Transport & Environment Callsite Inventory

Every file in `src/` that touches network transport (`reqwest` / `SharedHttpClient`), host FFI (`libloading`), or process environment variables (`std::env::var`) is accounted for below:

| Source File | Symbols / Callsites | Purpose & Boundary |
| --- | --- | --- |
| `src/lib.rs` | `DatastoreRoute::from_env`, `DatastoreRoute::validate`, `DatastoreRoute::for_backend`, `build_adapter` | Reads `DATASTORE`, `DATASTORE_ROUTE`, and (for `neon`/`supabase` only) `DATASTORE_TOKEN`; validates Neon/Supabase endpoints before adapter construction; enforces `Backend::Local` credential rejection. |
| `src/connector.rs` | `DEFAULT_CLOUD_ORIGIN`, `FIRST_PARTY_CLOUD_ORIGIN`, `LogicalTarget`, `LogicalConnector`, `RequestProofSigner`, `ScmCredential` | Per-workspace target selection without ambient env reads; delegates Cloud HTTP exclusively when `feature = "supabase"` and `Backend::Cloud` are selected. |
| `src/http_pool.rs` | `SharedHttpClient::{blocking_client, async_client}`, `validate_endpoint`, `canonical_url`, `read_bounded_body_async`, `with_dpop_nonce_retry*`, `bind_response_provenance` | Sole constructor of `reqwest::blocking::Client` and `reqwest::Client` (`Policy::none()`, `retry::never()`, bounded pool limits); enforces HTTPS/loopback-only URL policy and origin/auth provenance binding. |
| `src/cloud.rs` | `CloudConnector::{default_origin, from_runtime, discover_with_metadata*, account_request*, control_request*, reconcile_*}`, `send_auth_once*`, `handle_cloud_response`, `validate_origin`, `validate_start` | Reads `DACTYL_CLOUD_ORIGIN` / `DACTYL_CLOUD_PUBLISHABLE_KEY` only in explicit runtime helpers; dispatches bounded JSON HTTP calls to the verified origin only; rejects redirects and cross-origin profiles/handoffs. |
| `src/adapter/neon/mod.rs` | `NeonAdapter::{new_with_options_and_budget, send, send_async}`, `send_neon_request`, `send_neon_request_async` | Validates endpoint via `validate_endpoint`, enforces `StorageContext` and bounded bodies, and rejects `3xx`/cross-origin responses via `verify_response_origin`. |
| `src/adapter/supabase.rs` | `SupabaseAdapter::new`, `validate_endpoint`, `validate_rpc`, `prepare_control_request`, `send_once*`, `handle_supabase_response`, `control_request_with_auth_and_attempt*` | Validates HTTPS/loopback endpoints, publishable keys, schemas, and `DPoP` preflight requirements; enforces `3xx`/origin checks and bounded JSON decoding. |
| `src/adapter/sqlite/ffi.rs` | `Api::load` | Reads optional `DACTYL_SQLITE_LIBRARY` to load the host SQLite shared library via `libloading`; performs zero network I/O. |
| `src/adapter/sqlite/mod.rs` | `SqliteAdapter`, `open_database`, `backup_database`, `recover_database` | Local-only SQLite filesystem and C-ABI operations; zero network sockets or remote endpoints. |
| `src/adapter/sqlite/inspection.rs` | `inspect_snapshot` | Local-only read-only SQLite diagnostic snapshot; zero network I/O. |
| `src/adapter/sqlite/recovery.rs` | `recover` | Local-only Linux/Android dump-reload recovery; zero network I/O. |
| `src/adapter/sqlite/recovery/tests.rs` | `#[cfg(test)]` child-process test harness | Test-only environment variables (`DACTYL_RECOVERY_UMASK_CHILD`, `DACTYL_RECOVERY_INTERRUPT_ROOT`, `DACTYL_RECOVERY_INTERRUPT_PHASE`) for local SQLite crash/umask recovery simulation; zero network I/O. |
