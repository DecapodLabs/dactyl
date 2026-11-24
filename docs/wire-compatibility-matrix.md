# Wire & Resource-Budget Compatibility Matrix Across Release Skew

Issue [#146](https://github.com/DecapodLabs/dactyl/issues/146) defines the versioned wire, context, provenance, resource-budget, and origin-cutover compatibility rules across mixed releases of **Dactyl** (`dactyl-db 0.19.0`, MSRV Rust `1.85`), **Decapod** (latest published `v0.164.1`; `Cargo.toml` currently pinning `dactyl-db = "=0.16.1"` with `0.17.0` control-RPC registry consumer handoff), and **Propodus** (accepted Edge onboarding + PostgREST `dactyl_api` RPC deployment).

A passing offline/loopback fixture never implies live hosted execution. `local_postgres`, `hosted_supabase`, and `live_propodus` remain distinct external qualification profiles (`unavailable` until executed against live infrastructure).

---

## 1. Versioned Contract Inventory

| Contract Constant | Source | Current Version | Minimum Accepted Floor | Downgrade / Unknown Version Behavior |
| --- | --- | --- | --- | --- |
| `PROVENANCE_CONTRACT_VERSION` | `src/connector.rs` | `1` | `1` (or completely omitted on legacy unadvertised servers) | `0` or `>= 2` fails closed with `AdapterErrorKind::Protocol` (`unsupported_provenance_version`). |
| `STORAGE_CONTEXT_VERSION` | `src/contract.rs` | `1` | `1` (non-zero `u16`) | `0` fails closed with `AdapterErrorKind::Protocol` (`invalid_context`); Propodus additionally enforces `context.version == 1`. |
| `RESOURCE_BUDGET_VERSION` | `src/budget.rs` | `1` | `1` (strictly `== 1`) | `0` or `>= 2` fails closed with `DactylError::Config`. |
| `SUPABASE_RPC_CONTRACT_VERSION` | `src/supabase_rpc.rs` | `1` | `1` | Sent in `dactyl-contract-version: 1` header and `version: 1` request envelope; server rejects unsupported versions. |
| `DeploymentProfile.version` | `src/cloud.rs` | `1` | `1` (strictly `== 1`) | `0` or `>= 2` fails closed with `AdapterErrorKind::Protocol` without local SQLite fallback. |

---

## 2. Per-Envelope Compatibility & Skew Rules

### 2.1 `EffectProvenance` (`PROVENANCE_CONTRACT_VERSION = 1`)

Carried via `Decapod-Cloud-Provenance-Version` + `Decapod-Cloud-*` HTTP headers and/or JSON `"provenance": { ... }` body envelope (`src/http_pool.rs`):

- **Required fields (when provenance is advertised)**:
  - `version`: integer `1` (both header and JSON must agree if both are present; omitting `version` when other provenance fields are present fails with `invalid_provenance`).
  - `effect_status`: required enum string.
  - Correlation fields when caller passes `OperationAttempt`: `attempt_id` is required (`provenance_missing_attempt_id` / `provenance_attempt_mismatch`), `scope_ref` is required if caller supplied one (`provenance_scope_mismatch`), and `request_digest` must match when returned (`provenance_digest_mismatch` on direct response; `idempotency_conflict` on `ServerReadback`).
- **Additive extensions (supported forward-compatible)**:
  - Unknown JSON keys inside `"provenance": { ... }` are ignored.
  - Extra `Decapod-Cloud-*` diagnostic headers (bounded to `16` headers, `64`-byte names, `256`-byte values, `4096` total metadata bytes) plus `Sunset`, `Deprecation`, `Retry-After`, and `DPoP-Nonce` are captured in `CloudResponseMetadata`.
  - Unadvertised legacy responses (neither provenance headers other than diagnostic `Decapod-Cloud-Request-Id` nor JSON `"provenance"`) cleanly map to `EffectStatus::UnsupportedProvenance` without fabricating a receipt (`contract_version = None`).
- **Unknown enum refusal (fail closed -> `AdapterErrorKind::Protocol` / `invalid_provenance`)**:
  - `effect_status` accepts only: `committed_observed`, `refused`, `rolled_back`, `uncertain`, `accepted_not_committed`, `pending`, `in_progress`, `not_attempted`, `absent`.
  - `decision` accepts only: `allowed`, `accepted`, `refused`, `denied`.
  - `operation_kind` accepts only: `read`, `write`, `schema`.
  - `auth_scheme` accepts only: `bearer`, `dpop`, `none`.
  - `reason_code` accepts only allowlisted public codes in `allowlisted_cloud_code` (`src/http_pool.rs`).
- **Downgrade & conflict refusal**:
  - `version == 0` or `version >= 2` -> `unsupported_provenance_version`.
  - Header vs. JSON disagreement -> `provenance_conflict`.
  - Origin mismatch or 3xx redirect -> `provenance_origin_mismatch`.
  - Auth scheme mismatch -> `provenance_auth_mismatch`.
  - Direct 2xx claiming `not_attempted` or non-2xx claiming `committed_observed` or `committed_observed` + `refused` -> `provenance_status_conflict`.

### 2.2 `StorageContext` (`STORAGE_CONTEXT_VERSION = 1`)

Carried locally (ignored by SQLite execution) and serialized as `{"version": <u16>, "payload": <object>}` on Neon, Supabase service, and Supabase native RPC (`src/contract.rs`):

- **Required fields**: `version` (`u16`, must be `>= 1`), `payload` (must be JSON object).
- **Additive extensions**: Arbitrary caller-owned JSON keys inside `payload` are forwarded untouched; bounded by `ResourceBudget::max_context_bytes` (default `65,536` bytes -> `AdapterErrorKind::Quota` / `context_budget_exceeded`).
- **Downgrade / invalid refusal**: `version == 0` or non-object `payload` fails with `AdapterErrorKind::Protocol` (`invalid_context`).

### 2.3 `ResourceBudget` (`RESOURCE_BUDGET_VERSION = 1`)

In-process per-connection and per-operation/batch limit contract (`src/budget.rs`):

- **Compatibility**: `OpenOptions { access_mode, lock_timeout }` struct literals remain source-compatible; `Connection::open*` defaults to `ResourceBudget::v1()`, while `Connection::open_with_budget` and `LogicalConnector::with_http_budget` allow explicit tuning.
- **Required invariants (`ResourceBudget::validate`)**:
  - `version == 1` (downgrade `0` or unsupported `>= 2` fails with `DactylError::Config`).
  - `connect_timeout > 0`, `request_timeout > 0`, `http_idle_timeout > 0`.
  - `http_max_in_flight` in `1..=64`; `http_max_idle_per_host <= 64`.
- **Default v1 bounds**: `256 KiB` SQL, `64 KiB` context, `4 MiB` cell/param/wire, `256` columns, `100,000` rows, `64 MiB` decoded result bytes (`32x` BLOB JSON-array expansion charge), `1,024` atomic batch ops, `5s` connect / `15s` request / `15s` idle timeout, `MAX_JSON_DEPTH = 64`.
- **Origin & outcome separation**: Preflight rejections return `FailureOrigin::LocalBudget` + `MutationOutcome::NotAttempted`; remote policy denials return `FailureOrigin::RemotePolicy`; malformed/oversized wire responses return `FailureOrigin::MalformedResponse`; post-send mutating failures return `MutationOutcome::Uncertain` (never auto-retried).

### 2.4 Native RPC & Legacy Datastore Wire Envelopes (`SUPABASE_RPC_CONTRACT_VERSION = 1`)

| Carrier | Request Envelope | Required Response Fields | Additive Extensions | Incompatible / Skew Refusal |
| --- | --- | --- | --- | --- |
| Native RPC (`dactyl_query_v1`, `dactyl_batch_v1`, `dactyl_read_batch_v1`) | Header `dactyl-contract-version: 1` + `{"request": {"version": 1, ...}}` | `columns`, `rows`, `affected_rows`, `generated_keys` (required by `validate_rpc_result`), or `{"results": [...]}` | Unknown extra fields on `Response` / `BatchResponse` ignored | Missing `generated_keys` -> `Protocol`; `results.len() != ops.len()` -> `Protocol`; duplicate/missing row columns -> `Protocol`; object/non-byte-array/`u64 > i64::MAX`/non-finite cell -> `Capability` (`unsupported_type`). |
| Edge Gateway (`cloud-query`, `cloud-batch`, `cloud-control`) | `{"version": 1, ...}` + mandatory `DPoP` proof (`device_proof_required` if absent) | Same as native RPC for query/batch; opaque JSON for `cloud-control` | Same as native RPC | Missing DPoP proof fails locally (`NotAttempted`); 404 on `decapod-onboarding/cloud-control` falls back once to `propodus-onboarding/cloud-control`. |
| Legacy Service HTTP (`/query`, `/batch`) | Unversioned `{"sql": ..., ...}` / `{"operations": ..., ...}` | `columns`, `rows`, `affected_rows` (`generated_keys` defaults to `[]` if omitted) | Extra response fields ignored | Same cell/row/count validation; `Parameter` deserialization rejects `u64 > i64::MAX`. |

### 2.5 Discovery (`DeploymentProfile`) & Managed Auth / Token Envelopes

- **Discovery (`GET /functions/v1/decapod-onboarding/config`, single `404` fallback to `/functions/v1/propodus-onboarding/config`)**:
  - *Required fields*: `version` (`== 1`), `project_origin`, `publishable_key` (`sb_publishable_*`), `schema` (valid SQL identifier `1..=63`).
  - *Strict schema (`#[serde(deny_unknown_fields)]`)*: Unknown fields in discovery JSON are **rejected** as `AdapterErrorKind::Protocol` in v1; adding discovery fields requires a coordinated v2 profile revision.
  - *Stale / Downgrade / Redirect refusal*: `version == 0` or `version >= 2`, `project_origin` mismatch (e.g., stale cache/config returning `DEFAULT_CLOUD_ORIGIN` when resolving `FIRST_PARTY_CLOUD_ORIGIN`), 3xx redirect, or body `> 16 KiB` fail closed with `Protocol` and never fall back to SQLite.
- **Auth Start / Status / Token Envelopes**:
  - `AuthenticationStart`: requires UUID `flow_id`, origin-bound `browser_url` under `/{decapod,propodus}-onboarding/begin?flow_id=...&ticket=...`, `expires_at`; additive fields ignored.
  - `status`: strict enum (`pending | ready | consumed | complete`); unknown enum value fails with `Protocol`.
  - `TokenResponse`: requires `access_token`, `refresh_token`, `expires_in` (`1..=86400`); optional `provider_token`; additive GoTrue fields (e.g. `token_type`, `user`) ignored.
  - `AccountOperation`: `requires_human_bearer()` rejects `dpc_at_` device tokens locally (`authentication_required`) on all operations except `DeviceRefresh` and `DeviceRevoke`.

---

## 3. Sync vs. Async Connector & `api.decapodlabs.ai` (`#138`) Compatibility

- **Sync / Async parity**: `LogicalConnector`, `CloudConnector`, and `Connection` expose synchronous and `*_async` methods backed by `SharedHttpClient` (`src/http_pool.rs`). Both paths enforce identical discovery validation, `ResourceBudget` bounds, `RequestProofSigner` DPoP `htu` calculation, single pre-execution `401 use_dpop_nonce` retry, `EffectProvenance` verification, and `MutationOutcome::Uncertain` preservation.
- **First-party origin cutover (`FIRST_PARTY_CLOUD_ORIGIN = "https://api.decapodlabs.ai"`, `#138`)**:
  - `DEFAULT_CLOUD_ORIGIN` (`https://goyxpcmxetztjlrrcwew.supabase.co`) remains the default until live DNS/TLS, OAuth callback cutover, and server-advertised `project_origin` are independently verified.
  - Explicit opt-in via `LogicalTarget::first_party_cloud()` or `LogicalConnector::first_party_cloud[_async]()` binds:
    - `canonical_auth_url()` -> `https://api.decapodlabs.ai/auth`
    - `canonical_repository_url(org, repo)` -> `https://api.decapodlabs.ai/decapod/<org>/<repo>` (traversal/query/fragment segments rejected)
    - `RequestProofInput::canonical_url()` (`htu`) -> exact selected origin + route path with query/fragment stripped (`https://api.decapodlabs.ai/functions/v1/decapod-onboarding/...` or `/rest/v1/rpc/...`), never rewriting `htu` to `DEFAULT_CLOUD_ORIGIN`.
  - No silent legacy authority: stale discovery returning `DEFAULT_CLOUD_ORIGIN` on `FIRST_PARTY_CLOUD_ORIGIN` fails closed (`Protocol`), and `dpop_aud_mismatch` (`401`) propagates as `AdapterErrorKind::Authentication` without falling back to `Bearer` or SQLite.

---

## 4. Recorded Skew Fixture Coverage

`tests/wire_compatibility.rs` and recorded fixtures under `tests/fixtures/wire_compat/` deterministically verify:
- Supported additive extensions across RPC responses, legacy `/query` envelopes, provenance headers/JSON, GoTrue tokens, `StorageContext`, and `ResourceBudget::v1`.
- Refusal of version downgrades (`version: 0`), unsupported future versions (`version: 2`), unknown enums (`effect_status`, `decision`, `operation_kind`, `auth_scheme`, `reason_code`, auth `status`), `DeploymentProfile` extra fields, stale cross-origin discovery, `u64 > i64::MAX` integer overflow on `Parameter` / row cells, missing RPC `generated_keys`, and `api.decapodlabs.ai` DPoP `htu` / gateway parity across both sync and async paths.
