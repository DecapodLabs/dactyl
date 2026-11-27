# Connector Guarantee Matrix & Released Artifact Qualification (`#158`)

Dactyl is a thin datastore and managed-authentication transport boundary. Valid TLS,
sender-constrained `DPoP` proofs, matching correlation identifiers, or server-emitted
provenance headers never independently establish domain authorization, SQL durability,
or Decapod/Propodus governance readiness.

This specification complements [cloud-failure-conformance.md](cloud-failure-conformance.md)
(`#143`), [cloud-connectors.md](cloud-connectors.md) (`#140`),
[wire-compatibility-matrix.md](wire-compatibility-matrix.md) (`#146`), and
`tests/operational_disposition_150.rs` (`#150`) without altering Dactyl's transport or
introducing independent domain policy.

---

## 1. Guarantee Matrix Across Execution Tiers

| Guarantee / Property | Local SQLite (`Backend::Local`) | Authenticated Cloud Transport (`ObservationSource::DirectResponse`) | Independently Reconciled Server Effect (`ObservationSource::ServerReadback` + Independent Observer) |
| --- | --- | --- | --- |
| **Network / TLS boundary** | Zero network sockets (`trust-and-egress-inventory.md`). File path opened directly via host SQLite C-ABI. | HTTPS-only for non-loopback endpoints; `3xx` redirects fail closed (`provenance_origin_mismatch`). Proves transport confidentiality/origin match to the selected endpoint, **not** backend authorization or durable SQL commit. | Re-verifies origin/auth scheme on a separate readback request; independent DB observer confirms backend persistence outside the original write response. |
| **Sender possession (`DPoP` / `Bearer`)** | Rejected if remote token or cloud provenance is supplied (`cloud_connector_required`, `LocalPreflight`). | `DPoP` proves caller key possession + `htm`/`htu`/`ath`/nonce binding for that request (at most one pre-execution `401 use_dpop_nonce` retry); **does not** prove RLS, seat entitlement, or transaction commit. | Reconcile readbacks require the same `auth_scheme` and same-origin `OperationAttempt` identity; auth mismatches fail closed (`provenance_auth_mismatch`). |
| **Response correlation (`request_id`, `attempt_id`, `request_digest`, `scope_ref`)** | N/A (`attempt_id` retained in `LocalPreflight` error context only). | Proves the server echoed matching correlation tokens for this `DirectResponse` envelope (`provenance_*_mismatch` on mismatch). **Does not** prove a row persisted if an authenticated server emitted an inaccurate `committed_observed` envelope or `accepted_not_committed`. | `reconcile_operation*` / `reconcile_control_attempt*` preserves `OperationAttempt` and converts payload digest drift into `AdapterErrorKind::IdempotencyConflict` (`idempotency_conflict`). |
| **Server-originated metadata (`EffectProvenance`, `CloudRecoveryFacts`)** | None (`EffectProvenance::local_preflight`). | Records what the endpoint *claimed* (`DirectResponse`) plus bounded recovery facts (`CloudRecoveryFacts`). Unadvertised responses preserve `EffectStatus::UnsupportedProvenance` (`contract_version = None`). | `ReconciliationObservation` preserves `prior_client_status` unchanged alongside `ObservationSource::ServerReadback`, distinguishing direct claims from separate readbacks (`not_attempted` / `absent` vs `committed_observed`). |
| **Mutation outcome & retry policy** | Local transaction atomicity (`MutationOutcome::RolledBack` on constraint/rollback, `NotAttempted` on preflight). | Direct HTTP `200` `accepted_not_committed`, post-send drops, timeouts, and truncated bodies fail closed as `MutationOutcome::Uncertain` -> `OperationalAction::Reconcile` (never blind retry, even when `is_retryable()` or `RecoverableWithinAuthority` appears post-send). | Only a separate readback or independent DB observer resolves `Uncertain`; no retry or fallback silently upgrades an uncertain or missing-provenance failure into `CommittedObserved`. |
| **Domain / governance authority** | Application owns schema and retries. | **None.** Decapod and Propodus remain the sole authorities over task/grant/governance state; neither must elevate transport receipts without required DB/service evidence. | Independent backend state query + Decapod/Propodus policy verification remain required for end-to-end hosted acceptance. |

---

## 2. Controlled Negative Fixtures (`tests/released_claim_qualification_158.rs`)

The controlled suite exercises cases not covered by transport-only happy-path tests:

1. **Local SQLite observer parity & elevation refusal (`sqlite_local_observer_verifies_commit_rollback_and_blocks_cloud_elevation`)**:
   - An independent `AccessMode::ReadOnly` connection verifies `0 -> 1 -> 1` row count across commit and constraint rollback (`MutationOutcome::RolledBack`, `OperationalAction::Stop`), while `write_with_attempt` / `reconcile_control_attempt` fail locally with `cloud_connector_required`.
2. **Protocol-conformant inaccurate authenticated backend claim (`inaccurate_authenticated_claim_remains_direct_response_not_observed_persistence`)**:
   - Loopback DPoP gateway returns HTTP `200` with `version=1`, matching `attempt_id`, `request_digest`, `scope_ref`, `decision=allowed`, and `effect_status=committed_observed` while persisting `0` rows.
   - `EffectProvenance` preserves `ObservationSource::DirectResponse` (never advertising independent persistence); separate `ServerReadback` surfaces `EffectStatus::NotAttempted` (`ObservationSource::ServerReadback`) while preserving `prior_client_status == Some(CommittedObserved)`.
3. **Valid correlation envelope without a committed effect (`valid_correlation_envelope_without_committed_effect_fails_closed`)**:
   - HTTP `200` with valid correlation metadata + `effect_status=accepted_not_committed` fails closed as `MutationOutcome::Uncertain` and `OperationalAction::Reconcile` on both `write_with_attempt` and `account_request_with_attempt`.
   - Direct HTTP `200` claiming `not_attempted` fails closed with `provenance_status_conflict` (`Uncertain` / `Reconcile`), while `ServerReadback` cleanly reports `NotAttempted`.
4. **Missing provenance & stale/mismatched evidence (`missing_provenance_and_stale_mismatched_evidence_never_silently_escalate`)**:
   - Missing provenance yields `UnsupportedProvenance` (`contract_version = None`), and post-send `503` without provenance stays `Uncertain` -> `Reconcile` even when `CloudRecoveryFacts` carries `RecoverableWithinAuthority` (`AfterSend`).
   - Stale/mismatched `request_digest` (`IdempotencyConflict`), `attempt_id` (`provenance_attempt_mismatch`), `origin` (`provenance_origin_mismatch`, `origin_bound = false`), and repeated `401 use_dpop_nonce` (`Authentication` after single bounded retry without falling back to `Bearer`) all stop cleanly without escalating to `CommittedObserved`.

---

## 3. Released Artifact vs. Accepted Source (`dactyl-db 0.21.0`)

Pinned against the published crates.io release and clean consumer qualification run:

- **Accepted Git source (`DecapodLabs/dactyl`)**: `0e7213a40d1a3106276f0a5c124bd59c57757053` (`tag: v0.21.0`).
- **Published crate archive**: `dactyl-db-0.21.0.crate` (`481,155` bytes, `sha256: 0b24815ef383ab731ccfa6e6b83e2dbb62ba3b444137b973683c17324d9a19d7`, source `registry+https://github.com/rust-lang/crates.io-index`).
- **VCS provenance inside crate**: `.cargo_vcs_info.json` records `{"git": {"sha1": "0e7213a40d1a3106276f0a5c124bd59c57757053"}, "path_in_vcs": ""}`.
- **Packaged artifact vs. source comparison**: Every shipped Rust source, test, and doc file in `dactyl-db-0.21.0.crate` matches `0e7213a40d1a3106276f0a5c124bd59c57757053` byte-for-byte; only `Cargo.toml` reflects Cargo's standard registry normalization (`build = false`, `autolib = false`, stripped `[dev-dependencies]`, original preserved in `Cargo.toml.orig`). Feature defaults remain `default = []`, with `sqlite`, `neon`, and `supabase` opt-in.
- **Clean registry consumer run (`cargo +1.85.0 test --offline`, `dactyl-db = "=0.21.0"`, `features = ["sqlite", "neon", "supabase"]`, zero `[patch]`/`path`)**:
  - `Cargo.lock` SHA-256: `7286ec02bdd029214acdda50e3a83a83737791199aa124a4001ef2232b59cd32`.
  - Exercised disposable real SQLite file (`0 -> 1 -> 1` verified by independent `AccessMode::ReadOnly` connection) and controlled loopback DPoP gateway (`inaccurate_claim -> DirectResponse`, `ServerReadback -> NotAttempted`, `accepted_not_committed -> Uncertain/Reconcile`, `lost_ack -> Reconcile` with `write_count == 1`): `1 passed; 0 failed` (`exit 0`).
- **Baseline `0.17.0` registry proof boundary**: `tools/proof/registry_consumer.py` remains pinned to `dactyl-db = "=0.17.0"` (`control_rpc`) for `#94` backward compatibility; the `#158` run above qualifies `0.21.0` directly from crates.io without modifying `check.py`'s `0.17.0` anchor.

---

## 4. Remaining Trust Assumptions & Non-Proven Scope Classification

- **Remaining endpoint trust assumptions**:
  - HTTPS endpoints still rely on system WebPKI root certificates (`webpki-roots`), DNS resolution for the pinned origin, and the authenticated backend's honesty for database effects not separately inspected via an independent DB observer.
  - Caller applications remain responsible for `RequestProofSigner` key custody, private `UserSession` persistence, avoiding automatic retry of `MutationOutcome::Uncertain` / `OperationalAction::Reconcile`, and independent verification of Decapod/Propodus domain preconditions.
- **Explicit proof-scope classification**:
  - **`local_sqlite`** (`tests/released_claim_qualification_158.rs`, `tests/operational_disposition_150.rs`, `tests/storage_fixtures.rs`): **passed** against host SQLite C-ABI.
  - **`synthetic_http`** (`tests/released_claim_qualification_158.rs`, `tests/effect_provenance.rs`, `tests/transport_fault_reconciliation_143.rs`, `tests/operational_disposition_150.rs`, `tests/cloud_connector.rs`, `tests/supabase_rpc.rs`): **passed** against controlled loopback TCP/HTTP fixtures.
  - **`registry_consumer_0_21_0`** (`dactyl-db = "=0.21.0"` clean consumer on Rust `1.85.0`): **passed** against disposable local SQLite + controlled loopback HTTP observer.
  - **`local_postgres`** (`tests/supabase_service.rs::local_postgres_service_contract`, `#[ignore]`): **unavailable** in CI/local offline runs without `DACTYL_SUPABASE_FIXTURE` (missing fixture explicitly fails with `"unavailable is not a pass"`).
  - **`local_native_service` & `protected_hosted_native_scenarios`** (`tests/native_service.rs`, `#[ignore]`): **unavailable** without private `DACTYL_NATIVE_FIXTURE` / `DACTYL_NATIVE_HOSTED_PROOF`.
  - **`hosted_supabase` / `live_propodus` (`Dactyl #135`)**: **unavailable / not proven here**; no live customer data or paid infrastructure was touched.
