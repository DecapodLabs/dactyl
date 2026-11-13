# Intent

## Existing Supabase provider qualification (issue 91)

Dactyl provides a small generic Rust datastore and managed-auth transport over SQLite, explicit Neon/Supabase HTTP services and native Supabase RPC. The third provider is implemented in accepted source; issue 91 now aggregates remaining qualification rather than recreating it. docs/supabase-service.md separates the compatibility /query and /batch route from logical Backend::Cloud discovery and native RPC. Propodus is the private SQL/Auth/onboarding infrastructure boundary; no generic Go task server is required. Decapod owns local/cloud UX and durable private user-session custody. Issues 70, 79, 85 and 94 respectively own hosted execution, capability inventory, adversarial proof and registry handoff. Published source and synthetic tests never imply managed OAuth, pooler-mode or deployment acceptance.

## Resource budgets, streaming reads, and catalog predicates (issue 101)

Dactyl measures its repository-local dogfooding database (`docs/datastore/dogfooding-inventory.json`) and bounds per-operation and per-batch resource growth across SQLite, Neon, and Supabase through an additive, versioned `ResourceBudget` (`RESOURCE_BUDGET_VERSION = 1` in `src/budget.rs`), `Connection::open_with_budget` / `Connection::visit_rows`, single-statement local catalog predicates (`Connection::has_table`, `Connection::has_column`), and explicit `FailureOrigin` / `MutationOutcome` classification (`src/error.rs`). Existing `OpenOptions { access_mode, lock_timeout }` struct literals, `Rows` / `Row` callers, `SnapshotLimits`, `DactylError` / `AdapterErrorKind` variants, and Neon / Supabase wire envelopes remain source- and wire-compatible. Dactyl continues to own only physical execution and local maintenance; schema meaning, content-addressed component tables, retry/idempotency policy, and cross-connection concurrency limits remain caller-owned.

## Supported consumers and dependency resolution

Dactyl keeps the same datastore and managed-session APIs while requiring Rust
and Cargo 1.85 in this next-0.x-minor proposal. The prior inactive Quinn upper
bound kept Cargo 1.75 resolution possible but prevented a consumer from selecting
the patched protocol package. `Cargo.toml` now requires quinn-proto >=0.11.19
without enabling HTTP/3. Both the repository and a fresh consumer lock must meet
that floor; the full existing feature, lint, package and MSRV proof remains
required through `tools/proof/check.py`. No package publication or hosted rollout
is established by this change.

## Bounded diagnostic snapshots

The `dactyl-db` crate provides consistent, bounded physical reads for Decapod's
native storage inventory. Callers retain semantic schema and history ownership.
Snapshot inspection requires a dedicated read-only handle; unsupported remote
adapters fail explicitly without introducing a second schema or maintenance path.

## Logical backend access and user sessions

Applications expose per-workspace local/cloud selection; Dactyl resolves SQLite
or the native Supabase connector through `Backend`, `LogicalTarget`,
`LogicalConnector`, `DatastoreRoute::for_backend`, and `CloudConnector` in
`src/lib.rs`, `src/connector.rs`, and `src/cloud.rs` without ambient environment
variables or process-global connector state. Portable connector, session, and
account types (`LogicalTarget`, `LogicalConnector`, `AuthenticationSecrets`,
`AuthenticationStart`, `UserSession`, `AccountOperation`, `ScmProvider`,
`ScmCredential`, `DEFAULT_CLOUD_ORIGIN`) compile across all Cargo feature
combinations so consumers never re-gate session/account models. Propodus names
the IaC for the hosted backend. Dactyl discovers its public deployment profile,
fails closed without SQLite fallback when discovery or compilation fails, and
transports managed Auth requests, account readiness/billing requests
(`Register`, `Status`, `Quote`, `Checkout`, `Manage`, `Portal`), and
current-user RPC credentials. The application retains browser interaction,
durable pending-flow state and private session custody. Ordinary cloud authority
is the authenticated Supabase user and active assigned seat within the
tenant/project. Linked code-host identities and separately scoped governance
grants remain backend policy; repository permissions are not mirrored as
ordinary data authority. Post-send transport or truncation failures on mutating
or one-time requests surface `outcome_unknown` so callers never blindly replay
ambiguous writes. Offline connector/auth fixtures (`tests/logical_connector.rs`,
`tests/cloud_connector.rs`) establish local SQLite and synthetic HTTP transport
behavior only; `local_postgres`, `hosted_supabase`, and `live_propodus` OAuth,
checkout, and deployed profile availability remain separate rollout evidence and
are recorded as `unavailable` when unexercised.

## Secure shared-directory SQLite recovery

Explicit SQLite recovery supports intentionally shared Unix storage without exposing rebuild contents. Dactyl owns fresh private same-filesystem staging; callers retain filesystem policy and must quiesce clients and filesystem writers. No automatic repair or consumer integration is added.

## Product Outcome
- Establish Dactyl as the small application-layer database provider for read/write-heavy apps, targeted for the v0.7.0 release. The same normalized reads, explicit writes, opaque atomic batches, and access modes must work against a real local SQLite file and remote Vercel Neon without exposing backend handles or adding database administration behavior.

## What This Project Is
dactyl-db is a Rust application driver over a real local SQLite file and remote Vercel Neon. Dactyl selects the physical route, binds application values, delegates SQL execution and file durability to SQLite, forwards remote requests with a versioned opaque storage context, and normalizes rows, write results, atomic results, schema projections, and typed failures. Database administration, schema ownership, migration order, query planning, analytics, retries, idempotency, tenancy, authorization, and business intelligence remain outside the crate.

Issue #88 adds one deliberately narrow physical-storage responsibility: Dactyl
defines explicit verification, online backup, and logical dump/reload recovery
for local SQLite. It does not auto-repair on startup or expose raw SQLite or
Decapod-specific recovery behavior. Operators select recovery explicitly,
preserve the original file and sidecars, and receive typed, auditable results.

Key operating facts:
- **Primary languages**: Rust
- **Detected surfaces**: not detected yet

## Product View
```mermaid
flowchart LR
  U[Primary User] --> P[dactyl-db]
  P --> O[User-visible Outcome]
  P --> G[Proof Gates]
  G --> E[Evidence Artifacts]
```

## Inferred Baseline
- Repository: dactyl-db
- Product type: not classified yet
- Primary languages: Rust
- Detected surfaces: not detected yet

## Scope
| Area | In Scope | Proof Surface |
|---|---|---|
| Core workflow | Define a concrete user-visible workflow | Acceptance criteria + tests |
| Data contracts | Document canonical inputs/outputs | [INTERFACES.md](./INTERFACES.md) and schema checks |
| Delivery quality | Block promotion on broken proof surfaces | [VALIDATION.md](./VALIDATION.md) blocking gates |

## Non-Goals (Falsifiable)
| Non-goal | How to falsify |
|---|---|
| Feature creep beyond the primary outcome | Any PR adds capability not tied to outcome criteria |
| Shipping without evidence | Missing validation artifacts for promoted changes |
| Ambiguous ownership boundaries | Missing owner/system-of-record in interfaces |

## Constraints
- Technical: runtime, dependency, and topology boundaries are explicit.
- Operational: deployment, rollback, and incident ownership are defined.
- Security/compliance: sensitive data handling and authz are mandatory.
- Context boundary: Decapod owns the meaning of `StorageContext`; Propodus owns
  membership and repository authorization; Dactyl validates only the envelope
  and forwards it to Neon.

## Acceptance Criteria (must be objectively testable)
- [x] Local SQLite proves the backend-neutral fixture matrix in `tests/storage_fixtures.rs`: parameterized reads/writes, explicit keys, conditional CAS/zero-row updates, atomic state-plus-event commit and rollback, read-only rejection, typed constraint errors, concurrent scoped writes, deterministic `DROP` cleanup, and storage-context no-op when cloud tenancy fields are present or absent.
- [x] The same fixture cases run through the Neon adapter against an executing in-process mock; live Propodus/Vercel Neon is recorded as `unavailable` unless `DACTYL_LIVE_PROPODUS_ROUTE` is set, and a skipped live backend is never reported as passed.
- [x] Remote query and atomic requests preserve the versioned opaque context; local operations remain context-neutral; missing remote context fails closed with typed authentication/protocol errors; remote authorization denials surface as `AdapterErrorKind::Authorization`.
- [x] The local route opens real SQLite files through the optional `sqlite` feature. Existing files open unchanged; read/write creation, read-only access, SQLite locking/journaling, typed errors, and the backend-neutral schema projection are covered by tests.
- [x] Issue #77's revised connector requirement is proved by `tests/sqlite_existing.rs`: the checked-in Decapod fixture opens without conversion, preserves catalog and values, accepts updates, reopens successfully, and covers NULL/REAL/blob/generated-key and missing/read-only behavior.
- [x] Issue #88's local maintenance contract is proved by `tests/sqlite_maintenance.rs`: healthy verification, WAL/SHM-aware online backup, damaged-index detection where `REINDEX` fails, verified logical recovery, metadata/data preservation, DELETE journal-mode behavior, atomic archive conflict handling, same-process open-connection refusal, and bounded lock errors.
- [x] The additive maintenance methods remain local-only; the Neon contract returns typed capability errors without network calls.
- [ ] Live Propodus/Vercel Neon deployment, route translation, and provider CAS/`version_conflict` proof remain a follow-up issue.
- [ ] Non-functional targets are met (latency, reliability, cost, etc.).
- [ ] Validation gates pass and artifacts are attached.
- [ ] `cargo test` passes for unit/integration coverage
- [ ] `cargo clippy -- -D warnings` passes with no denied lints
- [ ] `cargo fmt --check` passes on the repo

## Epistemic Custody Fields

### Active Assumptions
- [ ] List any assumptions made to proceed.
- [ ] Flag assumptions that require future verification.

### Confidence & Risk Level
- **Confidence**: Low/Medium/High (Rationale: )
- **Risk**: Low/Medium/High (Impact of wrong assumptions: )

### Measured vs Inferred Facts
| Fact | Source (Provenance) | Type (Measured/Inferred) |
|---|---|---|
| | | |

### Unresolved Contradictions
- [ ] List any evidence that conflicts with current assumptions or intent.

### Deferred Questions
- [ ] Questions to be answered later.

### Stop Conditions
- [ ] Explicit conditions under which the agent should stop and ask for help.

### Proof Required Before Completion
- [ ] Specific evidence needed to prove the outcome is met.

## Tradeoffs Register
| Decision | Benefit | Cost | Review Trigger |
|---|---|---|---|
| Simplicity vs extensibility | Faster iteration | Potential rework | Feature set expands |
| Strict gates vs dev speed | Higher confidence | More upfront discipline | Lead time regressions |

## First Implementation Slice
- [x] Define the smallest user-visible workflow: one parameterized application read and one parameterized application write.
- [x] Define the shared route, parameter, row, affected-count, and error contracts.
- [x] Postpone schema administration, migrations, transactions, query analysis, retries, and business intelligence.

## Open Questions (with decision deadlines)
| Question | Owner | Deadline | Decision |
|---|---|---|---|
| Which interfaces are versioned at launch? | TBD | YYYY-MM-DD | |
| Which non-functional target is hardest to hit? | TBD | YYYY-MM-DD | |

<!-- decapod:declared-capabilities:start -->

## Declared Capability Surfaces

These declarations select authoring and proof obligations; they do not attest implemented runtime guarantees.

- `authentication`
- `background-processing`
- `event-driven`
- `external-integrations`
- `infrastructure-management`
- `persistent-state`
- `public-api`
- `scheduled-jobs`
- `secrets-handling`

<!-- decapod:declared-capabilities:end -->

<!-- decapod:codebase-attestation:start -->

## Codebase Attestation

- Repository signal fingerprint: `a7a057f783efbf536549d413204a9e09cae9e963fb7a89f823cb25e6e7f56cf1`
- Significant implementation surfaces: `.github/` (4 files), `Cargo.lock/` (1 files), `Cargo.toml/` (1 files), `README.md/` (1 files), `examples/` (1 files), `src/` (18 files), `tests/` (23 files), `tools/` (2 files)
- Refreshed from the current codebase by `decapod rpc --op specs.refresh`
<!-- decapod:codebase-attestation:end -->

## Supabase provider boundary (issue 91)

Add an opt-in third provider selected at connection construction. Existing
SQLite and Neon configurations, features, defaults and operation contracts stay
compatible. Supabase clients use authenticated HTTPS with user bearer credentials
and the opaque versioned StorageContext. Database passwords and service-role
keys belong exclusively to a separately authorized service/operator boundary.
No native Postgres transport, deployment, data migration, or identity policy is
introduced by the client adapter.

## Native read-only batch boundary

The generic native connector selects a separate physically read-only atomic batch boundary. Identity issuance and effective authorization remain service-owned; local and legacy transports retain their existing contracts.
