# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0](https://github.com/DecapodLabs/dactyl/compare/v0.2.1...v0.3.0) (2026-08-12)


### Features

* add lightweight pure-Rust datastore primitives ([#51](https://github.com/DecapodLabs/dactyl/issues/51)-[#57](https://github.com/DecapodLabs/dactyl/issues/57)) ([#58](https://github.com/DecapodLabs/dactyl/issues/58)) ([7aedc77](https://github.com/DecapodLabs/dactyl/commit/7aedc77694cbdc4332b32d5d80c47017486e2a7c))
* complete local SQLite fixtures for [#57](https://github.com/DecapodLabs/dactyl/issues/57) and [#64](https://github.com/DecapodLabs/dactyl/issues/64) ([#69](https://github.com/DecapodLabs/dactyl/issues/69)) ([d966d49](https://github.com/DecapodLabs/dactyl/commit/d966d493ae6d9313bf06936e2824feabd63b8cca))
* complete pure-rust storage parity for Decapod ([#61](https://github.com/DecapodLabs/dactyl/issues/61)) ([e7941cf](https://github.com/DecapodLabs/dactyl/commit/e7941cff4fe652dd12963862dbb7b7b61670cac6))
* **dactyl:** complete atomic transaction/batch contract ([#24](https://github.com/DecapodLabs/dactyl/issues/24)) ([#43](https://github.com/DecapodLabs/dactyl/issues/43)) ([b7c9866](https://github.com/DecapodLabs/dactyl/commit/b7c986601a7ffdf40b69bcf881a9974966cc9194))
* **dactyl:** complete typed/NULL-safe named row projections ([#25](https://github.com/DecapodLabs/dactyl/issues/25)) ([#41](https://github.com/DecapodLabs/dactyl/issues/41)) ([d069fd8](https://github.com/DecapodLabs/dactyl/commit/d069fd8c98e9e088842879bd398fba42fe2fe4ff))
* forward opaque storage context for Issue [#64](https://github.com/DecapodLabs/dactyl/issues/64) ([#65](https://github.com/DecapodLabs/dactyl/issues/65)) ([4647ff3](https://github.com/DecapodLabs/dactyl/commit/4647ff3556749f6728eeff4392f77d6cf811d3f0))
* make dactyl a thin application read write driver ([#48](https://github.com/DecapodLabs/dactyl/issues/48)) ([7a6d825](https://github.com/DecapodLabs/dactyl/commit/7a6d82594e76e92e34dbfe6c4464d44444b4ba2e))
* prepare dactyl as Decapod datastore boundary ([#45](https://github.com/DecapodLabs/dactyl/issues/45)) ([6292f28](https://github.com/DecapodLabs/dactyl/commit/6292f28dd8170afb6b575d8a20889b8ebb9811af))


### Bug Fixes

* pin decapod version to 0.98.2 ([e9b4664](https://github.com/DecapodLabs/dactyl/commit/e9b4664e6dd295751e3a6e6803905958caffad6e))
* **release:** prevent recursive releases and tag collisions ([#36](https://github.com/DecapodLabs/dactyl/issues/36)) ([d3eb33f](https://github.com/DecapodLabs/dactyl/commit/d3eb33f6463ebe5e83713ae293812f34a95c08d6))
* **release:** synchronize facade and macros versions ([#38](https://github.com/DecapodLabs/dactyl/issues/38)) ([96e5126](https://github.com/DecapodLabs/dactyl/commit/96e5126750140e717f848ab574a8224e3d90fb71))

## [Unreleased]

## [0.7.0] (2026-08-12)

### Features
- add lightweight pure-Rust datastore primitives (#51-#57) (#58) (7aedc77)
- complete local SQLite fixtures for #57 and #64 (#69) (d966d49)
- complete pure-rust storage parity for Decapod (#61) (e7941cf)
- dactyl: complete atomic transaction/batch contract (#24) (#43) (b7c9866)
- dactyl: complete typed/NULL-safe named row projections (#25) (#41) (d069fd8)
- forward opaque storage context for Issue #64 (#65) (4647ff3)
- make dactyl a thin application read write driver (#48) (7a6d825)
- prepare dactyl as Decapod datastore boundary (#45) (6292f28)

### Bug Fixes
- pin decapod version to 0.98.2 (e9b4664)
- release: prevent recursive releases and tag collisions (#36) (d3eb33f)
- release: synchronize facade and macros versions (#38) (96e5126)

### Added

- complete the local SQLite half of the backend-neutral storage fixture
  matrix: conditional CAS/zero-row writes, state-plus-event atomic
  commit/rollback, concurrent scoped writes with deterministic cleanup,
  storage-context no-op including unused tenancy fields, and an executing
  Neon mock that runs the same cases without claiming live Propodus/Vercel
  Neon parity ([#57](https://github.com/DecapodLabs/dactyl/issues/57),
  [#64](https://github.com/DecapodLabs/dactyl/issues/64)).

### Fixed

- deserialize JSON blob parameters as byte arrays so Neon writes bind the
  same `Blob` values as local SQLite, and map remote constraint/contention
  codes into typed adapter errors.

- extend the pure-Rust local schema surface with caller-owned multi-statement
  DDL, defaults, composite constraints, foreign keys, structural indexes, and
  rollback/reopen upgrade proofs; map stable remote error codes into typed
  adapter outcomes without adding retries or Propodus policy
  ([#52](https://github.com/DecapodLabs/dactyl/issues/52),
  [#54](https://github.com/DecapodLabs/dactyl/issues/54),
  [#55](https://github.com/DecapodLabs/dactyl/issues/55),
  [#56](https://github.com/DecapodLabs/dactyl/issues/56),
  [#57](https://github.com/DecapodLabs/dactyl/issues/57)).
- implement the lightweight local datastore in Rust with no SQLite C binding,
  rusqlite dependency, SQLite subprocess, or Turso runtime dependency
  ([#51](https://github.com/DecapodLabs/dactyl/issues/51)).
- add caller-supplied atomic schema/data batches, explicit generated-key
  results, typed contention/read-only errors, and local durable reopen tests
  ([#52](https://github.com/DecapodLabs/dactyl/issues/52),
  [#53](https://github.com/DecapodLabs/dactyl/issues/53),
  [#54](https://github.com/DecapodLabs/dactyl/issues/54),
  [#55](https://github.com/DecapodLabs/dactyl/issues/55),
  [#56](https://github.com/DecapodLabs/dactyl/issues/56)).
- add the backend-neutral contract and deterministic local conformance
  fixtures used to validate the Neon-shaped boundary ([#57](https://github.com/DecapodLabs/dactyl/issues/57)).

## [0.3.0](https://github.com/DecapodLabs/dactyl/compare/dactyl-db-macros-v0.2.5...dactyl-db-macros-v0.3.0) - 2026-08-06

### Added

- prepare dactyl as Decapod datastore boundary ([#45](https://github.com/DecapodLabs/dactyl/pull/45))

### Added

- *(dactyl)* add the backend-neutral `Connection` and `StorageOp`/`StorageResult` integration boundary, including caller-owned SQL scripts, connection policy, last-insert-id support, blob parameters, and read-only SQLite configuration.
- *(dactyl)* enforce runtime dialect analysis and inline datastore directives; unsafe constructs fail closed and only explicitly bounded rewrites are available through `ConnectionOptions` or `DATASTORE_REWRITE`.

### Changed

- *(dactyl)* keep SQLite and Neon adapter implementations private so the public contract no longer exposes `rusqlite` or transport-specific types.

## [0.2.5](https://github.com/DecapodLabs/dactyl/compare/dactyl-db-macros-v0.2.4...dactyl-db-macros-v0.2.5) - 2026-08-02

### Added

- *(dactyl)* complete atomic transaction/batch contract ([#24](https://github.com/DecapodLabs/dactyl/pull/24)) ([#43](https://github.com/DecapodLabs/dactyl/pull/43))
- *(dactyl)* complete typed/NULL-safe named row projections ([#25](https://github.com/DecapodLabs/dactyl/pull/25)) ([#41](https://github.com/DecapodLabs/dactyl/pull/41))

### Other

- release v0.2.4 ([#42](https://github.com/DecapodLabs/dactyl/pull/42))

## [0.2.4](https://github.com/DecapodLabs/dactyl/compare/dactyl-db-macros-v0.2.3...dactyl-db-macros-v0.2.4) - 2026-08-02

### Added

- *(dactyl)* complete typed/NULL-safe named row projections ([#25](https://github.com/DecapodLabs/dactyl/pull/25)) ([#41](https://github.com/DecapodLabs/dactyl/pull/41))

### Added

- *(dactyl)* complete typed/NULL-safe named row projections for [#25](https://github.com/DecapodLabs/dactyl/issues/25): `try_get`, `is_null`, borrowed `get_str_ref` / `get_json_ref`, explicit first-match duplicate-alias semantics, NULL conversion messages, unit + SQLite/Neon matrix conformance (also DecapodLabs/decapod#1111, dactyl #2).
- *(dactyl)* complete atomic batch contract for [#24](https://github.com/DecapodLabs/dactyl/issues/24): Neon-mock failure-injection, event-plus-state fixture on both adapters, nesting/retry/timeout/idempotency docs; neon adapter surfaces non-2xx batch bodies without requiring a success-shaped decode.

### Documentation

- *(dactyl)* document the full `Row` projection contract (scalars, NULL, missing columns, aliases, ownership/lifetime) in the README and crate docs.
- *(dactyl)* document `transaction` atomicity, nesting, retry, timeout, and idempotency semantics in README and crate docs.

## [0.2.3](https://github.com/DecapodLabs/dactyl/compare/dactyl-db-macros-v0.2.1...dactyl-db-macros-v0.2.3) - 2026-08-01

### Fixed

- *(release)* synchronize facade and macros versions ([#38](https://github.com/DecapodLabs/dactyl/pull/38))

### Other

- *(release)* keep alignment check version agnostic ([#40](https://github.com/DecapodLabs/dactyl/pull/40))

## [0.2.2](https://github.com/DecapodLabs/dactyl/compare/dactyl-db-v0.2.1...dactyl-db-v0.2.2) - 2026-08-01

### Fixed

- *(release)* prevent recursive releases and tag collisions ([#36](https://github.com/DecapodLabs/dactyl/pull/36))

### Other

- release v0.2.1

## [0.2.1](https://github.com/DecapodLabs/dactyl/compare/v0.1.6...v0.2.1) - 2026-08-01

### Added

- *(dactyl)* slim ambient-env query/execute/transaction API, caller-owned schema, typed rows, conformance + injection regression tests

### Added

- *(dactyl)* safe parameterized execution: `Parameter` (Null/Bool/Integer/Real/Text), `query(sql, params)`, `execute(sql, params)`, and `transaction(&[Statement])`. References DecapodLabs/dactyl#23, #24.
- *(dactyl)* typed named-column projection: strict `Row::get<T>` plus lenient `get_bool`/`get_int`/`get_real`/`get_str`/`get_json` with explicit NULL, missing-column, and conversion-error semantics. References DecapodLabs/dactyl#25.
- *(dactyl)* caller-owned schema: dactyl never bootstraps tables; `execute` is the DDL/migration surface. References DecapodLabs/dactyl#27.
- *(dactyl)* ambient-env routing contract: `DATASTORE`, `DATASTORE_ROUTE`, `DATASTORE_TOKEN`; no `init()`, no `read`/`write` split, no `optimize` flag, no legacy `DACTYL_*` vars. References DecapodLabs/dactyl#26.

### Changed

- *(dactyl)* BREAKING: `read(query, params, optimize)` / `write(query, params, optimize)` replaced by a single `query(sql, params)` entry point plus `execute` and `transaction`. The process-global connection cache is removed; each call builds a short-lived adapter. Records the routing-contract decision from DecapodLabs/dactyl#26.

## [0.2.0](https://github.com/DecapodLabs/dactyl/compare/v0.1.6...v0.2.0) - 2026-08-01

### Added

- *(dactyl)* implement Parameter, transaction/batch, raw DDL execution, and named column row extraction

### Other

- release v0.1.6
- *(dactyl)* refresh specs footprint

## [0.1.4](https://github.com/DecapodLabs/dactyl/compare/v0.1.3...v0.1.4) - 2026-08-01

### Added

- *(dactyl)* simplify adapter selection to DATASTORE and DATASTORE_ROUTE. Reference DecapodLabs/dactyl#1.
- *(dactyl)* bootstrap crate with read/write facade, both adapters, query! macro, conformance harness

### Other

- release v0.1.3
- release v0.1.1
- *(dactyl)* bump dactyl_macros and dactyl package versions to 0.1.1 to fix crates.io bootstrap dependency
- *(dactyl)* configure release-plz for GitOps release management
- *(dactyl)* add crates.io publishing configuration and release scripts
- *(dactyl)* refresh specs fingerprint and update documentation for simplified adapter selection
- automated container updates
- *(dactyl)* read/write are the only public API; first call auto-bootstraps the adapter
- ignore .decapod runtime artifacts in .gitignore
- Initial commit

## [0.1.3](https://github.com/DecapodLabs/dactyl/compare/v0.1.2...v0.1.3) - 2026-08-01

### Other

- *(dactyl)* merge origin/main into chore/rename-to-dactyl-db and resolve version conflict to 0.1.3
- *(dactyl)* merge origin/main into chore/rename-to-dactyl-db and resolve conflicts
- *(dactyl)* rename package to dactyl-db and macros to dactyl-db-macros

## [0.1.2](https://github.com/DecapodLabs/dactyl/compare/v0.1.1...v0.1.2) - 2026-07-31

### Added

- *(dactyl)* simplify adapter selection to DATASTORE and DATASTORE_ROUTE. Reference DecapodLabs/dactyl#1.
- *(dactyl)* bootstrap crate with read/write facade, both adapters, query! macro, conformance harness

### Other

- *(dactyl)* bump dactyl_macros and dactyl package versions to 0.1.1 to fix crates.io bootstrap dependency
- *(dactyl)* configure release-plz for GitOps release management
- *(dactyl)* add crates.io publishing configuration and release scripts
- *(dactyl)* refresh specs fingerprint and update documentation for simplified adapter selection
- automated container updates
- *(dactyl)* read/write are the only public API; first call auto-bootstraps the adapter
- ignore .decapod runtime artifacts in .gitignore
- Initial commit
## Unreleased

## [0.6.2](https://github.com/DecapodLabs/dactyl/compare/dactyl-db-v0.6.1...dactyl-db-v0.6.2) - 2026-08-12

### Added

- complete local SQLite fixtures for #57 and #64 ([#69](https://github.com/DecapodLabs/dactyl/pull/69))

### Other

- release v0.6.2 ([#71](https://github.com/DecapodLabs/dactyl/pull/71))
- reconcile Decapod state with latest origin/main ([#68](https://github.com/DecapodLabs/dactyl/pull/68))
- release v0.6.1 ([#67](https://github.com/DecapodLabs/dactyl/pull/67))

## [0.6.2](https://github.com/DecapodLabs/dactyl/compare/dactyl-db-v0.6.1...dactyl-db-v0.6.2) - 2026-08-12

### Added

- complete local SQLite fixtures for #57 and #64 ([#69](https://github.com/DecapodLabs/dactyl/pull/69))

### Other

- reconcile Decapod state with latest origin/main ([#68](https://github.com/DecapodLabs/dactyl/pull/68))
- release v0.6.1 ([#67](https://github.com/DecapodLabs/dactyl/pull/67))

## [0.6.1](https://github.com/DecapodLabs/dactyl/compare/dactyl-db-v0.6.0...dactyl-db-v0.6.1) - 2026-08-12

### Added

- forward opaque storage context for Issue #64 ([#65](https://github.com/DecapodLabs/dactyl/pull/65))

### Other

- release v0.6.1 ([#66](https://github.com/DecapodLabs/dactyl/pull/66))
- release v0.6.0 ([#63](https://github.com/DecapodLabs/dactyl/pull/63))

## [0.6.1](https://github.com/DecapodLabs/dactyl/compare/dactyl-db-v0.6.0...dactyl-db-v0.6.1) - 2026-08-12

### Added

- forward opaque storage context for Issue #64 ([#65](https://github.com/DecapodLabs/dactyl/pull/65))

### Other

- release v0.6.0 ([#63](https://github.com/DecapodLabs/dactyl/pull/63))

## [0.6.0](https://github.com/DecapodLabs/dactyl/compare/dactyl-db-v0.5.0...dactyl-db-v0.6.0) - 2026-08-12

### Added

- complete pure-rust storage parity for Decapod ([#61](https://github.com/DecapodLabs/dactyl/pull/61))

### Other

- release v0.6.0 ([#62](https://github.com/DecapodLabs/dactyl/pull/62))
- release v0.5.0 ([#60](https://github.com/DecapodLabs/dactyl/pull/60))

## [0.6.0](https://github.com/DecapodLabs/dactyl/compare/dactyl-db-v0.5.0...dactyl-db-v0.6.0) - 2026-08-12

### Added

- complete pure-rust storage parity for Decapod ([#61](https://github.com/DecapodLabs/dactyl/pull/61))

### Other

- release v0.5.0 ([#60](https://github.com/DecapodLabs/dactyl/pull/60))

## [0.5.0](https://github.com/DecapodLabs/dactyl/compare/dactyl-db-v0.4.0...dactyl-db-v0.5.0) - 2026-08-11

### Added

- add lightweight pure-Rust datastore primitives (#51-#57) ([#58](https://github.com/DecapodLabs/dactyl/pull/58))

### Other

- release v0.5.0 ([#59](https://github.com/DecapodLabs/dactyl/pull/59))
- release v0.4.0 ([#50](https://github.com/DecapodLabs/dactyl/pull/50))

## [0.5.0](https://github.com/DecapodLabs/dactyl/compare/dactyl-db-v0.4.0...dactyl-db-v0.5.0) - 2026-08-11

### Added

- add lightweight pure-Rust datastore primitives (#51-#57) ([#58](https://github.com/DecapodLabs/dactyl/pull/58))

### Other

- release v0.4.0 ([#50](https://github.com/DecapodLabs/dactyl/pull/50))

## [0.4.0](https://github.com/DecapodLabs/dactyl/compare/dactyl-db-v0.3.0...dactyl-db-v0.4.0) - 2026-08-06

### Added

- make dactyl a thin application read write driver ([#48](https://github.com/DecapodLabs/dactyl/pull/48))

### Other

- release v0.4.0 ([#49](https://github.com/DecapodLabs/dactyl/pull/49))
- Update Decapod version in README

## [0.4.0](https://github.com/DecapodLabs/dactyl/compare/dactyl-db-v0.3.0...dactyl-db-v0.4.0) - 2026-08-06

### Added

- make dactyl a thin application read write driver ([#48](https://github.com/DecapodLabs/dactyl/pull/48))

### Other

- Update Decapod version in README

- *(dactyl-db)* reduce the public contract to congruent application `read` /
  `write` operations for SQLite and Neon, remove the proc-macro/query-analysis
  layer, and replace the rusqlite facade with a private SQLite C-API driver
  ([#47](https://github.com/DecapodLabs/dactyl/issues/47)).
