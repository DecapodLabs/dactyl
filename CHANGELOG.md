# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.4](https://github.com/DecapodLabs/dactyl/compare/dactyl-db-macros-v0.2.3...dactyl-db-macros-v0.2.4) - 2026-08-02

### Added

- *(dactyl)* complete typed/NULL-safe named row projections ([#25](https://github.com/DecapodLabs/dactyl/pull/25)) ([#41](https://github.com/DecapodLabs/dactyl/pull/41))

### Added

- *(dactyl)* complete typed/NULL-safe named row projections for [#25](https://github.com/DecapodLabs/dactyl/issues/25): `try_get`, `is_null`, borrowed `get_str_ref` / `get_json_ref`, explicit first-match duplicate-alias semantics, NULL conversion messages, unit + SQLite/Neon matrix conformance (also DecapodLabs/decapod#1111, dactyl #2).

### Documentation

- *(dactyl)* document the full `Row` projection contract (scalars, NULL, missing columns, aliases, ownership/lifetime) in the README and crate docs.

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
