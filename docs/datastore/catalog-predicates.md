# Bounded catalog predicates

The additive `Connection::has_table` and `Connection::has_column` methods answer one schema question without constructing `StoreSchema`, fetching every index/foreign key, or counting application rows. SQLite binds names as values and executes one SELECT per predicate. Full `inspect_schema` and its version-1 projection are unchanged. No data, schema, indexes, durability pragmas or persisted bytes change.

Table matching preserves `StoreSchema::table`: the stored name must equal the supplied name or its ASCII-lowercase form. It does not introduce arbitrary case-insensitive matching for quoted mixed-case names. Catalog filtering and `table_info` column visibility match full inspection, including its existing temporary-table shadowing behavior. Column names match exactly; views and generated/hidden columns omitted by full inspection remain omitted. Each call reads current state, so ALTER/DROP need no cache invalidation. Table-valued pragmas require SQLite 3.16 or newer; engine errors retain their original typed categories.

Remote adapters return `AdapterErrorKind::Capability` with `unsupported_schema_inspection`. They do not issue RPCs, inspect a different database, or fall back to local authority. Dactyl still owns only physical execution. Callers own schema interpretation, migrations, tenant authorization and governance judgment. Consumers must wait for a published crate release before using the new API; no unpublished path dependency is added to Decapod.

## Reproducible evidence

`tests/catalog_predicates.rs::measure_catalog_predicates` compares the unchanged full-inspection path from accepted `1fa52a2` / published 0.11.1 with the new public method in the same process. The disposable fixture has 32 tables, a unique stream/sequence index and 0/1K/10K/100K event rows with 1,024-byte payloads. Three trials alternate variant order and mix present/missing names across 256 pairs. All trial percentiles remain in `catalog-measurements.json`; results are warm in-process measurements using SQLite 3.54.0 and Cargo's default unoptimized test profile on macOS arm64.

At 100K history, median trial table-predicate p50 is 1.658 ms before and 0.0238 ms after; p95 is 2.297 ms versus 0.0447 ms, and p99 is 2.851 ms versus 0.1235 ms. These are medians of trial percentiles, not pooled percentiles or end-to-end CLI guarantees. A test-only thread-local prepare counter observes 161 prepared statements for full inspection of the 32-table indexed fixture versus exactly one per new predicate (including a missing table). Instrumentation is absent from production and from the integration timing binary.

This saves 160 statements per predicate in that fixture and eliminates all application-row counts from the predicate path. It saves no persistent bytes or writes. Column-predicate latency, allocations, connection startup, actual Decapod invocation latency, cloud transport and hosted capacity are not measured here. Decapod integration remains a separate increment after Dactyl publication.

```sh
cargo test --features sqlite --test catalog_predicates
cargo test --features sqlite --lib predicates_prepare -- --nocapture
DACTYL_CATALOG_REPORT=/absolute/new/report.json \
  cargo test --features sqlite --test catalog_predicates measure_catalog_predicates \
  -- --ignored --nocapture
```

The report path must not already exist. Fixtures never open a user database. Ordinary regression tests cover exact name matching, quotes, Unicode, NUL/injection-shaped inputs, views, temporary shadowing, live DDL changes, read-only access and byte preservation. Remote contract tests check explicit capability rejection. These checks are separate from full publication readiness and from PostgreSQL or hosted Supabase proof.
