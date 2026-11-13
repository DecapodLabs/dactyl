#![cfg(feature = "sqlite")]

use dactyl_db::{Connection, DatastoreRoute, Parameter};
use serde_json::json;
use std::{fs::OpenOptions, io::Write, time::Instant};

#[test]
fn predicates_agree_with_full_inspection_and_observe_ddl() {
    let db = Connection::open(DatastoreRoute::sqlite(":memory:")).unwrap();
    for sql in [
        "CREATE TABLE events(id INTEGER PRIMARY KEY, value TEXT)",
        "CREATE TABLE \"Mixed\"(\"NaMe\" TEXT)",
        "CREATE TABLE \"quoted\"\"table\"(\"odd\"\"column\" TEXT)",
        "CREATE TABLE \"資料\"(\"列\" TEXT)",
        "CREATE VIEW visible_view AS SELECT id FROM events",
        "CREATE TABLE sqliteXcustom(value TEXT)",
    ] {
        db.write(sql, &[]).unwrap();
    }
    for shadow in [false, true] {
        if shadow {
            db.write("CREATE TEMP TABLE events(shadow TEXT)", &[])
                .unwrap();
        }
        let schema = db.inspect_schema().unwrap();
        for table in [
            "events",
            "EVENTS",
            "Mixed",
            "MIXED",
            "mixed",
            "quoted\"table",
            "資料",
            "visible_view",
            "sqlite_schema",
            "sqliteXcustom",
            "missing",
            "events' OR 1=1 --",
            "events\0",
        ] {
            assert_eq!(
                db.has_table(table).unwrap(),
                schema.table(table).is_some(),
                "table {table:?}"
            );
            for column in [
                "id",
                "value",
                "shadow",
                "NaMe",
                "name",
                "odd\"column",
                "列",
                "missing",
                "id' OR 1=1 --",
                "id\0",
            ] {
                let expected = schema
                    .table(table)
                    .is_some_and(|t| t.columns.iter().any(|c| c.name == column));
                assert_eq!(
                    db.has_column(table, column).unwrap(),
                    expected,
                    "column {table:?}.{column:?}"
                );
            }
        }
    }
    db.write("DROP TABLE temp.events", &[]).unwrap();
    assert!(!db.has_column("events", "added").unwrap());
    db.write("ALTER TABLE events ADD COLUMN added TEXT", &[])
        .unwrap();
    assert!(db.has_column("events", "added").unwrap());
    db.write("DROP TABLE events", &[]).unwrap();
    assert!(!db.has_table("events").unwrap());
    assert!(!db.has_column("events", "added").unwrap());
}

#[test]
fn predicates_work_read_only_without_changing_persisted_bytes() {
    use dactyl_db::{AccessMode, OpenOptions as StoreOptions};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("catalog.db");
    let route = DatastoreRoute::sqlite(path.to_string_lossy());
    let db = Connection::open(route.clone()).unwrap();
    db.write(
        "CREATE TABLE evidence(id TEXT PRIMARY KEY, payload BLOB)",
        &[],
    )
    .unwrap();
    db.write("INSERT INTO evidence VALUES('proof',X'00FF01')", &[])
        .unwrap();
    drop(db);
    let before = std::fs::read(&path).unwrap();
    let db = Connection::open_with_options(
        route,
        StoreOptions {
            access_mode: AccessMode::ReadOnly,
            ..StoreOptions::default()
        },
    )
    .unwrap();
    assert!(db.has_table("evidence").unwrap());
    assert!(db.has_column("evidence", "payload").unwrap());
    assert!(!db.has_column("evidence", "missing").unwrap());
    drop(db);
    assert_eq!(std::fs::read(path).unwrap(), before);
}

fn percentiles(mut samples: Vec<u128>) -> serde_json::Value {
    samples.sort_unstable();
    let at = |p: usize| samples[(samples.len() * p).div_ceil(100).saturating_sub(1)];
    json!({"samples":samples.len(),"p50_ns":at(50),"p95_ns":at(95),"p99_ns":at(99)})
}

#[test]
#[ignore = "explicit disposable catalog performance measurement"]
fn measure_catalog_predicates() {
    let mut cases = Vec::new();
    for history in [0, 1_000, 10_000, 100_000] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("catalog.db");
        let db = Connection::open(DatastoreRoute::sqlite(path.to_string_lossy())).unwrap();
        db.write("CREATE TABLE events(id INTEGER PRIMARY KEY, stream TEXT NOT NULL, seq INTEGER NOT NULL, payload TEXT NOT NULL)", &[]).unwrap();
        db.write(
            "CREATE UNIQUE INDEX events_sequence ON events(stream,seq)",
            &[],
        )
        .unwrap();
        for table in 0..31 {
            db.write(
                &format!("CREATE TABLE fixture_{table}(id TEXT PRIMARY KEY, value TEXT)"),
                &[],
            )
            .unwrap();
        }
        if history != 0 {
            db.write("WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<?1) INSERT INTO events SELECT x,'history',x,printf('%01024d',x) FROM n", &[Parameter::Integer(history)]).unwrap();
        }
        let version = db.read("SELECT sqlite_version() AS version", &[]).unwrap();
        let sqlite_version = version.as_slice()[0].get_str("version").unwrap();
        for trial in 0..3 {
            let mut legacy = Vec::new();
            let mut targeted = Vec::new();
            // Alternate both timing order and positive/negative predicates.
            for iteration in 0..256 {
                let table = if iteration % 2 == 0 {
                    "events"
                } else {
                    "absent"
                };
                let expected = table == "events";
                for variant in [trial % 2, 1 - trial % 2] {
                    let start = Instant::now();
                    let observed = if variant == 0 {
                        db.inspect_schema().unwrap().table(table).is_some()
                    } else {
                        db.has_table(table).unwrap()
                    };
                    let elapsed = start.elapsed().as_nanos();
                    assert_eq!(observed, expected);
                    if variant == 0 {
                        legacy.push(elapsed);
                    } else {
                        targeted.push(elapsed);
                    }
                }
            }
            cases.push(json!({"history_rows":history,"table_count":32,"trial":trial,"file_bytes":std::fs::metadata(&path).unwrap().len(),"sqlite_version":sqlite_version,"legacy":percentiles(legacy),"targeted_api":percentiles(targeted)}));
        }
    }
    let report = json!({"schema_version":1,"baseline_source":"Legacy full inspection from Dactyl 1fa52a2 / published 0.11.1 versus additive targeted API; identical fixture in one process","fixture":"32 tables, one growing history, 1024-byte payload, unique stream/sequence index; 3 alternating-order trials, 256 positive/negative pairs per trial","scope":"Warm in-process predicates, real Dactyl adapter; no CLI cold-start, allocation, cloud, mutation, durability or network claim; no runtime pragma changes","cases":cases});
    if let Some(path) = std::env::var_os("DACTYL_CATALOG_REPORT") {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .unwrap();
        serde_json::to_writer_pretty(&mut file, &report).unwrap();
        file.write_all(b"\n").unwrap();
    }
    println!("{}", serde_json::to_string(&report).unwrap());
}
