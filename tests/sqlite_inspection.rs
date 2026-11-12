#![cfg(feature = "sqlite")]

use dactyl_db::{AccessMode, Connection, DatastoreRoute, OpenOptions, SnapshotLimits};
use std::time::Duration;

fn readonly(path: &std::path::Path) -> Connection {
    Connection::open_with_options(
        DatastoreRoute::sqlite(path.to_string_lossy()),
        OpenOptions {
            access_mode: AccessMode::ReadOnly,
            lock_timeout: Duration::from_millis(100),
        },
    )
    .unwrap()
}

#[test]
fn snapshot_is_consistent_across_concurrent_wal_writes_and_never_initializes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.db");
    let writer = Connection::open(DatastoreRoute::sqlite(path.to_string_lossy())).unwrap();
    writer
        .write("create table records(value text)", &[])
        .unwrap();
    writer.write("pragma journal_mode=WAL", &[]).unwrap();
    writer
        .write("insert into records values ('before')", &[])
        .unwrap();
    let reader = readonly(&path);
    reader
        .inspect_snapshot(SnapshotLimits::default(), &mut |snapshot| {
            let before = snapshot.read("select value from records", &[])?;
            writer.write("insert into records values ('after')", &[])?;
            assert_eq!(snapshot.read("select value from records", &[])?, before);
            Ok(())
        })
        .unwrap();
    reader
        .inspect_snapshot(SnapshotLimits::default(), &mut |snapshot| {
            assert_eq!(
                snapshot
                    .read("select value from records", &[])?
                    .as_slice()
                    .len(),
                2
            );
            Ok(())
        })
        .unwrap();
    let missing = dir.path().join("missing.db");
    assert!(Connection::open_with_options(
        DatastoreRoute::sqlite(missing.to_string_lossy()),
        OpenOptions {
            access_mode: AccessMode::ReadOnly,
            ..OpenOptions::default()
        }
    )
    .is_err());
    assert!(!missing.exists());
}

#[test]
fn mutation_and_pragma_are_rejected_and_early_stop_releases_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.db");
    let writer = Connection::open(DatastoreRoute::sqlite(path.to_string_lossy())).unwrap();
    writer
        .write("create table records(value text)", &[])
        .unwrap();
    writer
        .write("insert into records values ('one'), ('two')", &[])
        .unwrap();
    assert_eq!(
        writer
            .inspect_snapshot(SnapshotLimits::default(), &mut |_| Ok(()))
            .unwrap_err()
            .adapter_code(),
        Some("inspection_requires_read_only")
    );
    let reader = readonly(&path);
    for sql in [
        "delete from records",
        "pragma user_version=100",
        "with x as (select 1) delete from records",
    ] {
        assert!(reader
            .inspect_snapshot(SnapshotLimits::default(), &mut |snapshot| {
                snapshot.read(sql, &[])?;
                Ok(())
            })
            .is_err());
    }
    reader
        .inspect_snapshot(SnapshotLimits::default(), &mut |snapshot| {
            let status = snapshot.visit("select value from records", &[], &mut |_| Ok(false))?;
            assert!(!status.complete);
            assert_eq!(status.rows, 1);
            Ok(())
        })
        .unwrap();
    writer
        .write("insert into records values ('three')", &[])
        .unwrap();
    assert_eq!(
        writer
            .read("select * from records", &[])
            .unwrap()
            .as_slice()
            .len(),
        3
    );
}

#[test]
fn row_byte_and_vm_budgets_fail_without_successful_truncation_and_are_scoped() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.db");
    let writer = Connection::open(DatastoreRoute::sqlite(path.to_string_lossy())).unwrap();
    writer
        .write("create table records(value text)", &[])
        .unwrap();
    writer
        .write("insert into records values ('é🙂'), ('other')", &[])
        .unwrap();
    let reader = readonly(&path);
    for (limits, sql) in [
        (SnapshotLimits { rows: 1, ..SnapshotLimits::default() }, "select value from records"),
        (SnapshotLimits { result_bytes: 1, ..SnapshotLimits::default() }, "select value from records"),
        (SnapshotLimits { vm_steps: 1000, ..SnapshotLimits::default() }, "with recursive n(x) as (select 1 union all select x+1 from n where x<10000000) select sum(x) from n"),
    ] {
        let error = reader.inspect_snapshot(limits, &mut |snapshot| { snapshot.read(sql, &[])?; Ok(()) }).unwrap_err();
        assert_eq!(error.adapter_code(), Some("inspection_budget_exceeded"));
    }
    reader
        .inspect_snapshot(SnapshotLimits::default(), &mut |snapshot| {
            let rows = snapshot.read("select '', x'', NULL, -9223372036854775808", &[])?;
            assert_eq!(rows.as_slice()[0].values[0], serde_json::json!(""));
            assert_eq!(rows.as_slice()[0].values[1], serde_json::json!([]));
            assert_eq!(rows.as_slice()[0].values[2], serde_json::Value::Null);
            assert_eq!(rows.as_slice()[0].values[3], serde_json::json!(i64::MIN));
            Ok(())
        })
        .unwrap();
}
