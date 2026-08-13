#![cfg(feature = "sqlite")]

use std::time::Duration;

use dactyl_db::{
    AccessMode, AdapterErrorKind, Connection, DatastoreRoute, GeneratedKey, OpenOptions, Operation,
    OperationResult, Parameter,
};
use rusqlite::Connection as NativeConnection;
use tempfile::NamedTempFile;

#[test]
fn generated_keys_and_reopen_are_explicit_and_durable() {
    let file = NamedTempFile::new().unwrap();
    let path = file.path().to_string_lossy().into_owned();
    let db = Connection::open(DatastoreRoute::sqlite(&path)).unwrap();
    db.write(
        "create table app (id integer primary key, name text unique)",
        &[],
    )
    .unwrap();
    let result = db
        .write_result(
            "insert into app (name) values ($1)",
            &[Parameter::Text("persisted".into())],
        )
        .unwrap();
    assert_eq!(result.affected_rows, 1);
    assert_eq!(result.generated_key(), Some(&GeneratedKey::Integer(1)));

    let reopened = Connection::open(DatastoreRoute::sqlite(&path)).unwrap();
    let rows = reopened.read("select id, name from app", &[]).unwrap();
    assert_eq!(rows.as_slice()[0].get_int("id").unwrap(), 1);
    assert_eq!(rows.as_slice()[0].get_str("name").unwrap(), "persisted");
}

#[test]
fn atomic_batch_rolls_back_schema_and_data_together() {
    let file = NamedTempFile::new().unwrap();
    let db = Connection::open(DatastoreRoute::sqlite(file.path().to_string_lossy())).unwrap();
    let error = db
        .atomic(&[
            Operation::schema(
                "create table app (id integer primary key, name text)",
                Vec::new(),
            ),
            Operation::write(
                "insert into missing (name) values ($1)",
                vec![Parameter::Text("x".into())],
            ),
        ])
        .unwrap_err();
    assert!(matches!(
        error,
        dactyl_db::DactylError::Adapter {
            kind: AdapterErrorKind::Query,
            ..
        }
    ));
    assert!(db.read("select id from app", &[]).is_err());
}

#[test]
fn atomic_results_preserve_order_and_zero_row_writes() {
    let file = NamedTempFile::new().unwrap();
    let db = Connection::open(DatastoreRoute::sqlite(file.path().to_string_lossy())).unwrap();
    let result = db
        .atomic(&[
            Operation::schema(
                "create table app (id integer primary key, name text)",
                Vec::new(),
            ),
            Operation::write(
                "insert into app (name) values ($1)",
                vec![Parameter::Text("one".into())],
            ),
            Operation::write(
                "update app set name = $1 where id = $2",
                vec![Parameter::Text("none".into()), Parameter::Integer(99)],
            ),
            Operation::read("select id, name from app", Vec::new()),
        ])
        .unwrap();
    assert!(matches!(result.results[0], OperationResult::Write(_)));
    assert!(matches!(result.results[1], OperationResult::Write(_)));
    match &result.results[2] {
        OperationResult::Write(result) => assert_eq!(result.affected_rows, 0),
        other => panic!("unexpected result: {other:?}"),
    }
    match &result.results[3] {
        OperationResult::Rows(rows) => assert_eq!(rows.len(), 1),
        other => panic!("unexpected result: {other:?}"),
    }
}

#[test]
fn read_only_open_is_non_mutating_and_typed() {
    let file = NamedTempFile::new().unwrap();
    let path = file.path().to_string_lossy().into_owned();
    let db = Connection::open(DatastoreRoute::sqlite(&path)).unwrap();
    db.write("create table app (id integer primary key)", &[])
        .unwrap();
    drop(db);

    let readonly = Connection::open_with_options(
        DatastoreRoute::sqlite(&path),
        OpenOptions {
            access_mode: AccessMode::ReadOnly,
            lock_timeout: Duration::from_millis(5),
        },
    )
    .unwrap();
    assert_eq!(readonly.access_mode(), AccessMode::ReadOnly);
    assert!(readonly.read("select id from app", &[]).unwrap().is_empty());
    let error = readonly
        .write("insert into app default values", &[])
        .unwrap_err();
    assert!(matches!(
        error,
        dactyl_db::DactylError::Adapter {
            kind: AdapterErrorKind::ReadOnly,
            ..
        }
    ));

    let missing = tempfile::tempdir().unwrap().path().join("missing.store");
    let error = Connection::open_with_options(
        DatastoreRoute::sqlite(missing.to_string_lossy()),
        OpenOptions {
            access_mode: AccessMode::ReadOnly,
            lock_timeout: Duration::from_millis(5),
        },
    );
    let error = match error {
        Ok(_) => panic!("read-only open unexpectedly succeeded"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        dactyl_db::DactylError::Adapter {
            kind: AdapterErrorKind::NotFound,
            ..
        }
    ));
}

#[test]
fn lock_timeout_is_typed_and_bounded() {
    let file = NamedTempFile::new().unwrap();
    let path = file.path().to_string_lossy().into_owned();
    let setup = Connection::open(DatastoreRoute::sqlite(&path)).unwrap();
    setup
        .write("create table app (id integer primary key)", &[])
        .unwrap();
    drop(setup);
    let blocker = NativeConnection::open(&path).unwrap();
    blocker.execute_batch("BEGIN EXCLUSIVE").unwrap();
    let db = Connection::open_with_options(
        DatastoreRoute::sqlite(&path),
        OpenOptions {
            access_mode: AccessMode::ReadWrite,
            lock_timeout: Duration::from_millis(5),
        },
    )
    .unwrap();
    let error = db.write("insert into app default values", &[]).unwrap_err();
    assert!(matches!(
        error.adapter_kind(),
        Some(AdapterErrorKind::Busy | AdapterErrorKind::Locked)
    ));
    assert!(error.is_retryable());
    blocker.execute_batch("ROLLBACK").unwrap();
}

#[test]
fn separate_open_connections_refresh_before_mutating() {
    let file = NamedTempFile::new().unwrap();
    let path = file.path().to_string_lossy().into_owned();
    let first = Connection::open(DatastoreRoute::sqlite(&path)).unwrap();
    let second = Connection::open(DatastoreRoute::sqlite(&path)).unwrap();
    first
        .write("create table app (id integer primary key, name text)", &[])
        .unwrap();
    first
        .write(
            "insert into app (name) values ($1)",
            &[Parameter::Text("first".into())],
        )
        .unwrap();
    second
        .write(
            "insert into app (name) values ($1)",
            &[Parameter::Text("second".into())],
        )
        .unwrap();
    let rows = Connection::open(DatastoreRoute::sqlite(&path))
        .unwrap()
        .read("select id, name from app order by id", &[])
        .unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn caller_owned_schema_supports_upgrade_constraints_indexes_and_cascade() {
    let file = NamedTempFile::new().unwrap();
    let path = file.path().to_string_lossy().into_owned();
    let db = Connection::open(DatastoreRoute::sqlite(&path)).unwrap();
    db.atomic(&[Operation::schema(
        "create table parents (id integer primary key); create table children (id integer primary key, parent_id integer not null references parents(id) on delete cascade, name text default 'child'); create unique index if not exists children_name on children(name)",
        Vec::new(),
    )])
    .unwrap();
    db.write(
        "insert into parents (id) values ($1)",
        &[Parameter::Integer(7)],
    )
    .unwrap();
    db.write(
        "insert into children (id, parent_id) values ($1, $2)",
        &[Parameter::Integer(8), Parameter::Integer(7)],
    )
    .unwrap();
    db.write(
        "insert into parents (id) values ($1)",
        &[Parameter::Integer(10)],
    )
    .unwrap();
    let error = db
        .write(
            "insert into children (id, parent_id) values ($1, $2)",
            &[Parameter::Integer(9), Parameter::Integer(10)],
        )
        .unwrap_err();
    assert!(matches!(
        error,
        dactyl_db::DactylError::Adapter {
            kind: AdapterErrorKind::Constraint,
            ..
        }
    ));
    let error = db
        .write(
            "insert into children (id, parent_id) values ($1, $2)",
            &[Parameter::Integer(9), Parameter::Integer(99)],
        )
        .unwrap_err();
    assert!(matches!(
        error,
        dactyl_db::DactylError::Adapter {
            kind: AdapterErrorKind::Constraint,
            ..
        }
    ));

    db.write(
        "create table pairs (left_id integer, right_id integer, unique (left_id, right_id))",
        &[],
    )
    .unwrap();
    db.write(
        "insert into pairs (left_id, right_id) values ($1, $2)",
        &[Parameter::Integer(1), Parameter::Integer(2)],
    )
    .unwrap();
    db.write(
        "insert into pairs (left_id, right_id) values ($1, $2)",
        &[Parameter::Integer(1), Parameter::Integer(3)],
    )
    .unwrap();
    let error = db
        .write(
            "insert into pairs (left_id, right_id) values ($1, $2)",
            &[Parameter::Integer(1), Parameter::Integer(2)],
        )
        .unwrap_err();
    assert!(matches!(
        error,
        dactyl_db::DactylError::Adapter {
            kind: AdapterErrorKind::Constraint,
            ..
        }
    ));

    db.atomic(&[Operation::schema(
        "alter table children add column status text not null default 'open'",
        Vec::new(),
    )])
    .unwrap();
    let row = db
        .read(
            "select status from children where id = $1",
            &[Parameter::Integer(8)],
        )
        .unwrap();
    assert_eq!(row.as_slice()[0].get_str("status").unwrap(), "open");

    db.write(
        "delete from parents where id = $1",
        &[Parameter::Integer(7)],
    )
    .unwrap();
    assert!(db.read("select id from children", &[]).unwrap().is_empty());
    let reopened = Connection::open(DatastoreRoute::sqlite(&path)).unwrap();
    assert!(reopened
        .read("select id from parents", &[])
        .unwrap()
        .as_slice()[0]
        .get_int("id")
        .is_ok());
}

#[test]
fn failed_schema_upgrade_does_not_survive_reopen() {
    let file = NamedTempFile::new().unwrap();
    let path = file.path().to_string_lossy().into_owned();
    let db = Connection::open(DatastoreRoute::sqlite(&path)).unwrap();
    db.write("create table app (id integer primary key)", &[])
        .unwrap();
    db.write("insert into app (id) values ($1)", &[Parameter::Integer(1)])
        .unwrap();
    let error = db
        .atomic(&[Operation::schema(
            "alter table app add column status text default 'open'; alter table missing add column value text",
            Vec::new(),
        )])
        .unwrap_err();
    assert!(matches!(
        error,
        dactyl_db::DactylError::Adapter {
            kind: AdapterErrorKind::Query,
            ..
        }
    ));
    let reopened = Connection::open(DatastoreRoute::sqlite(&path)).unwrap();
    assert!(reopened.read("select status from app", &[]).is_err());
}

#[test]
fn read_only_atomic_schema_and_reader_reuse_are_non_mutating() {
    let file = NamedTempFile::new().unwrap();
    let path = file.path().to_string_lossy().into_owned();
    let db = Connection::open(DatastoreRoute::sqlite(&path)).unwrap();
    db.write("create table app (id integer primary key)", &[])
        .unwrap();
    drop(db);
    let options = OpenOptions {
        access_mode: AccessMode::ReadOnly,
        lock_timeout: Duration::from_millis(5),
    };
    let readers = (0..3)
        .map(|_| Connection::open_with_options(DatastoreRoute::sqlite(&path), options).unwrap())
        .collect::<Vec<_>>();
    let error = readers[0]
        .atomic(&[Operation::schema(
            "create table forbidden (id integer primary key)",
            Vec::new(),
        )])
        .unwrap_err();
    assert!(matches!(
        error,
        dactyl_db::DactylError::Adapter {
            kind: AdapterErrorKind::ReadOnly,
            ..
        }
    ));
    for reader in readers {
        assert!(reader.read("select id from app", &[]).unwrap().is_empty());
    }
    assert!(Connection::open(DatastoreRoute::sqlite(&path))
        .unwrap()
        .read("select id from forbidden", &[])
        .is_err());
}
