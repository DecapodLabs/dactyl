#![cfg(feature = "sqlite")]

use std::fs;

use dactyl_db::{AccessMode, AdapterErrorKind, Connection, DatastoreRoute, OpenOptions, Parameter};
use tempfile::{NamedTempFile, TempDir};

const FIXTURE: &str = "tests/fixtures/decapod_legacy.sqlite";

#[test]
fn existing_sqlite_fixture_opens_without_conversion() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("decapod.db");
    fs::copy(FIXTURE, &path).unwrap();

    let db = Connection::open(DatastoreRoute::sqlite(path.to_string_lossy())).unwrap();
    assert_eq!(
        db.inspect_schema()
            .unwrap()
            .table("events")
            .unwrap()
            .row_count,
        1
    );
    assert_eq!(
        db.inspect_schema()
            .unwrap()
            .table("tasks")
            .unwrap()
            .row_count,
        1
    );

    let rows = db
        .read(
            "select event_id, payload from events where event_id = $1",
            &[Parameter::Text("evt1".into())],
        )
        .unwrap();
    assert_eq!(
        rows.as_slice()[0].get_str("payload").unwrap(),
        r#"{"title":"imported"}"#
    );

    let task = db
        .read(
            "select title, revision, payload from tasks where id = $1",
            &[Parameter::Text("task-1".into())],
        )
        .unwrap();
    assert_eq!(
        task.as_slice()[0].get_str("title").unwrap(),
        "imported task"
    );
    assert_eq!(task.as_slice()[0].get_int("revision").unwrap(), 3);
    assert_eq!(
        task.as_slice()[0].get_blob("payload").unwrap(),
        vec![0, 1, 255]
    );

    assert!(fs::read(&path).unwrap().starts_with(b"SQLite format 3\0"));
}

#[test]
fn existing_sqlite_fixture_is_writable_and_reopens() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("decapod.db");
    fs::copy(FIXTURE, &path).unwrap();
    let route = DatastoreRoute::sqlite(path.to_string_lossy());

    let db = Connection::open(route.clone()).unwrap();
    assert_eq!(
        db.write(
            "update tasks set revision = $1 where id = $2",
            &[Parameter::Integer(4), Parameter::Text("task-1".into())],
        )
        .unwrap(),
        1
    );
    drop(db);

    let reopened = Connection::open(route).unwrap();
    let rows = reopened
        .read(
            "select revision from tasks where id = $1",
            &[Parameter::Text("task-1".into())],
        )
        .unwrap();
    assert_eq!(rows.as_slice()[0].get_int("revision").unwrap(), 4);
}

#[test]
fn real_sqlite_supports_null_real_blob_and_generated_keys() {
    let file = NamedTempFile::new().unwrap();
    let db = Connection::open(DatastoreRoute::sqlite(file.path().to_string_lossy())).unwrap();
    db.write(
        "create table values_test (id integer primary key, nullable text, real_value real, payload blob)",
        &[],
    )
    .unwrap();
    let result = db
        .write_result(
            "insert into values_test (nullable, real_value, payload) values ($1, $2, $3)",
            &[
                Parameter::Null,
                Parameter::Real(1.5),
                Parameter::Blob(vec![9, 8, 7]),
            ],
        )
        .unwrap();
    assert_eq!(result.affected_rows, 1);
    assert_eq!(
        result.generated_key(),
        Some(&dactyl_db::GeneratedKey::Integer(1))
    );

    let rows = db
        .read("select nullable, real_value, payload from values_test", &[])
        .unwrap();
    let row = &rows.as_slice()[0];
    assert!(row.is_null("nullable").unwrap());
    assert_eq!(row.get_real("real_value").unwrap(), 1.5);
    assert_eq!(row.get_blob("payload").unwrap(), vec![9, 8, 7]);
}

#[test]
fn sqlite_read_only_and_missing_paths_fail_closed() {
    let missing = TempDir::new().unwrap().path().join("missing.db");
    let error = match Connection::open_with_options(
        DatastoreRoute::sqlite(missing.to_string_lossy()),
        OpenOptions {
            access_mode: AccessMode::ReadOnly,
            ..OpenOptions::default()
        },
    ) {
        Ok(_) => panic!("read-only open unexpectedly created a missing database"),
        Err(error) => error,
    };
    assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::NotFound));
    assert_eq!(error.adapter_code(), Some("missing_database"));

    let file = NamedTempFile::new().unwrap();
    let db = Connection::open(DatastoreRoute::sqlite(file.path().to_string_lossy())).unwrap();
    db.write("create table readonly_test (id integer primary key)", &[])
        .unwrap();
    drop(db);
    let readonly = Connection::open_with_options(
        DatastoreRoute::sqlite(file.path().to_string_lossy()),
        OpenOptions {
            access_mode: AccessMode::ReadOnly,
            ..OpenOptions::default()
        },
    )
    .unwrap();
    let error = readonly
        .write("insert into readonly_test default values", &[])
        .unwrap_err();
    assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::ReadOnly));
}

#[test]
fn malformed_local_file_fails_closed_as_a_typed_capability_error() {
    let file = NamedTempFile::new().unwrap();
    fs::write(file.path(), b"not a SQLite database").unwrap();
    let error = match Connection::open(DatastoreRoute::sqlite(file.path().to_string_lossy())) {
        Ok(_) => panic!("malformed local file unexpectedly opened"),
        Err(error) => error,
    };
    assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Capability));
    assert_eq!(error.adapter_code(), Some("invalid_database"));
}
