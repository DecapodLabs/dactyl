#![cfg(feature = "sqlite")]

use dactyl_db::{
    AdapterErrorKind, Connection, Datastore, DatastoreRoute, Parameter, StorageContext,
};
use serde_json::json;
use tempfile::NamedTempFile;

#[test]
fn sqlite_read_write_is_the_only_driver_contract() {
    let file = NamedTempFile::new().expect("temp sqlite file");
    let db = Connection::open(DatastoreRoute::sqlite(file.path().to_string_lossy())).unwrap();
    assert_eq!(db.datastore(), Datastore::Sqlite);

    db.write(
        "create table app (id integer primary key, name text, enabled integer, payload blob)",
        &[],
    )
    .unwrap();
    assert_eq!(
        db.write(
            "insert into app (name, enabled, payload) values ($1, $2, $3)",
            &[
                Parameter::Text("opened".into()),
                Parameter::Bool(true),
                Parameter::Blob(vec![1, 2, 3]),
            ],
        )
        .unwrap(),
        1
    );

    let rows = db
        .read("select id, name, enabled, payload from app", &[])
        .unwrap();
    let row = &rows.as_slice()[0];
    assert_eq!(row.get_int("id").unwrap(), 1);
    assert_eq!(row.get_str("name").unwrap(), "opened");
    assert!(row.get_bool("enabled").unwrap());
    assert_eq!(
        row.get_json("payload").unwrap(),
        serde_json::json!([1, 2, 3])
    );
}

#[test]
fn sqlite_errors_are_typed_without_string_parsing() {
    let file = NamedTempFile::new().expect("temp sqlite file");
    let db = Connection::open(DatastoreRoute::sqlite(file.path().to_string_lossy())).unwrap();
    db.write(
        "create table app (id integer primary key, name text unique)",
        &[],
    )
    .unwrap();
    db.write(
        "insert into app (name) values ($1)",
        &[Parameter::Text("a".into())],
    )
    .unwrap();

    let error = db
        .write(
            "insert into app (name) values ($1)",
            &[Parameter::Text("a".into())],
        )
        .unwrap_err();
    assert!(matches!(
        error,
        dactyl_db::DactylError::Adapter {
            kind: AdapterErrorKind::Constraint,
            ..
        }
    ));
}

#[test]
fn sqlite_ignores_remote_context_without_changing_local_results() {
    let file = NamedTempFile::new().unwrap();
    let path = file.path().to_string_lossy().into_owned();
    let db = Connection::open_with_context(
        DatastoreRoute::sqlite(path),
        Some(
            StorageContext::new(
                1,
                json!({"opaque_target": "remote", "opaque_session": "ignored"}),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    db.write("create table app (id integer primary key, name text)", &[])
        .unwrap();
    db.write(
        "insert into app (name) values ($1)",
        &[Parameter::Text("local".into())],
    )
    .unwrap();
    assert_eq!(db.context().unwrap().version(), 1);
    assert_eq!(
        db.read("select name from app", &[]).unwrap().as_slice()[0]
            .get_str("name")
            .unwrap(),
        "local"
    );
}
