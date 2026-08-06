#![cfg(feature = "sqlite")]

use dactyl_db::{AdapterErrorKind, Connection, Datastore, DatastoreRoute, Parameter};
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
