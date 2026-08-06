#![cfg(feature = "sqlite")]

use dactyl_db::{
    Connection, ConnectionOptions, Datastore, DatastoreRoute, Parameter, Statement, StorageOp,
    StorageResult,
};
use tempfile::NamedTempFile;

#[test]
fn connection_contract_covers_scripts_batches_and_last_insert_id() {
    let db = NamedTempFile::new().unwrap();
    let connection = Connection::open(DatastoreRoute::sqlite(db.path().to_string_lossy()))
        .expect("sqlite connection");

    connection
        .execute_batch(
            "CREATE TABLE records (id INTEGER PRIMARY KEY, body BLOB NOT NULL);\
             CREATE INDEX records_id ON records(id);",
        )
        .expect("schema script");
    connection
        .transaction(&[
            Statement::new(
                "INSERT INTO records(body) VALUES ($1)",
                vec![Parameter::Blob(vec![1, 2, 3])],
            ),
            Statement::new(
                "INSERT INTO records(body) VALUES ($1)",
                vec![Parameter::Blob(vec![4, 5])],
            ),
        ])
        .expect("atomic writes");

    assert_eq!(connection.last_insert_id().unwrap(), 2);
    let rows = connection
        .query("SELECT id, body FROM records ORDER BY id", &[])
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows.as_slice()[0].get_json("body").unwrap(),
        serde_json::json!([1, 2, 3])
    );

    let result = connection
        .execute_op(StorageOp::Query {
            sql: "SELECT count(*) AS count FROM records".into(),
            params: vec![],
        })
        .unwrap();
    assert!(matches!(result, StorageResult::Rows(rows) if rows.len() == 1));
    assert_eq!(connection.datastore(), Datastore::Sqlite);
}

#[test]
fn runtime_analysis_rejects_unsafe_sql_and_allows_bounded_rewrites() {
    let db = NamedTempFile::new().unwrap();
    let route = DatastoreRoute::sqlite(db.path().to_string_lossy());
    let strict = Connection::open(route.clone()).unwrap();
    assert!(matches!(
        strict.query("SELECT now()", &[]),
        Err(dactyl_db::DactylError::Unsupported { .. })
    ));

    let rewriting =
        Connection::open_with_options(route, ConnectionOptions::default().with_rewrites(true))
            .unwrap();
    let rows = rewriting
        .query("SELECT now() AS current_time", &[])
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows.as_slice()[0].get_str("current_time").is_ok());
}

#[test]
fn inline_directives_are_checked_against_the_connection() {
    let db = NamedTempFile::new().unwrap();
    let connection = Connection::open(DatastoreRoute::sqlite(db.path().to_string_lossy())).unwrap();
    let rows = connection
        .query("-- dactyl: sqlite\nSELECT 1 AS value", &[])
        .unwrap();
    assert_eq!(rows.as_slice()[0].get_int("value").unwrap(), 1);

    assert!(matches!(
        connection.query("-- dactyl: neon\nSELECT 1", &[]),
        Err(dactyl_db::DactylError::Routing(_))
    ));
}

#[test]
fn read_only_policy_is_enforced_by_the_local_boundary() {
    let db = NamedTempFile::new().unwrap();
    let writable = Connection::open(DatastoreRoute::sqlite(db.path().to_string_lossy())).unwrap();
    writable
        .execute("CREATE TABLE values_table (value INTEGER)", &[])
        .unwrap();

    let read_only = Connection::open_with_options(
        DatastoreRoute::sqlite(db.path().to_string_lossy()),
        ConnectionOptions::default().read_only(true),
    )
    .unwrap();
    assert_eq!(
        read_only
            .query("SELECT count(*) AS count FROM values_table", &[])
            .unwrap()
            .len(),
        1
    );
    assert!(read_only
        .execute("INSERT INTO values_table(value) VALUES (1)", &[])
        .is_err());
}
