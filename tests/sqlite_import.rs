#![cfg(all(feature = "sqlite", feature = "legacy-import"))]

use std::fs;
use std::path::Path;

use dactyl_db::{
    import_sqlite_file, AccessMode, AdapterErrorKind, Connection, DatastoreRoute, OpenOptions,
    Operation, Parameter,
};
use tempfile::{NamedTempFile, TempDir};

const FIXTURE: &str = "tests/fixtures/decapod_legacy.sqlite";
const CATALOG: &str = include_str!("fixtures/decapod_catalog.sql");

fn copy_fixture(dir: &TempDir, name: &str) -> std::path::PathBuf {
    let dest = dir.path().join(name);
    fs::copy(FIXTURE, &dest).unwrap();
    dest
}

#[test]
fn decapod_catalog_is_accepted_schema() {
    let file = NamedTempFile::new().unwrap();
    let db = Connection::open(DatastoreRoute::sqlite(file.path().to_string_lossy())).unwrap();
    db.atomic(&[Operation::schema(CATALOG, Vec::new())])
        .unwrap();
    let schema = db.inspect_schema().unwrap();
    assert!(schema.table("events").is_some());
    assert!(schema.table("tasks").is_some());
    assert!(schema.table("obligations").is_some());
    assert!(
        schema.table("task_tags").unwrap().foreign_keys[0].on_delete
            == dactyl_db::ForeignKeyAction::Cascade
    );
    assert_eq!(schema.row_count(), 0);
}

#[test]
fn sqlite_import_reopens_with_deterministic_schema_and_values() {
    let dir = TempDir::new().unwrap();
    let source = copy_fixture(&dir, "decapod.db");
    let dest = dir.path().join("dactyl.store");
    let report = import_sqlite_file(&source, &dest).unwrap();
    assert!(!report.already_converted);
    assert_eq!(report.tables, 5);
    assert!(report.rows >= 4);

    let db = Connection::open(DatastoreRoute::sqlite(dest.to_string_lossy())).unwrap();
    let schema = db.inspect_schema().unwrap();
    assert_eq!(schema.table("events").unwrap().row_count, 1);
    assert_eq!(schema.table("tasks").unwrap().row_count, 1);
    assert!(schema.indexes.iter().any(|index| index.unique));

    let event = db
        .read(
            "select event_id, payload from events where event_id = $1",
            &[Parameter::Text("evt1".into())],
        )
        .unwrap();
    assert_eq!(
        event.as_slice()[0].get_str("payload").unwrap(),
        r#"{"title":"imported"}"#
    );

    let task = db
        .read(
            "select id, title, payload, revision from tasks where id = $1",
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

    db.write(
        "update tasks set revision = $1 where id = $2",
        &[Parameter::Integer(4), Parameter::Text("task-1".into())],
    )
    .unwrap();
    let reopened = Connection::open(DatastoreRoute::sqlite(dest.to_string_lossy())).unwrap();
    let revision = reopened
        .read(
            "select revision from tasks where id = $1",
            &[Parameter::Text("task-1".into())],
        )
        .unwrap();
    assert_eq!(revision.as_slice()[0].get_int("revision").unwrap(), 4);
}

#[test]
fn in_place_import_backups_source_and_is_idempotent() {
    let dir = TempDir::new().unwrap();
    let path = copy_fixture(&dir, "decapod.db");
    let first = import_sqlite_file(&path, &path).unwrap();
    assert!(!first.already_converted);
    assert!(Path::new(&first.backup.clone().unwrap()).exists());
    assert!(fs::read(&path).unwrap().starts_with(b"{"));

    let second = import_sqlite_file(&path, &path).unwrap();
    assert!(second.already_converted);

    let from_backup = import_sqlite_file(first.backup.as_ref().unwrap(), &path).unwrap();
    assert!(from_backup.already_converted);
}

#[test]
fn divergent_destination_is_not_overwritten() {
    let dir = TempDir::new().unwrap();
    let source = copy_fixture(&dir, "source.db");
    let dest = dir.path().join("dest.store");
    import_sqlite_file(&source, &dest).unwrap();
    let db = Connection::open(DatastoreRoute::sqlite(dest.to_string_lossy())).unwrap();
    db.write(
        "insert into tasks (id, hash, title) values ($1, $2, $3)",
        &[
            Parameter::Text("task-2".into()),
            Parameter::Text("zzz".into()),
            Parameter::Text("diverged".into()),
        ],
    )
    .unwrap();
    drop(db);
    let error = import_sqlite_file(&source, &dest).unwrap_err();
    assert!(matches!(
        error,
        dactyl_db::DactylError::Adapter {
            kind: AdapterErrorKind::Conflict,
            ..
        }
    ));
    assert_eq!(error.adapter_code(), Some("divergent_destination"));
}

#[test]
fn corrupt_and_missing_inputs_are_typed() {
    let dir = TempDir::new().unwrap();
    let missing = dir.path().join("missing.db");
    let error = import_sqlite_file(&missing, &missing).unwrap_err();
    assert_eq!(error.adapter_code(), Some("missing_input"));

    let garbage = dir.path().join("garbage.db");
    fs::write(&garbage, b"not a database").unwrap();
    let error = import_sqlite_file(&garbage, dir.path().join("out.store")).unwrap_err();
    assert_eq!(error.adapter_code(), Some("not_sqlite"));

    let corrupt = dir.path().join("corrupt.db");
    let mut header = b"SQLite format 3\0".to_vec();
    header.extend_from_slice(&[0xff; 32]);
    fs::write(&corrupt, header).unwrap();
    let error = import_sqlite_file(&corrupt, dir.path().join("out2.store")).unwrap_err();
    assert_eq!(error.adapter_code(), Some("corrupt_input"));
}

#[cfg(unix)]
#[test]
fn read_only_destination_is_typed() {
    use std::os::unix::fs::PermissionsExt;
    let dir = TempDir::new().unwrap();
    let source = copy_fixture(&dir, "source.db");
    let dest = dir.path().join("locked.store");
    fs::write(
        &dest,
        b"{\"format_version\":2,\"tables\":{},\"indexes\":{}}",
    )
    .unwrap();
    let mut perms = fs::metadata(&dest).unwrap().permissions();
    perms.set_mode(0o444);
    fs::set_permissions(&dest, perms).unwrap();
    let error = import_sqlite_file(&source, &dest).unwrap_err();
    let mut perms = fs::metadata(&dest).unwrap().permissions();
    perms.set_mode(0o644);
    let _ = fs::set_permissions(&dest, perms);
    assert!(matches!(
        error,
        dactyl_db::DactylError::Adapter {
            kind: AdapterErrorKind::ReadOnly,
            ..
        }
    ));
}

#[test]
fn ordinary_open_still_rejects_sqlite_header() {
    let dir = TempDir::new().unwrap();
    let source = copy_fixture(&dir, "still-sqlite.db");
    let error = match Connection::open(DatastoreRoute::sqlite(source.to_string_lossy())) {
        Ok(_) => panic!("sqlite file opened as a Dactyl store"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        dactyl_db::DactylError::Adapter {
            kind: AdapterErrorKind::Capability,
            ..
        }
    ));
}

#[test]
fn imported_store_honors_read_only_and_atomic() {
    let dir = TempDir::new().unwrap();
    let source = copy_fixture(&dir, "source.db");
    let dest = dir.path().join("dactyl.store");
    import_sqlite_file(&source, &dest).unwrap();
    let readonly = Connection::open_with_options(
        DatastoreRoute::sqlite(dest.to_string_lossy()),
        OpenOptions {
            access_mode: AccessMode::ReadOnly,
            lock_timeout: std::time::Duration::from_millis(5),
        },
    )
    .unwrap();
    assert_eq!(readonly.access_mode(), AccessMode::ReadOnly);
    assert_eq!(readonly.read("select id from tasks", &[]).unwrap().len(), 1);
    let error = readonly
        .write(
            "delete from tasks where id = $1",
            &[Parameter::Text("task-1".into())],
        )
        .unwrap_err();
    assert!(matches!(
        error,
        dactyl_db::DactylError::Adapter {
            kind: AdapterErrorKind::ReadOnly,
            ..
        }
    ));

    let db = Connection::open(DatastoreRoute::sqlite(dest.to_string_lossy())).unwrap();
    db.atomic(&[
        Operation::write(
            "insert into tasks (id, hash, title) values ($1, $2, $3)",
            vec![
                Parameter::Text("task-2".into()),
                Parameter::Text("def".into()),
                Parameter::Text("second".into()),
            ],
        ),
        Operation::write(
            "insert into missing (id) values ($1)",
            vec![Parameter::Text("nope".into())],
        ),
    ])
    .unwrap_err();
    assert_eq!(db.read("select id from tasks", &[]).unwrap().len(), 1);
}
