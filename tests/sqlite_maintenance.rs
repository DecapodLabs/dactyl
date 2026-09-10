#![cfg(feature = "sqlite")]

use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use dactyl_db::{
    AdapterErrorKind, Connection, DatastoreRoute, RecoveryJournalMode, RecoveryOptions,
};
use tempfile::TempDir;

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn sqlite(path: &Path) -> Connection {
    if !path.exists() {
        fs::File::create(path).expect("create SQLite test file");
    }
    Connection::open(DatastoreRoute::sqlite(path.to_string_lossy()))
        .expect("SQLite connection should open")
}

#[test]
fn integrity_and_online_backup_cover_wal_sidecars_and_metadata() {
    let directory = TempDir::new().unwrap();
    let source_path = directory.path().join("source.db");
    let backup_path = directory.path().join("backup.db");
    let db = sqlite(&source_path);

    db.write(
        "create table records (id integer primary key, name text, payload blob)",
        &[],
    )
    .unwrap();
    db.write("pragma journal_mode = WAL", &[]).unwrap();
    db.write("pragma wal_autocheckpoint = 0", &[]).unwrap();
    db.write("pragma user_version = 37", &[]).unwrap();
    db.write("pragma application_id = 4242", &[]).unwrap();
    db.write(
        "insert into records (name, payload) values ($1, $2)",
        &["live".into(), vec![1_u8, 2, 3].into()],
    )
    .unwrap();

    let health = db.verify_integrity().unwrap();
    assert_eq!(health.journal_mode.to_ascii_lowercase(), "wal");
    assert_eq!(health.user_version, 37);
    assert_eq!(health.application_id, 4242);

    let sibling = sqlite(&source_path);
    let backup = db.backup(&backup_path).unwrap();
    drop(sibling);
    assert_eq!(backup.source_journal_mode.to_ascii_lowercase(), "wal");
    assert_eq!(backup.destination_journal_mode.to_ascii_lowercase(), "wal");
    assert!(backup.bytes > 0);
    assert!(backup_path.is_file());
    assert!(!sidecar(&backup_path, "-wal").exists());
    assert!(!sidecar(&backup_path, "-shm").exists());

    let restored = sqlite(&backup_path);
    let backup_health = restored.verify_integrity().unwrap();
    assert_eq!(backup_health.journal_mode.to_ascii_lowercase(), "wal");
    assert_eq!(backup_health.user_version, 37);
    assert_eq!(backup_health.application_id, 4242);
    assert_eq!(
        restored
            .read("select count(*) as count from records", &[])
            .unwrap()
            .as_slice()[0]
            .get_int("count")
            .unwrap(),
        1
    );
}

#[test]
fn online_backup_reports_bounded_lock_contention() {
    let directory = TempDir::new().unwrap();
    let source_path = directory.path().join("locked.db");
    let backup_path = directory.path().join("locked-backup.db");
    let blocker = sqlite(&source_path);
    blocker
        .write("create table records (id integer primary key)", &[])
        .unwrap();
    blocker.write("begin exclusive", &[]).unwrap();
    let db = sqlite(&source_path);
    let error = db
        .backup(&backup_path)
        .expect_err("backup should honor the bounded busy timeout");
    assert!(matches!(
        error.adapter_kind(),
        Some(AdapterErrorKind::Timeout | AdapterErrorKind::Busy | AdapterErrorKind::Locked)
    ));
    blocker.write("rollback", &[]).unwrap();
}

#[test]
fn damaged_secondary_index_reindexes_badly_but_logical_recovery_succeeds() {
    let directory = TempDir::new().unwrap();
    let source_path = directory.path().join("corrupt.db");
    let archive_path = directory.path().join("corrupt.before-recovery.db");
    let page_size;
    let root_page;
    {
        let db = sqlite(&source_path);
        db.write(
            "create table records (id integer primary key, name text not null, payload blob)",
            &[],
        )
        .unwrap();
        db.write("create unique index records_name on records(name)", &[])
            .unwrap();
        db.write("pragma journal_mode = WAL", &[]).unwrap();
        db.write("pragma user_version = 9", &[]).unwrap();
        db.write("pragma application_id = 99", &[]).unwrap();
        for (name, payload) in [("one", vec![1_u8]), ("two", vec![2_u8])] {
            db.write(
                "insert into records (name, payload) values ($1, $2)",
                &[name.into(), payload.into()],
            )
            .unwrap();
        }
        db.write("pragma wal_checkpoint(TRUNCATE)", &[]).unwrap();
        page_size = db.read("pragma page_size", &[]).unwrap().as_slice()[0]
            .get_int(0)
            .unwrap();
        root_page = db
            .read(
                "select rootpage from sqlite_schema where type = 'index' and name = 'records_name'",
                &[],
            )
            .unwrap()
            .as_slice()[0]
            .get_int(0)
            .unwrap();
    }
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&source_path)
            .unwrap();
        file.seek(SeekFrom::Start((root_page as u64 - 1) * page_size as u64))
            .unwrap();
        file.write_all(&[0]).unwrap();
        file.sync_all().unwrap();
    }

    let mut db = sqlite(&source_path);
    let error = db.verify_integrity().unwrap_err();
    assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Corrupt));
    assert_eq!(error.adapter_code(), Some("corrupt_database"));
    let reindex = db.write("reindex records_name", &[]).unwrap_err();
    assert_eq!(reindex.adapter_kind(), Some(AdapterErrorKind::Corrupt));

    let result = db
        .recover_from_dump_reload(RecoveryOptions::new(
            archive_path.to_string_lossy(),
            RecoveryJournalMode::Delete,
        ))
        .unwrap();
    assert_eq!(result.journal_mode, RecoveryJournalMode::Delete);
    assert!(archive_path.is_file());
    assert_eq!(result.user_version, 9);
    assert_eq!(result.application_id, 99);

    let health = db.verify_integrity().unwrap();
    assert_eq!(health.journal_mode.to_ascii_lowercase(), "delete");
    assert_eq!(health.user_version, 9);
    assert_eq!(health.application_id, 99);
    let rows = db
        .read("select name, payload from records order by id", &[])
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows.as_slice()[0].get_str("name").unwrap(), "one");
    assert_eq!(rows.as_slice()[1].get_blob("payload").unwrap(), &[2][..]);
    assert!(!sidecar(&source_path, "-wal").exists());
}

#[test]
fn recovery_refuses_open_sibling_connections_and_existing_archive_without_mutation() {
    let directory = TempDir::new().unwrap();
    let source_path = directory.path().join("source.db");
    let archive_path = directory.path().join("archive.db");
    let mut primary = sqlite(&source_path);
    primary
        .write(
            "create table records (id integer primary key, name text)",
            &[],
        )
        .unwrap();
    let sibling = sqlite(&source_path);
    let error = primary
        .recover_from_dump_reload(RecoveryOptions::new(
            archive_path.to_string_lossy(),
            RecoveryJournalMode::Delete,
        ))
        .unwrap_err();
    assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Conflict));
    assert_eq!(error.adapter_code(), Some("open_connections"));
    drop(sibling);

    fs::write(&archive_path, b"operator-owned archive").unwrap();
    let error = primary
        .recover_from_dump_reload(RecoveryOptions::new(
            archive_path.to_string_lossy(),
            RecoveryJournalMode::Delete,
        ))
        .unwrap_err();
    assert_eq!(error.adapter_kind(), Some(AdapterErrorKind::Conflict));
    assert_eq!(error.adapter_code(), Some("path_exists"));
    assert_eq!(fs::read(&archive_path).unwrap(), b"operator-owned archive");
    assert_eq!(
        primary.read("select name from records", &[]).unwrap().len(),
        0
    );
}
