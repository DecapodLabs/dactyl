use super::*;
use std::os::unix::fs::symlink;
use std::process::Command;
use tempfile::TempDir;

fn fixture(directory: &Path, mode: u32) -> (SqliteAdapter, PathBuf, PathBuf) {
    let source = directory.join("source.db");
    let archive = directory.join("archive.db");
    let db =
        SqliteAdapter::open_with_options(source.to_str().unwrap(), OpenOptions::default()).unwrap();
    {
        let connection = db.connection().unwrap();
        exec_sql(&connection, "CREATE TABLE records(id INTEGER PRIMARY KEY, name TEXT); INSERT INTO records VALUES (1, 'private data'); PRAGMA user_version=9; PRAGMA application_id=99").unwrap();
    }
    fs::set_permissions(&source, Permissions::from_mode(mode)).unwrap();
    (db, source, archive)
}
fn run(db: &mut SqliteAdapter, archive: &Path) -> Result<RecoveryResult, DactylError> {
    recover(
        db,
        &RecoveryOptions::new(archive.to_str().unwrap(), RecoveryJournalMode::Delete),
    )
}
fn mode(path: &Path) -> u32 {
    fs::symlink_metadata(path).unwrap().mode() & 0o7777
}
fn stages(directory: &Path) -> Vec<PathBuf> {
    fs::read_dir(directory)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with(".dactyl-recovery-")
        })
        .collect()
}
fn readable(db: &SqliteAdapter) {
    let connection = db.connection().unwrap();
    let rows = query_rows(&connection, "SELECT name FROM records", &[]).unwrap();
    assert_eq!(row_string(&rows.as_slice()[0], 0).unwrap(), "private data");
}
struct ResetHook;
impl Drop for ResetHook {
    fn drop(&mut self) {
        HOOK.with(|hook| *hook.borrow_mut() = None);
    }
}
fn hook(callback: impl FnMut(Phase, &mut Stage) -> Result<(), DactylError> + 'static) -> ResetHook {
    HOOK.with(|hook| *hook.borrow_mut() = Some(Box::new(callback)));
    ResetHook
}
fn injected() -> DactylError {
    DactylError::adapter_with_code(
        AdapterErrorKind::Storage,
        "injected_failure",
        "injected recovery failure",
    )
}

#[test]
fn recovery_preserves_shared_and_private_modes_ownership_and_archives() {
    for source_mode in [0o600, 0o640, 0o660] {
        let directory = TempDir::new().unwrap();
        fs::set_permissions(directory.path(), Permissions::from_mode(0o2770)).unwrap();
        let (mut db, source, archive) = fixture(directory.path(), source_mode);
        let before = fs::metadata(&source).unwrap();
        let bytes = fs::read(&source).unwrap();
        run(&mut db, &archive).unwrap();
        assert_eq!(mode(&source), source_mode);
        assert_eq!(mode(&archive), source_mode);
        assert_eq!(mode(directory.path()), 0o2770);
        let active = fs::metadata(&source).unwrap();
        let original = fs::metadata(&archive).unwrap();
        assert_eq!((active.uid(), active.gid()), (before.uid(), before.gid()));
        assert_eq!(
            (original.uid(), original.gid(), original.ino()),
            (before.uid(), before.gid(), before.ino())
        );
        assert_ne!(active.ino(), original.ino());
        assert_eq!(fs::read(&archive).unwrap(), bytes);
        readable(&db);
        exec_sql(
            &db.connection().unwrap(),
            "INSERT INTO records VALUES (2, 'shared write')",
        )
        .unwrap();
        drop(db);
        let reopened =
            SqliteAdapter::open_with_options(source.to_str().unwrap(), OpenOptions::default())
                .unwrap();
        assert_eq!(
            query_rows(
                &reopened.connection().unwrap(),
                "SELECT name FROM records",
                &[]
            )
            .unwrap()
            .len(),
            2
        );
        assert!(stages(directory.path()).is_empty());
    }
}

#[test]
fn permissive_umask_isolated_child() {
    if std::env::var_os("DACTYL_RECOVERY_UMASK_CHILD").is_none() {
        return;
    }
    let directory = TempDir::new().unwrap();
    fs::set_permissions(directory.path(), Permissions::from_mode(0o2770)).unwrap();
    let (mut db, source, archive) = fixture(directory.path(), 0o660);
    let _hook = hook(|phase, stage| {
        if matches!(phase, Phase::Created | Phase::Rebuilding | Phase::Rebuilt) {
            assert_eq!(mode(&stage.path) & 0o777, 0o700);
            assert_eq!(mode(&stage.rebuilt()), 0o600);
            let journal = sidecar_path(&stage.rebuilt(), "-journal");
            if phase != Phase::Rebuilt {
                assert_eq!(mode(&journal), 0o600);
                stage
                    .journal_identity
                    .as_ref()
                    .unwrap()
                    .check(&journal)
                    .unwrap();
                if phase == Phase::Rebuilding {
                    assert!(fs::metadata(journal).unwrap().len() > 0);
                }
            }
            assert!(!sidecar_path(&stage.rebuilt(), "-wal").exists());
            assert!(!sidecar_path(&stage.rebuilt(), "-shm").exists());
        }
        Ok(())
    });
    run(&mut db, &archive).unwrap();
    assert_eq!(mode(&source), 0o660);
    assert_eq!(mode(&archive), 0o660);
}

#[test]
fn restrictive_creation_survives_umask_zero() {
    let status = Command::new("sh").arg("-c").arg("umask 000; exec \"$1\" --exact adapter::sqlite::recovery::tests::permissive_umask_isolated_child --nocapture")
        .arg("dactyl-recovery-test").arg(std::env::current_exe().unwrap())
        .env("DACTYL_RECOVERY_UMASK_CHILD", "1").status().unwrap();
    assert!(status.success());
}

#[test]
fn failures_restore_original_and_keep_diagnostics_private() {
    for failed_phase in [
        Phase::Created,
        Phase::Rebuilding,
        Phase::Rebuilt,
        Phase::Synced,
        Phase::Archived,
        Phase::Activated,
        Phase::Reopen,
    ] {
        let directory = TempDir::new().unwrap();
        let (mut db, source, archive) = fixture(directory.path(), 0o660);
        let original = fs::metadata(&source).unwrap();
        let _hook = hook(move |phase, _| {
            if phase == failed_phase {
                Err(injected())
            } else {
                Ok(())
            }
        });
        assert_eq!(
            run(&mut db, &archive).unwrap_err().adapter_code(),
            Some("injected_failure")
        );
        assert_eq!(fs::metadata(&source).unwrap().ino(), original.ino());
        assert_eq!(mode(&source), 0o660);
        assert!(!archive.exists());
        readable(&db);
        let retained = stages(directory.path());
        if matches!(failed_phase, Phase::Activated | Phase::Reopen) {
            assert_eq!(retained.len(), 1);
            assert_eq!(mode(&retained[0]) & 0o777, 0o700);
            assert_eq!(mode(&retained[0].join("failed.db")), 0o600);
        } else {
            assert!(retained.is_empty(), "{failed_phase:?}: {retained:?}");
        }
    }
}

#[test]
fn archive_and_sidecar_symlink_collisions_are_never_followed() {
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let directory = TempDir::new().unwrap();
        let (mut db, source, archive) = fixture(directory.path(), 0o600);
        let victim = directory.path().join("absent-victim");
        let collision = sidecar_path(&archive, suffix);
        symlink(&victim, &collision).unwrap();
        let before = fs::read(&source).unwrap();
        assert_eq!(
            run(&mut db, &archive).unwrap_err().adapter_code(),
            Some("path_exists")
        );
        assert_eq!(fs::read(&source).unwrap(), before);
        assert!(!victim.exists());
        assert!(fs::symlink_metadata(collision)
            .unwrap()
            .file_type()
            .is_symlink());
    }
}

#[test]
fn source_sidecar_and_parent_symlinks_are_rejected() {
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let directory = TempDir::new().unwrap();
        let (mut db, source, archive) = fixture(directory.path(), 0o600);
        let victim = directory.path().join("victim");
        let target = sidecar_path(&source, suffix);
        if suffix.is_empty() {
            fs::rename(&target, &victim).unwrap();
        } else {
            fs::write(&victim, b"unrelated").unwrap();
        }
        symlink(&victim, &target).unwrap();
        let before = fs::read(&victim).unwrap();
        assert_eq!(
            run(&mut db, &archive).unwrap_err().adapter_code(),
            Some("unsafe_recovery_file")
        );
        assert_eq!(fs::read(&victim).unwrap(), before);
        assert!(!archive.exists());
    }
    let directory = TempDir::new().unwrap();
    let real = directory.path().join("real");
    fs::create_dir(&real).unwrap();
    let link = directory.path().join("link");
    symlink(&real, &link).unwrap();
    let (mut db, _, archive) = fixture(&link, 0o600);
    assert_eq!(
        run(&mut db, &archive).unwrap_err().adapter_code(),
        Some("unsafe_recovery_directory")
    );
}

#[test]
fn collision_during_activation_preserves_competing_file_and_archive() {
    let directory = TempDir::new().unwrap();
    let (mut db, source, archive) = fixture(directory.path(), 0o600);
    let original = fs::read(&source).unwrap();
    let collision = source.clone();
    let _hook = hook(move |phase, _| {
        if phase == Phase::Archived {
            fs::write(&collision, b"other writer").unwrap();
        }
        Ok(())
    });
    assert_eq!(
        run(&mut db, &archive).unwrap_err().adapter_code(),
        Some("recovery_rollback_failed")
    );
    assert_eq!(fs::read(&source).unwrap(), b"other writer");
    assert_eq!(fs::read(&archive).unwrap(), original);
}

#[test]
fn archive_collision_after_rebuild_does_not_overwrite() {
    let directory = TempDir::new().unwrap();
    let (mut db, source, archive) = fixture(directory.path(), 0o600);
    let original = fs::read(&source).unwrap();
    let collision = archive.clone();
    let _hook = hook(move |phase, _| {
        if phase == Phase::Synced {
            fs::write(&collision, b"other archive").unwrap();
        }
        Ok(())
    });
    assert_eq!(
        run(&mut db, &archive).unwrap_err().adapter_code(),
        Some("path_exists")
    );
    assert_eq!(fs::read(&archive).unwrap(), b"other archive");
    assert_eq!(fs::read(&source).unwrap(), original);
    readable(&db);
}

#[test]
fn staging_replacement_and_unknown_contents_are_not_cleaned_up() {
    let directory = TempDir::new().unwrap();
    let (mut db, source, archive) = fixture(directory.path(), 0o600);
    let other = directory.path().join("unrelated");
    fs::create_dir(&other).unwrap();
    fs::write(other.join("rebuilt.db"), b"unrelated database").unwrap();
    let target = other.clone();
    let _hook = hook(move |phase, stage| {
        if phase == Phase::Rebuilt {
            fs::rename(&stage.path, stage.path.with_extension("moved")).unwrap();
            symlink(&target, &stage.path).unwrap();
        }
        Ok(())
    });
    assert_eq!(
        run(&mut db, &archive).unwrap_err().adapter_code(),
        Some("recovery_path_changed")
    );
    assert_eq!(
        fs::read(other.join("rebuilt.db")).unwrap(),
        b"unrelated database"
    );
    assert!(source.exists());
    assert!(!archive.exists());
}

#[test]
fn cleanup_leaves_replaced_journal_and_unowned_names_untouched() {
    let directory = TempDir::new().unwrap();
    let (mut db, _, archive) = fixture(directory.path(), 0o600);
    let _hook = hook(|phase, stage| {
        if phase == Phase::Created {
            let journal = sidecar_path(&stage.rebuilt(), "-journal");
            fs::rename(&journal, stage.path.join("original-journal")).unwrap();
            fs::write(&journal, b"unrelated journal").unwrap();
            fs::write(sidecar_path(&stage.rebuilt(), "-wal"), b"unrelated wal").unwrap();
            return Err(injected());
        }
        Ok(())
    });
    assert!(run(&mut db, &archive).is_err());
    let retained = stages(directory.path());
    assert_eq!(retained.len(), 1);
    assert_eq!(
        fs::read(retained[0].join("rebuilt.db-journal")).unwrap(),
        b"unrelated journal"
    );
    assert_eq!(
        fs::read(retained[0].join("rebuilt.db-wal")).unwrap(),
        b"unrelated wal"
    );
}

#[test]
fn interruption_child() {
    let Ok(root) = std::env::var("DACTYL_RECOVERY_INTERRUPT_ROOT") else {
        return;
    };
    let phase = std::env::var("DACTYL_RECOVERY_INTERRUPT_PHASE").unwrap();
    let (mut db, _, archive) = fixture(Path::new(&root), 0o660);
    let _hook = hook(move |current, _| {
        if format!("{current:?}") == phase {
            std::process::exit(73);
        }
        Ok(())
    });
    run(&mut db, &archive).unwrap();
    panic!("interruption checkpoint was not reached");
}

#[test]
fn abrupt_exit_leaves_private_staging_and_recoverable_original() {
    for phase in [
        Phase::Created,
        Phase::Rebuilding,
        Phase::Rebuilt,
        Phase::Synced,
        Phase::Archived,
        Phase::Activated,
        Phase::Reopen,
    ] {
        let directory = TempDir::new().unwrap();
        let status = Command::new("sh").arg("-c").arg("umask 000; exec \"$1\" --exact adapter::sqlite::recovery::tests::interruption_child --nocapture")
            .arg("dactyl-recovery-test").arg(std::env::current_exe().unwrap())
            .env("DACTYL_RECOVERY_INTERRUPT_ROOT", directory.path())
            .env("DACTYL_RECOVERY_INTERRUPT_PHASE", format!("{phase:?}")).status().unwrap();
        assert_eq!(status.code(), Some(73));
        let source = directory.path().join("source.db");
        let archive = directory.path().join("archive.db");
        let original = if archive.exists() { archive } else { source };
        let db =
            SqliteAdapter::open_with_options(original.to_str().unwrap(), OpenOptions::default())
                .unwrap();
        readable(&db);
        assert_eq!(mode(&original), 0o660);
        for stage in stages(directory.path()) {
            assert_eq!(mode(&stage) & 0o777, 0o700);
            for entry in fs::read_dir(&stage).unwrap() {
                let path = entry.unwrap().path();
                let allowed = if matches!(phase, Phase::Synced | Phase::Archived)
                    && path.file_name().unwrap() == "rebuilt.db"
                {
                    0o660
                } else {
                    0o600
                };
                assert_eq!(mode(&path), allowed, "{phase:?}: {}", path.display());
            }
        }
    }
}

#[test]
fn archive_must_not_alias_any_source_sidecar() {
    let directory = TempDir::new().unwrap();
    let (mut db, source, _) = fixture(directory.path(), 0o600);
    for suffix in SIDECARS {
        assert_eq!(
            run(&mut db, &sidecar_path(&source, suffix))
                .unwrap_err()
                .adapter_code(),
            Some("recovery_paths_must_share_parent")
        );
        readable(&db);
    }
}

#[test]
fn staging_name_collisions_are_skipped_without_touching_entries() {
    let directory = TempDir::new().unwrap();
    let (_db, source, _) = fixture(directory.path(), 0o600);
    let name = |nonce| {
        directory
            .path()
            .join(format!(".dactyl-recovery-{}-{nonce}", std::process::id()))
    };
    fs::write(name(42), b"occupied").unwrap();
    symlink(directory.path().join("absent"), name(43)).unwrap();
    let stage =
        Stage::create_with_nonce(&source, &fs::metadata(source.clone()).unwrap(), 42).unwrap();
    assert_eq!(stage.path, name(44));
    drop(stage);
    assert_eq!(fs::read(name(42)).unwrap(), b"occupied");
    assert!(fs::symlink_metadata(name(43))
        .unwrap()
        .file_type()
        .is_symlink());
    assert!(!name(44).exists());
}

#[test]
fn cross_filesystem_staging_is_rejected_before_directory_creation() {
    let directory = TempDir::new().unwrap();
    let (_db, source, _) = fixture(directory.path(), 0o600);
    let foreign = Path::new("/dev/shm");
    let metadata = fs::metadata(foreign).expect("Linux shared memory filesystem");
    if metadata.dev() == fs::metadata(directory.path()).unwrap().dev() {
        return;
    }
    let error = Stage::create_with_nonce(&source, &metadata, 10)
        .err()
        .unwrap();
    assert_eq!(error.adapter_code(), Some("recovery_cross_filesystem"));
    assert!(stages(directory.path()).is_empty());
}

#[test]
fn diagnostic_sync_failure_still_restores_original() {
    let directory = TempDir::new().unwrap();
    let (mut db, source, archive) = fixture(directory.path(), 0o660);
    let original = fs::metadata(&source).unwrap().ino();
    let _hook = hook(|phase, _| {
        if matches!(phase, Phase::Activated | Phase::Quarantined) {
            Err(injected())
        } else {
            Ok(())
        }
    });
    assert_eq!(
        run(&mut db, &archive).unwrap_err().adapter_code(),
        Some("recovery_rollback_failed")
    );
    assert_eq!(fs::metadata(&source).unwrap().ino(), original);
    readable(&db);
    assert!(!archive.exists());
    let retained = stages(directory.path());
    assert_eq!(mode(&retained[0].join("failed.db")), 0o600);
}

#[test]
fn extended_acl_is_rejected_without_changing_original_or_directory() {
    // Linux POSIX ACL xattr: version=2, owner rw, named fixture-owner uid r,
    // group r, mask r, other none. Avoid an external setfacl dependency.
    let mut acl = 2_u32.to_le_bytes().to_vec();
    for (tag, perms, id) in [
        (1_u16, 6_u16, u32::MAX),
        (2, 4, 1000),
        (4, 4, u32::MAX),
        (16, 4, u32::MAX),
        (32, 0, u32::MAX),
    ] {
        acl.extend_from_slice(&tag.to_le_bytes());
        acl.extend_from_slice(&perms.to_le_bytes());
        acl.extend_from_slice(&id.to_le_bytes());
    }
    for default_acl in [false, true] {
        let directory = TempDir::new().unwrap();
        let (mut db, source, archive) = fixture(directory.path(), 0o600);
        // Use the mapped owner UID even in a one-UID user namespace.
        acl[16..20].copy_from_slice(&fs::metadata(&source).unwrap().uid().to_le_bytes());
        let path = if default_acl {
            directory.path()
        } else {
            &source
        };
        let name = if default_acl {
            "system.posix_acl_default"
        } else {
            "system.posix_acl_access"
        };
        match rustix::fs::setxattr(path, name, &acl, rustix::fs::XattrFlags::empty()) {
            Ok(()) => {}
            Err(rustix::io::Errno::OPNOTSUPP) => continue,
            Err(error) => panic!("cannot create ACL fixture: {error}"),
        }
        let before = fs::read(&source).unwrap();
        let original_mode = mode(&source);
        let directory_mode = mode(directory.path());
        assert_eq!(
            run(&mut db, &archive).unwrap_err().adapter_code(),
            Some("recovery_metadata_unsupported")
        );
        assert_eq!(fs::read(&source).unwrap(), before);
        assert_eq!(mode(&source), original_mode);
        assert_eq!(mode(directory.path()), directory_mode);
        assert!(stages(directory.path()).is_empty());
        assert!(!archive.exists());
    }
}

#[test]
fn changed_source_inode_is_rejected_before_rebuild() {
    let directory = TempDir::new().unwrap();
    let (mut db, source, archive) = fixture(directory.path(), 0o600);
    let old = directory.path().join("old.db");
    fs::rename(&source, &old).unwrap();
    fs::write(&source, b"replacement belonging to another writer").unwrap();
    assert_eq!(
        run(&mut db, &archive).unwrap_err().adapter_code(),
        Some("recovery_path_changed")
    );
    assert_eq!(
        fs::read(&source).unwrap(),
        b"replacement belonging to another writer"
    );
    assert!(old.exists());
    assert!(!archive.exists());
    assert!(stages(directory.path()).is_empty());
}

#[test]
fn special_mode_bits_are_rejected_without_chmod() {
    let directory = TempDir::new().unwrap();
    let (mut db, source, archive) = fixture(directory.path(), 0o760);
    assert_eq!(
        run(&mut db, &archive).unwrap_err().adapter_code(),
        Some("unsafe_recovery_mode")
    );
    assert_eq!(mode(&source), 0o760);
    assert!(!archive.exists());
}

#[test]
fn unrelated_user_attributes_do_not_block_shared_recovery() {
    let directory = TempDir::new().unwrap();
    fs::set_permissions(directory.path(), Permissions::from_mode(0o2770)).unwrap();
    let (mut db, source, archive) = fixture(directory.path(), 0o660);
    for path in [&source, directory.path()] {
        rustix::fs::setxattr(
            path,
            "user.dactyl-test",
            b"non-policy annotation",
            rustix::fs::XattrFlags::empty(),
        )
        .unwrap();
    }
    run(&mut db, &archive).unwrap();
    assert_eq!(mode(&source), 0o660);
    assert_eq!(mode(directory.path()), 0o2770);
    let mut value = [0_u8; 64];
    let length = rustix::fs::getxattr(&archive, "user.dactyl-test", &mut value).unwrap();
    assert_eq!(&value[..length], b"non-policy annotation");
    readable(&db);
}
