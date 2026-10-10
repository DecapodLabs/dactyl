# Secure SQLite recovery in shared directories

`Connection::recover_from_dump_reload(RecoveryOptions::new(archive,
RecoveryJournalMode::Delete))` remains an explicit, quiesced maintenance action.
Ordinary open/read/write and online backup do not opt into this policy.

## Supported filesystem contract

The secure recovery implementation currently supports Linux and Android using
the host SQLite Unix VFS and a filesystem with atomic no-replace renames and
reliable file/directory sync. Other targets return the typed capability error
`secure_recovery_unsupported` before rebuilding or renaming user data. This is
a fail-closed boundary, not a cross-platform ACL implementation.

A `0660` database in a trusted-group `2770` directory is supported. Dactyl does
not chmod the existing database, archive, sidecars or shared directory. A fresh
private child of the database directory supplies same-filesystem staging; there
is no caller-selected staging path and no system-temporary-directory fallback.
The original owner, group and permission bits are preserved on the rebuilt
inode. If the process cannot reproduce ownership, recovery fails with
`recovery_ownership_unavailable` before disturbing the original. Executable or
special database mode bits are rejected with `unsafe_recovery_mode`.

Access policy beyond mode/owner/group is deliberately bounded. Source access
ACLs, inherited default ACLs, system/trusted access attributes and MAC labels
(such as SELinux/Smack) return `recovery_metadata_unsupported`. An ordinary
POSIX access ACL on the unchanged parent, unrelated `user.*` attributes and
setgid shared directories are accepted. Non-policy user attributes are not
copied to the new inode; all attributes remain on the original archived inode.
Oversized attribute lists fail closed rather than being incompletely inspected.
No permissions or security policy are changed to make an unsupported case pass.

## Private creation and activation

1. Dactyl exclusively creates a new `.dactyl-recovery-*` directory with mode
   no broader than `0700` beneath the active database's parent, including under
   umask `000`. Restrictive umasks that remove required owner permissions fail
   closed. Existing
   names, including dangling symlinks, are skipped, never reused.
2. Before writing database contents, Dactyl exclusively creates `rebuilt.db`
   and `rebuilt.db-journal` with mode `0600`. Rebuilding uses SQLite PERSIST
   journaling so the pre-created private journal is retained across writes.
   The rebuilt database never uses WAL/SHM. SQLite's Unix VFS also creates its
   delete-on-close sort/temp files as `0600`; custom VFS implementations and
   non-POSIX permission emulation are outside this guarantee.
3. Schema, table data, indexes, views, triggers, `sqlite_sequence`, user version,
   application ID and integrity are rebuilt/validated through SQLite. The final
   journal mode is explicitly DELETE, removing the staging journal. Only then
   are the original mode bits applied to the new inode, still inside `0700`.
4. Dactyl syncs the new file and staging directory, closes the old connection,
   preserves the original inode and surviving WAL/SHM/rollback-journal sidecars
   at unused archive names, and syncs the archive directory.
5. Atomic no-replace rename activates the single rebuilt file, then both affected
   directories are synced before reopening it. A competing file or symlink is
   never overwritten. The original/archive ownership, mode and inode remain
   unchanged. Normal shared access uses the same intended mode/group afterward.

The process-global umask is never changed. The tests set `umask 000` only in
isolated subprocesses and inspect creation checkpoints and a populated SQLite
rollback journal before permission restoration.

## Failure, interruption and ownership boundaries

Pre-activation failures leave the original in place or attempt restoration from
its archive. If restoration is blocked, the original remains preserved in the
archive and `recovery_rollback_failed` reports the failure. Cleanup removes only
identity-matched operation-owned staging files and the empty staging directory;
it never recursively removes unknown contents. Symlink/hardlink targets, changed
source/staging identities and archive/sidecar collisions fail closed. The adapter
records the opened source inode so a replacement before the recovery call is
also refused.

After activation, sync/reopen failures attempt to move the new inode back into
private staging as `failed.db`, restrict that new inode to `0600`, and restore
and reopen the original. A diagnostic-sync failure still attempts restoration.
Retained diagnostics stay beneath `0700` and their location is included in the
error. Unexpected active sidecars cannot be attributed to the operation, so they
are left untouched and rollback reports `recovery_rollback_failed`; the archived
original remains available. A rollback destination collision likewise preserves
the other writer's entry and reports the blocked restoration.

A process crash is not an automatic repair trigger. Before archival the original
is active; between archival and activation it is at the archive path; after
activation the original remains archived. Private staging may survive. On a
power loss, recoverability is bounded by the host filesystem's sync guarantees.
Operators must inspect the reported error and preserved paths before resuming;
Dactyl does not automatically delete old staging or guess which file to promote.

Callers must quiesce other SQLite connections **and filesystem name, content and
metadata writers from connection open through completion**. Same-process sibling
handles are refused; Dactyl cannot coordinate arbitrary external writers or
privileged/same-user attackers. Identity checks and no-clobber transitions detect
replacement attempts at the checked boundaries; they do not claim a race-free
sandbox against a malicious directory writer between system calls. Shared access
is intentional, but all writers must participate in this maintenance window.

## Consumer integration

The public `RecoveryOptions` shape is unchanged. Consumers call this supported
primitive rather than editing SQLite files. Decapod's private-directory recovery
guard remains in place until a released Dactyl version is explicitly integrated
and the consumer policy/proofs are updated separately. This change performs no
consumer modification, release, merge or deployment.

## Proof surfaces

- Existing `sqlite_maintenance` regressions: damaged-index recovery, WAL backup,
  bounded locking, open-sibling refusal and existing archive preservation
- Recovery unit tests: `0600`/`0640`/`0660` mode, group and original inode custody
  in a `2770` directory; permissive-umask creation; ACL rejection; stale source
  identities; symlink, sidecar, archive and staging-name collisions
- Failure injection before/after rebuild, validation boundary, sync boundary,
  archival, activation, reopen and diagnostic sync; ownership-bounded cleanup
- Subprocess abrupt exit at seven stages, checking private leftover staging and
  readable original data in the source or archive path

Unix VFS behavior is described by SQLite's [temporary-file documentation](https://www.sqlite.org/tempfiles.html)
and [Unix VFS implementation](https://github.com/sqlite/sqlite/blob/master/src/os_unix.c).
