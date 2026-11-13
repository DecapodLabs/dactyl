//! Private test inputs and exclusive receipts; never print OS/JSON errors.
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

pub type Check<T> = Result<T, &'static str>;
const LIMIT: u64 = 64 * 1024;

pub fn read(path: &Path) -> Check<Vec<u8>> {
    let before = std::fs::symlink_metadata(path).map_err(|_| "fixture unavailable")?;
    if !before.is_file() {
        return Err("fixture must be a protected regular file");
    }
    let file = File::open(path).map_err(|_| "fixture unavailable")?;
    let after = file.metadata().map_err(|_| "fixture metadata unavailable")?;
    if !after.is_file() || after.len() > LIMIT {
        return Err("fixture must be a bounded regular file");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if before.dev() != after.dev() || before.ino() != after.ino() {
            return Err("fixture changed while opening");
        }
        if after.permissions().mode() & 0o077 != 0 {
            return Err("fixture must not allow group or other access");
        }
        let mut bytes = Vec::new();
        file.take(LIMIT + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| "fixture read failed")?;
        if bytes.len() as u64 > LIMIT {
            return Err("fixture exceeds size limit");
        }
        Ok(bytes)
    }
    #[cfg(not(unix))]
    Err("protected fixture permission checks require Unix")
}

pub fn reserve(path: &Path) -> Check<File> {
    if !path.is_absolute() || path.file_name().is_none() {
        return Err("receipt must name a new absolute file");
    }
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path).map_err(|_| "receipt unavailable or already exists")
}

pub fn write(file: &mut File, value: &serde_json::Value) -> Check<()> {
    let bytes = serde_json::to_vec_pretty(value).map_err(|_| "receipt serialization failed")?;
    file.seek(SeekFrom::Start(0))
        .and_then(|_| file.set_len(0))
        .and_then(|_| file.write_all(&bytes))
        .and_then(|_| file.write_all(b"\n"))
        .and_then(|_| file.sync_all())
        .map_err(|_| "receipt write failed")
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::{symlink, PermissionsExt};

    #[test]
    fn protected_inputs_reject_links_public_files_and_oversize() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("private");
        std::fs::write(&path, b"synthetic-private-input").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(read(&path).unwrap(), b"synthetic-private-input");
        let link = dir.path().join("link");
        symlink(&path, &link).unwrap();
        assert!(read(&link).is_err());
        assert!(read(dir.path()).is_err());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
        assert!(read(&path).is_err());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::write(&path, vec![0; LIMIT as usize + 1]).unwrap();
        assert!(read(&path).is_err());
    }

    #[test]
    fn receipts_are_private_exclusive_and_do_not_follow_links() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("receipt");
        let mut file = reserve(&path).unwrap();
        write(&mut file, &serde_json::json!({"completed": 1})).unwrap();
        assert_eq!(file.metadata().unwrap().permissions().mode() & 0o077, 0);
        assert!(reserve(&path).is_err());
        let link = dir.path().join("receipt-link");
        symlink(&path, &link).unwrap();
        assert!(reserve(&link).is_err());
        assert!(reserve(Path::new("relative")).is_err());
    }
}
