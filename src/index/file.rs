use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct Stat {
    pub size: u64,
    pub mtime: i64,
    pub mode: u32,
}

/// Size, mtime and permissions of a regular file. `None` if missing or not a regular file.
pub fn stat(p: &Path) -> Option<Stat> {
    let m = std::fs::symlink_metadata(p).ok()?;
    if !m.file_type().is_file() {
        return None;
    }
    Some(Stat { size: m.len(), mtime: nanos(m.modified().ok()?), mode: m.permissions().mode() & 0o777 })
}

pub fn nanos(t: SystemTime) -> i64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_nanos() as i64,
        Err(e) => -(e.duration().as_nanos() as i64),
    }
}

pub fn now() -> i64 {
    nanos(SystemTime::now())
}

pub fn hash_file(p: &Path) -> std::io::Result<([u8; 32], u64)> {
    let mut h = blake3::Hasher::new();
    let size = std::io::copy(&mut std::fs::File::open(p)?, &mut h)?;
    Ok((*h.finalize().as_bytes(), size))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_dir(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("rust-sync-{name}-{}", std::process::id()))
    }

    #[test]
    fn nanos_is_signed_offset_from_unix_epoch() {
        let d = std::time::Duration::new(3, 5);
        assert_eq!(nanos(UNIX_EPOCH), 0);
        assert_eq!(nanos(UNIX_EPOCH + d), 3_000_000_005);
        assert_eq!(nanos(UNIX_EPOCH - d), -3_000_000_005);
    }

    #[test]
    fn stat_reports_regular_files_only() {
        let dir = test_dir("stat");
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("f");
        std::fs::write(&file, b"hello").unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o640)).unwrap();
        let link = dir.join("l");
        std::os::unix::fs::symlink(&file, &link).unwrap();
        let s = stat(&file).unwrap();
        assert_eq!((s.size, s.mode), (5, 0o640));
        assert_eq!(s.mtime, nanos(std::fs::metadata(&file).unwrap().modified().unwrap()));
        assert!(stat(&dir).is_none());
        assert!(stat(&link).is_none());
        assert!(stat(&dir.join("missing")).is_none());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn stat_drops_setuid_setgid_and_sticky_bits() {
        let dir = test_dir("stat-special");
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("f");
        std::fs::write(&file, b"x").unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o7755)).unwrap();
        assert_eq!(std::fs::metadata(&file).unwrap().permissions().mode() & 0o7777, 0o7755);
        assert_eq!(stat(&file).unwrap().mode, 0o755);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn hash_file_matches_blake3_across_buffer_boundary_and_fails_for_missing_file() {
        let dir = test_dir("hash");
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("f");
        let data: Vec<u8> = (0..150_000u32).map(|i| (i % 251) as u8).collect();
        std::fs::write(&file, &data).unwrap();
        assert_eq!(hash_file(&file).unwrap(), (*blake3::hash(&data).as_bytes(), data.len() as u64));
        std::fs::write(&file, b"").unwrap();
        assert_eq!(hash_file(&file).unwrap(), (*blake3::hash(b"").as_bytes(), 0));
        assert!(hash_file(&dir.join("missing")).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
