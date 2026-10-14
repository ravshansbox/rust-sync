use super::*;

use crate::paths::test_dir;

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
