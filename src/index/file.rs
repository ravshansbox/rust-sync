//! Reading and hashing regular files, and the current time in nanoseconds.

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

/// What `stat` reads from a regular file: size in bytes, mtime in nanoseconds and permission bits.
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

/// Nanoseconds since the Unix epoch, negative for times before it.
pub fn nanos(t: SystemTime) -> i64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_nanos() as i64,
        Err(e) => -(e.duration().as_nanos() as i64),
    }
}

/// The current time in nanoseconds since the Unix epoch.
pub fn now() -> i64 {
    nanos(SystemTime::now())
}

/// BLAKE3 hash of a file's contents and the number of bytes read.
pub fn hash_file(p: &Path) -> std::io::Result<([u8; 32], u64)> {
    let mut h = blake3::Hasher::new();
    let size = std::io::copy(&mut std::fs::File::open(p)?, &mut h)?;
    Ok((*h.finalize().as_bytes(), size))
}

#[cfg(test)]
mod tests;
