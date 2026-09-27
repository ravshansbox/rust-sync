//! What we know about each synced file, and how to compare two versions.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

/// Version vector: for each node (short ID), how many changes it has made.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct Vv(pub Vec<(u64, u64)>);

#[derive(Debug, PartialEq, Eq)]
pub enum Order {
    Equal,
    Newer,
    Older,
    Concurrent,
}

impl Vv {
    fn get(&self, node: u64) -> u64 {
        self.0.iter().find(|(n, _)| *n == node).map_or(0, |(_, c)| *c)
    }

    pub fn bumped(&self, node: u64) -> Vv {
        let mut v = self.clone();
        match v.0.iter_mut().find(|(n, _)| *n == node) {
            Some((_, c)) => *c += 1,
            None => v.0.push((node, 1)),
        }
        v.0.sort_unstable();
        v
    }

    pub fn merged(&self, other: &Vv) -> Vv {
        let mut v = self.clone();
        for &(n, c) in &other.0 {
            match v.0.iter_mut().find(|(m, _)| *m == n) {
                Some((_, mine)) => *mine = (*mine).max(c),
                None => v.0.push((n, c)),
            }
        }
        v.0.sort_unstable();
        v
    }

    /// How `self` relates to `other`.
    pub fn compare(&self, other: &Vv) -> Order {
        let (mut newer, mut older) = (false, false);
        for &(n, _) in self.0.iter().chain(&other.0) {
            let (a, b) = (self.get(n), other.get(n));
            newer |= a > b;
            older |= a < b;
        }
        match (newer, older) {
            (false, false) => Order::Equal,
            (true, false) => Order::Newer,
            (false, true) => Order::Older,
            (true, true) => Order::Concurrent,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct FileMeta {
    /// BLAKE3 hash of the content. `None` means the file was deleted.
    pub hash: Option<[u8; 32]>,
    pub size: u64,
    /// Modification time, nanoseconds since the Unix epoch.
    pub mtime: i64,
    pub mode: u32,
    pub vv: Vv,
    /// Short ID of the node that made this version.
    pub by: u64,
}

impl FileMeta {
    pub fn deleted(&self) -> bool {
        self.hash.is_none()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Entry {
    pub path: String,
    pub meta: FileMeta,
}

pub type Index = BTreeMap<String, FileMeta>;

pub fn load(path: &Path) -> Index {
    std::fs::read(path)
        .ok()
        .and_then(|b| postcard::from_bytes(&b).ok())
        .unwrap_or_default()
}

pub fn save(index: &Index, path: &Path) -> anyhow::Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, postcard::to_stdvec(index)?)?;
    std::fs::rename(tmp, path)?;
    Ok(())
}

/// Entries whose path is `prefix` or inside it.
pub fn under<'a>(index: &'a Index, prefix: &'a str) -> impl Iterator<Item = &'a String> + 'a {
    let dir = if prefix.ends_with('/') { prefix.to_string() } else { format!("{prefix}/") };
    let own = index.get_key_value(prefix).map(|(k, _)| k);
    own.into_iter().chain(
        index.range(dir.clone()..).map(|(k, _)| k).take_while(move |k| k.starts_with(&dir)),
    )
}

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

pub fn hash_file(p: &Path) -> std::io::Result<[u8; 32]> {
    let mut f = std::fs::File::open(p)?;
    let mut h = blake3::Hasher::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            return Ok(*h.finalize().as_bytes());
        }
        h.update(&buf[..n]);
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Action {
    /// We already have this version or a newer one.
    Nothing,
    /// Take the remote version.
    Take,
    /// Both changed. The remote version wins; keep ours as a conflict copy.
    Conflict,
    /// Both changed. Our version wins, so we keep it as it is.
    LocalWins,
    /// Both changed to the same content. Adopt the combined version history.
    Same,
}

pub fn decide(local: Option<&FileMeta>, remote: &FileMeta) -> Action {
    let Some(l) = local else { return Action::Take };
    match l.vv.compare(&remote.vv) {
        Order::Equal | Order::Newer => Action::Nothing,
        Order::Older => Action::Take,
        Order::Concurrent if l.hash == remote.hash => Action::Same,
        Order::Concurrent if remote_wins(l, remote) => Action::Conflict,
        Order::Concurrent => Action::LocalWins,
    }
}

/// Every node must pick the same winner, so this only looks at the two versions.
/// A change beats a deletion; otherwise the newer mtime wins, then the higher node ID.
fn remote_wins(l: &FileMeta, r: &FileMeta) -> bool {
    if l.deleted() != r.deleted() {
        return l.deleted();
    }
    (r.mtime, r.by) > (l.mtime, l.by)
}
