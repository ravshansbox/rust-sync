//! What we know about each synced file, and how to compare two versions.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

mod file;

pub use file::{Stat, hash_file, now, stat};

/// Version vector: for each node (short ID), how many changes it has made.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct Vv(pub Vec<(u64, u64)>);

/// How one version vector relates to another.
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

    pub fn bumped(&self, node: u64) -> Self {
        self.merged(&Self(vec![(node, self.get(node) + 1)]))
    }

    pub fn merged(&self, other: &Self) -> Self {
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
    pub fn compare(&self, other: &Self) -> Order {
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

/// One version of a file: its content hash, attributes and version history.
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
    pub const fn deleted(&self) -> bool {
        self.hash.is_none()
    }
}

/// A file path with its metadata, as sent between nodes.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Entry {
    pub path: String,
    pub meta: FileMeta,
}

impl From<(&String, &FileMeta)> for Entry {
    fn from((path, meta): (&String, &FileMeta)) -> Self {
        Self { path: path.clone(), meta: meta.clone() }
    }
}

/// File metadata for every known path, sorted by path.
pub type Index = BTreeMap<String, FileMeta>;

pub fn load(path: &Path) -> Index {
    std::fs::read(path)
        .ok()
        .and_then(|b| postcard::from_bytes(&b).ok())
        .unwrap_or_default()
}

pub fn save(index: &Index, path: &Path) -> anyhow::Result<()> {
    Ok(crate::paths::write_atomic(path, &postcard::to_stdvec(index)?)?)
}

/// Entries whose path is `prefix` or inside it.
pub fn under<'a>(index: &'a Index, prefix: &'a str) -> impl Iterator<Item = (&'a String, &'a FileMeta)> + 'a {
    let dir = if prefix.ends_with('/') { prefix.to_string() } else { format!("{prefix}/") };
    let own = index.get_key_value(prefix);
    own.into_iter().chain(index.range(dir.clone()..).take_while(move |(k, _)| k.starts_with(&dir)))
}

/// What to do with a remote version, as chosen by `decide`.
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
    /// Both changed to the same content and mode. Adopt the combined version history.
    Same,
}

pub fn decide(local: Option<&FileMeta>, remote: &FileMeta) -> Action {
    let Some(l) = local else { return Action::Take };
    match l.vv.compare(&remote.vv) {
        Order::Equal | Order::Newer => Action::Nothing,
        Order::Older => Action::Take,
        Order::Concurrent if l.hash == remote.hash && l.mode == remote.mode => Action::Same,
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

#[cfg(test)]
mod tests;
