//! What we know about each synced file, and how to compare two versions.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

mod file;

pub use file::{Stat, hash_file, now, stat};

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
mod tests {
    use super::*;

    fn test_dir(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("rust-sync-{name}-{}", std::process::id()))
    }

    fn vv(pairs: &[(u64, u64)]) -> Vv {
        Vv(pairs.to_vec())
    }

    fn meta(hash: Option<u8>, mtime: i64, by: u64, v: Vv) -> FileMeta {
        FileMeta { hash: hash.map(|b| [b; 32]), size: 0, mtime, mode: 0o644, vv: v, by }
    }

    #[test]
    fn bumped_increments_existing_and_adds_new_nodes_sorted() {
        assert_eq!(vv(&[(2, 1)]).bumped(2), vv(&[(2, 2)]));
        assert_eq!(vv(&[(2, 1)]).bumped(1), vv(&[(1, 1), (2, 1)]));
    }

    #[test]
    fn merged_takes_the_maximum_per_node() {
        let a = vv(&[(1, 3), (2, 1)]);
        let b = vv(&[(2, 4), (3, 1)]);
        assert_eq!(a.merged(&b), vv(&[(1, 3), (2, 4), (3, 1)]));
    }

    #[test]
    fn merged_keeps_our_count_when_it_is_higher() {
        assert_eq!(vv(&[(1, 5)]).merged(&vv(&[(1, 2)])), vv(&[(1, 5)]));
    }

    #[test]
    fn compare_orders_version_vectors() {
        assert_eq!(vv(&[(1, 1)]).compare(&vv(&[(1, 1)])), Order::Equal);
        assert_eq!(vv(&[(1, 2)]).compare(&vv(&[(1, 1)])), Order::Newer);
        assert_eq!(vv(&[(1, 1)]).compare(&vv(&[(1, 1), (2, 1)])), Order::Older);
        assert_eq!(vv(&[(1, 2)]).compare(&vv(&[(1, 1), (2, 1)])), Order::Concurrent);
    }

    #[test]
    fn compare_counts_nodes_only_we_know() {
        assert_eq!(vv(&[(1, 1), (2, 1)]).compare(&vv(&[(1, 1)])), Order::Newer);
    }

    #[test]
    fn decide_takes_remote_when_unknown_locally_or_newer() {
        let r = meta(Some(1), 0, 1, vv(&[(1, 2)]));
        assert_eq!(decide(None, &r), Action::Take);
        assert_eq!(decide(Some(&meta(Some(2), 0, 1, vv(&[(1, 1)]))), &r), Action::Take);
    }

    #[test]
    fn decide_ignores_equal_or_older_remote() {
        let l = meta(Some(1), 0, 1, vv(&[(1, 2)]));
        assert_eq!(decide(Some(&l), &meta(Some(1), 0, 1, vv(&[(1, 2)]))), Action::Nothing);
        assert_eq!(decide(Some(&l), &meta(Some(2), 0, 1, vv(&[(1, 1)]))), Action::Nothing);
    }

    #[test]
    fn decide_concurrent_same_content_is_same() {
        let l = meta(Some(1), 5, 1, vv(&[(1, 1)]));
        let r = meta(Some(1), 9, 2, vv(&[(2, 1)]));
        assert_eq!(decide(Some(&l), &r), Action::Same);
        let l = meta(None, 5, 1, vv(&[(1, 1)]));
        let r = meta(None, 9, 2, vv(&[(2, 1)]));
        assert_eq!(decide(Some(&l), &r), Action::Same);
    }

    #[test]
    fn decide_concurrent_newer_mtime_wins_then_higher_node() {
        let l = meta(Some(1), 5, 1, vv(&[(1, 1)]));
        assert_eq!(decide(Some(&l), &meta(Some(2), 9, 2, vv(&[(2, 1)]))), Action::Conflict);
        assert_eq!(decide(Some(&l), &meta(Some(2), 1, 2, vv(&[(2, 1)]))), Action::LocalWins);
        assert_eq!(decide(Some(&l), &meta(Some(2), 5, 2, vv(&[(2, 1)]))), Action::Conflict);
        assert_eq!(decide(Some(&l), &meta(Some(2), 5, 0, vv(&[(0, 1)]))), Action::LocalWins);
        assert_eq!(decide(Some(&l), &meta(Some(2), 5, 1, vv(&[(2, 1)]))), Action::LocalWins);
    }

    #[test]
    fn decide_concurrent_change_beats_deletion() {
        let changed = meta(Some(1), 1, 1, vv(&[(1, 1)]));
        let deleted = meta(None, 9, 2, vv(&[(2, 1)]));
        assert_eq!(decide(Some(&changed), &deleted), Action::LocalWins);
        assert_eq!(decide(Some(&deleted), &changed), Action::Conflict);
    }

    #[test]
    fn under_matches_prefix_and_its_children_only_with_or_without_trailing_slash() {
        let m = meta(Some(1), 0, 1, Vv::default());
        let index: Index = ["~/a", "~/a/b", "~/a/c/d", "~/ab", "~/b"].map(|k| (k.to_string(), m.clone())).into();
        let got: Vec<_> = under(&index, "~/a").map(|(k, _)| k.clone()).collect();
        assert_eq!(got, ["~/a", "~/a/b", "~/a/c/d"]);
        let got: Vec<_> = under(&index, "~/a/").map(|(k, _)| k.clone()).collect();
        assert_eq!(got, ["~/a/b", "~/a/c/d"]);
    }

    #[test]
    fn load_gives_empty_for_missing_or_damaged_file_and_save_round_trips() {
        let dir = test_dir("index");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("index.bin");
        assert!(load(&path).is_empty());
        let index: Index = [("~/a".to_string(), meta(Some(7), 42, 3, vv(&[(3, 2)])))].into();
        save(&index, &path).unwrap();
        assert_eq!(load(&path), index);
        assert!(!path.with_extension("tmp").exists());
        std::fs::write(&path, [0xff; 4]).unwrap();
        assert!(load(&path).is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
