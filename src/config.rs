//! The list of nodes and synced paths. This list is shared by every node.
//!
//! Each entry carries a stamp (Lamport counter, node ID). When two nodes disagree,
//! the entry with the higher stamp wins. Removals are kept as `removed` entries
//! so that a node that was offline cannot bring a removed entry back.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

pub const DEFAULT_PORT: u16 = 21987;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Stamp(pub u64, pub String);

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct NodeEntry {
    pub addr: String,
    pub removed: bool,
    pub stamp: Stamp,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RootEntry {
    pub is_dir: bool,
    pub removed: bool,
    pub stamp: Stamp,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Shared {
    pub nodes: BTreeMap<String, NodeEntry>,
    pub roots: BTreeMap<String, RootEntry>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Config {
    pub port: u16,
    pub clock: u64,
    pub shared: Shared,
}

impl Default for Config {
    fn default() -> Self {
        Self { port: DEFAULT_PORT, clock: 0, shared: Shared::default() }
    }
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        match std::fs::read(path) {
            Ok(b) => Ok(serde_json::from_slice(&b)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e.into()),
        }
    }

    pub fn save(&self, path: &Path) -> anyhow::Result<()> {
        crate::paths::write_atomic(path, &serde_json::to_vec_pretty(self)?)?;
        Ok(())
    }

    fn stamp(&mut self, me: &str) -> Stamp {
        self.clock += 1;
        Stamp(self.clock, me.to_string())
    }

    pub fn set_root(&mut self, me: &str, path: &str, is_dir: bool, removed: bool) {
        let stamp = self.stamp(me);
        self.shared.roots.insert(path.into(), RootEntry { is_dir, removed, stamp });
    }

    pub fn set_node(&mut self, me: &str, id: &str, addr: &str, removed: bool) {
        let stamp = self.stamp(me);
        self.shared.nodes.insert(id.into(), NodeEntry { addr: addr.into(), removed, stamp });
    }

    pub fn active_roots(&self) -> impl Iterator<Item = (&String, &RootEntry)> {
        self.shared.roots.iter().filter(|(_, r)| !r.removed)
    }

    pub fn trusted_nodes(&self) -> impl Iterator<Item = (&String, &NodeEntry)> {
        self.shared.nodes.iter().filter(|(_, n)| !n.removed)
    }

    pub fn active_root(&self, path: &str) -> Option<&RootEntry> {
        self.shared.roots.get(path).filter(|r| !r.removed)
    }

    pub fn trusted(&self, id: &str) -> Option<&NodeEntry> {
        self.shared.nodes.get(id).filter(|n| !n.removed)
    }

    /// The active root that contains `path`, if any.
    pub fn root_of(&self, path: &str) -> Option<&str> {
        self.active_roots()
            .map(|(r, _)| r.as_str())
            .find(|r| crate::paths::is_under(path, r))
    }

    /// Merge a peer's list into ours. Returns true if anything changed.
    pub fn merge(&mut self, other: &Shared, me: &str) -> bool {
        let mut changed = false;
        for (id, n) in &other.nodes {
            self.clock = self.clock.max(n.stamp.0);
            if id == me {
                continue;
            }
            if self.shared.nodes.get(id).is_none_or(|ours| n.stamp > ours.stamp) {
                self.shared.nodes.insert(id.clone(), n.clone());
                changed = true;
            }
        }
        for (path, r) in &other.roots {
            self.clock = self.clock.max(r.stamp.0);
            if self.shared.roots.get(path).is_none_or(|ours| r.stamp > ours.stamp) {
                self.shared.roots.insert(path.clone(), r.clone());
                changed = true;
            }
        }
        changed
    }
}

/// Add the default port if the address has none.
pub fn with_port(addr: &str) -> String {
    match addr.rsplit_once(':') {
        Some((_, p)) if p.parse::<u16>().is_ok() => addr.to_string(),
        _ => format!("{addr}:{DEFAULT_PORT}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_dir(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("rust-sync-{name}-{}", std::process::id()))
    }

    #[test]
    fn with_port_adds_default_only_when_missing() {
        assert_eq!(with_port("host"), format!("host:{DEFAULT_PORT}"));
        assert_eq!(with_port("host:80"), "host:80");
        assert_eq!(with_port("host:x"), format!("host:x:{DEFAULT_PORT}"));
        assert_eq!(with_port("host:70000"), format!("host:70000:{DEFAULT_PORT}"));
    }

    #[test]
    fn set_root_and_set_node_bump_the_clock() {
        let mut c = Config::default();
        c.set_root("me", "~/a", true, false);
        c.set_node("me", "peer", "host:1", false);
        assert_eq!(c.clock, 2);
        assert_eq!(c.shared.roots["~/a"].stamp, Stamp(1, "me".into()));
        assert_eq!(c.shared.nodes["peer"].stamp, Stamp(2, "me".into()));
    }

    #[test]
    fn removed_entries_are_hidden() {
        let mut c = Config::default();
        c.set_root("me", "~/a", true, false);
        c.set_root("me", "~/b", true, true);
        c.set_node("me", "p1", "h:1", false);
        c.set_node("me", "p2", "h:2", true);
        let roots: Vec<_> = c.active_roots().map(|(p, _)| p.as_str()).collect();
        assert_eq!(roots, ["~/a"]);
        assert!(c.active_root("~/a").unwrap().is_dir);
        assert!(c.active_root("~/b").is_none());
        assert!(c.active_root("~/c").is_none());
        let nodes: Vec<_> = c.trusted_nodes().map(|(id, _)| id.as_str()).collect();
        assert_eq!(nodes, ["p1"]);
        assert_eq!(c.trusted("p1").unwrap().addr, "h:1");
        assert!(c.trusted("p2").is_none());
        assert!(c.trusted("p3").is_none());
    }

    #[test]
    fn root_of_finds_the_containing_active_root() {
        let mut c = Config::default();
        c.set_root("me", "~/a", true, false);
        c.set_root("me", "~/b", true, true);
        assert_eq!(c.root_of("~/a"), Some("~/a"));
        assert_eq!(c.root_of("~/a/x"), Some("~/a"));
        assert_eq!(c.root_of("~/ab"), None);
        assert_eq!(c.root_of("~/b/x"), None);
    }

    #[test]
    fn merge_takes_higher_stamps_and_advances_the_clock() {
        let mut ours = Config::default();
        ours.set_root("me", "~/a", true, false);
        let mut theirs = Config { clock: 5, ..Config::default() };
        theirs.set_root("peer", "~/a", true, true);
        theirs.set_root("peer", "~/b", false, false);
        assert!(ours.merge(&theirs.shared, "me"));
        assert!(ours.shared.roots["~/a"].removed);
        assert!(!ours.shared.roots["~/b"].is_dir);
        assert_eq!(ours.clock, 7);
        assert!(!ours.merge(&theirs.shared, "me"));
    }

    #[test]
    fn merge_keeps_ours_when_stamp_is_not_higher() {
        let mut ours = Config { clock: 9, ..Config::default() };
        ours.set_root("me", "~/a", true, false);
        let mut theirs = Config::default();
        theirs.set_root("peer", "~/a", true, true);
        assert!(!ours.merge(&theirs.shared, "me"));
        assert!(!ours.shared.roots["~/a"].removed);
        assert_eq!(ours.clock, 10);
    }

    #[test]
    fn merge_takes_peer_nodes_and_breaks_equal_counters_by_node_id() {
        let mut ours = Config::default();
        ours.set_node("me", "p1", "old:1", false);
        let mut theirs = Config { clock: 5, ..Config::default() };
        theirs.set_node("peer", "p1", "new:1", false);
        theirs.set_node("peer", "p2", "new:2", false);
        assert!(ours.merge(&theirs.shared, "me"));
        assert_eq!(ours.trusted("p1").unwrap().addr, "new:1");
        assert_eq!(ours.trusted("p2").unwrap().addr, "new:2");
        let mut low = Config::default();
        low.set_node("a", "p1", "a:1", false);
        let mut high = Config::default();
        high.set_node("b", "p1", "b:1", false);
        assert!(!high.merge(&low.shared, "me"));
        assert!(low.merge(&high.shared, "me"));
        assert_eq!(low.trusted("p1").unwrap().addr, "b:1");
        assert_eq!(high.trusted("p1").unwrap().addr, "b:1");
    }

    #[test]
    fn merge_ignores_entries_about_ourselves() {
        let mut ours = Config::default();
        let mut theirs = Config { clock: 3, ..Config::default() };
        theirs.set_node("peer", "me", "h:1", true);
        assert!(!ours.merge(&theirs.shared, "me"));
        assert!(ours.shared.nodes.is_empty());
        assert_eq!(ours.clock, 4);
    }

    #[test]
    fn load_missing_file_gives_default_damaged_file_fails_and_save_round_trips() {
        let dir = test_dir("config");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.json");
        let c = Config::load(&path).unwrap();
        assert_eq!(c.port, DEFAULT_PORT);
        let mut c = Config { port: 1234, ..c };
        c.set_root("me", "~/a", true, false);
        c.save(&path).unwrap();
        let back = Config::load(&path).unwrap();
        assert_eq!(back.port, 1234);
        assert_eq!(back.clock, 1);
        assert!(back.shared.roots.contains_key("~/a"));
        std::fs::write(&path, b"not json").unwrap();
        assert!(Config::load(&path).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn load_fails_when_the_file_cannot_be_read() {
        let dir = test_dir("config-unreadable");
        std::fs::create_dir_all(&dir).unwrap();
        assert!(Config::load(&dir).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
