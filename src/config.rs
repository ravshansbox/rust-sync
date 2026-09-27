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
        Config { port: DEFAULT_PORT, clock: 0, shared: Shared::default() }
    }
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Config> {
        match std::fs::read(path) {
            Ok(b) => Ok(serde_json::from_slice(&b)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Config::default()),
            Err(e) => Err(e.into()),
        }
    }

    pub fn save(&self, path: &Path) -> anyhow::Result<()> {
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(self)?)?;
        std::fs::rename(tmp, path)?;
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
