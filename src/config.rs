//! The list of nodes and synced paths. This list is shared by every node.
//!
//! Each entry carries a stamp (Lamport counter, node ID). When two nodes disagree,
//! the entry with the higher stamp wins. Removals are kept as `removed` entries
//! so that a node that was offline cannot bring a removed entry back.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

/// Listen on this port, and dial it when a peer address has no port.
pub const DEFAULT_PORT: u16 = 21987;

/// When an entry was last changed: a clock value and the ID of the node that changed it. Higher stamps win.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Stamp(pub u64, pub String);

/// A node in the shared list: its address and whether it was removed.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct NodeEntry {
    /// Where to dial the node, as `host` or `host:port`.
    pub addr: String,
    /// True if the node was removed from the list.
    pub removed: bool,
    /// When this entry was last changed.
    pub stamp: Stamp,
}

/// A synced folder or file in the shared list, and whether it was removed.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RootEntry {
    /// True for a folder, false for a single file.
    pub is_dir: bool,
    /// True if the path is no longer synced.
    pub removed: bool,
    /// When this entry was last changed.
    pub stamp: Stamp,
}

/// The node and root lists that peers exchange and merge.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Shared {
    /// Nodes, keyed by node ID.
    pub nodes: BTreeMap<String, NodeEntry>,
    /// Synced paths, keyed by portable path (for example `~/Documents`).
    pub roots: BTreeMap<String, RootEntry>,
}

/// This node's saved settings: its port, its clock and the shared lists.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Config {
    /// The port this node listens on.
    pub port: u16,
    /// The Lamport clock: the highest stamp value seen or made by this node.
    pub clock: u64,
    /// The lists shared with peers.
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
        Ok(crate::paths::write_atomic(path, &serde_json::to_vec_pretty(self)?)?)
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
            if id != me {
                changed |= take_newer(&mut self.shared.nodes, id, n, |n| &n.stamp);
            }
        }
        for (path, r) in &other.roots {
            self.clock = self.clock.max(r.stamp.0);
            changed |= take_newer(&mut self.shared.roots, path, r, |r| &r.stamp);
        }
        changed
    }
}

/// Store `theirs` under `key` if its stamp is higher than ours. Returns true if stored.
fn take_newer<T: Clone>(ours: &mut BTreeMap<String, T>, key: &str, theirs: &T, stamp: impl Fn(&T) -> &Stamp) -> bool {
    if ours.get(key).is_some_and(|o| stamp(theirs) <= stamp(o)) {
        return false;
    }
    ours.insert(key.into(), theirs.clone());
    true
}

/// Add the default port if the address has none.
pub fn with_port(addr: &str) -> String {
    if let Ok(ip) = addr.parse::<std::net::Ipv6Addr>() {
        return std::net::SocketAddr::from((ip, DEFAULT_PORT)).to_string();
    }
    match addr.rsplit_once(':') {
        Some((_, p)) if p.parse::<u16>().is_ok() => addr.to_string(),
        _ => format!("{addr}:{DEFAULT_PORT}"),
    }
}

#[cfg(test)]
mod tests;
