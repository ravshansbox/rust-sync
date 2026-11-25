//! Plain-text output for the status, path list and node list commands.

use crate::config::Config;
use crate::net;
use crate::paths;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Status text. `connected` returns `None` when the daemon is not running.
/// `pending` holds untrusted nodes that tried to connect (ID -> address).
pub fn status_text(
    cfg: &Config,
    me: &str,
    connected: impl Fn(&str) -> Option<bool>,
    pending: &BTreeMap<String, String>,
) -> String {
    let daemon = if connected(me).is_none() { "Daemon: not running (start it with `rust-sync daemon`)\n" } else { "" };
    format!(
        "This node: {}\nPort: {}\n{daemon}\nNodes:\n{}\nPaths:\n{}\nConflict copies:\n{}",
        net::pretty_id(me),
        cfg.port,
        nodes_text(cfg, connected, pending),
        paths_text(cfg),
        conflicts_text(cfg)
    )
}

/// Conflict copies on disk: anywhere under synced folders, and next to synced files.
pub fn conflicts_text(cfg: &Config) -> String {
    let mut found = Vec::new();
    for (root, entry) in cfg.active_roots() {
        let Some(local) = paths::to_local(root) else { continue };
        if entry.is_dir {
            conflicts_under(&local, &mut found);
        } else if let (Some(parent), Some(stem)) = (local.parent(), local.file_stem()) {
            let prefix = format!("{}.sync-conflict-", stem.to_string_lossy());
            let extension = local.extension().map(|e| format!(".{}", e.to_string_lossy())).unwrap_or_default();
            for file in std::fs::read_dir(parent).into_iter().flatten().flatten() {
                let name = file.file_name().to_string_lossy().into_owned();
                if name.starts_with(&prefix) && name.ends_with(&extension) {
                    found.push(file.path());
                }
            }
        }
    }
    found.sort();
    found.dedup();
    if found.is_empty() {
        return "  (none)\n".into();
    }
    found.iter().map(|p| format!("  {}\n", paths::to_portable(p).unwrap_or_else(|| p.display().to_string()))).collect()
}

fn conflicts_under(dir: &Path, found: &mut Vec<PathBuf>) {
    for file in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let Ok(file_type) = file.file_type() else { continue };
        if file_type.is_dir() {
            conflicts_under(&file.path(), found);
        } else if file_type.is_file() && file.file_name().to_string_lossy().contains(".sync-conflict-") {
            found.push(file.path());
        }
    }
}

/// Synced files and folders. Folders end with `/`.
pub fn paths_text(cfg: &Config) -> String {
    let roots: Vec<_> = cfg.active_roots().collect();
    if roots.is_empty() {
        return "  (none) add one with `rust-sync path add <path>`\n".into();
    }
    roots.into_iter().map(|(p, r)| format!("  {p}{}\n", if r.is_dir && !p.ends_with('/') { "/" } else { "" })).collect()
}

/// Trusted nodes with their address and connection state, then untrusted nodes
/// waiting to be accepted. `connected` returns `None` when the daemon is not running.
pub fn nodes_text(
    cfg: &Config,
    connected: impl Fn(&str) -> Option<bool>,
    pending: &BTreeMap<String, String>,
) -> String {
    let mut s = String::new();
    let nodes: Vec<_> = cfg.trusted_nodes().collect();
    if nodes.is_empty() {
        s.push_str("  (none) add one with `rust-sync node add <host>`\n");
    }
    s.extend(nodes.into_iter().map(|(id, n)| {
        let state = match connected(id) {
            Some(true) => "  connected",
            Some(false) => "  offline",
            None => "",
        };
        let also = if n.aliases.is_empty() { String::new() } else { format!(" (also {})", n.aliases.join(", ")) };
        format!("  {}  {}{also}{state}\n", net::pretty_id(id), n.addr)
    }));
    if !pending.is_empty() {
        s.push_str("\nWaiting to be trusted (run `rust-sync node add <address>` to accept):\n");
        s.extend(pending.iter().map(|(id, addr)| format!("  {}  {addr}\n", net::pretty_id(id))));
    }
    s
}

#[cfg(test)]
mod tests;
