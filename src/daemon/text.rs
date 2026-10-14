use crate::config::Config;
use crate::net;
use std::collections::BTreeMap;

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
        "This node: {}\nPort: {}\n{daemon}\nNodes:\n{}\nPaths:\n{}",
        net::pretty_id(me),
        cfg.port,
        nodes_text(cfg, connected, pending),
        paths_text(cfg)
    )
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
        format!("  {}  {}{state}\n", net::pretty_id(id), n.addr)
    }));
    if !pending.is_empty() {
        s.push_str("\nWaiting to be trusted (run `rust-sync node add <address>` to accept):\n");
        s.extend(pending.iter().map(|(id, addr)| format!("  {}  {addr}\n", net::pretty_id(id))));
    }
    s
}

#[cfg(test)]
mod tests;
