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
mod tests {
    use super::*;

    #[test]
    fn paths_text_lists_active_roots_and_marks_folders() {
        let mut cfg = Config::default();
        assert_eq!(paths_text(&cfg), "  (none) add one with `rust-sync path add <path>`\n");
        cfg.set_root("me", "~/docs", true, false);
        cfg.set_root("me", "~/gone", true, true);
        cfg.set_root("me", "~/notes.md", false, false);
        assert_eq!(paths_text(&cfg), "  ~/docs/\n  ~/notes.md\n");
    }

    #[test]
    fn paths_text_does_not_add_a_second_slash_to_the_root_folder() {
        let mut cfg = Config::default();
        cfg.set_root("me", "/", true, false);
        assert_eq!(paths_text(&cfg), "  /\n");
    }

    #[test]
    fn nodes_text_shows_state_and_pending_nodes() {
        let mut cfg = Config::default();
        let none = BTreeMap::new();
        assert_eq!(nodes_text(&cfg, |_| None, &none), "  (none) add one with `rust-sync node add <host>`\n");
        cfg.set_node("me", "aaaaaaaabbbbbbbb", "a:1", false);
        cfg.set_node("me", "cccccccc", "c:1", false);
        cfg.set_node("me", "dddddddd", "d:1", true);
        let pending = BTreeMap::from([("eeeeeeee".to_string(), "1.2.3.4:5".to_string())]);
        assert_eq!(
            nodes_text(&cfg, |id| Some(id == "cccccccc"), &pending),
            "  AAAAAAAA-BBBBBBBB  a:1  offline\n  CCCCCCCC  c:1  connected\n\n\
             Waiting to be trusted (run `rust-sync node add <address>` to accept):\n  EEEEEEEE  1.2.3.4:5\n"
        );
        assert_eq!(nodes_text(&cfg, |_| None, &none), "  AAAAAAAA-BBBBBBBB  a:1\n  CCCCCCCC  c:1\n");
    }

    #[test]
    fn status_text_says_when_daemon_is_not_running() {
        let cfg = Config::default();
        let none = BTreeMap::new();
        let stopped = status_text(&cfg, "abcd", |_| None, &none);
        assert!(stopped.starts_with(&format!("This node: ABCD\nPort: {}\nDaemon: not running", cfg.port)));
        let running = status_text(&cfg, "abcd", |_| Some(true), &none);
        assert_eq!(
            running,
            format!(
                "This node: ABCD\nPort: {}\n\nNodes:\n{}\nPaths:\n{}",
                cfg.port,
                nodes_text(&cfg, |_| Some(true), &none),
                paths_text(&cfg)
            )
        );
    }
}
