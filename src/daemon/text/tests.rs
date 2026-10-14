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
