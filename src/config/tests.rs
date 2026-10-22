use super::*;

use crate::paths::test_dir;

#[test]
fn with_port_adds_default_only_when_missing() {
    assert_eq!(with_port("host"), format!("host:{DEFAULT_PORT}"));
    assert_eq!(with_port("host:80"), "host:80");
    assert_eq!(with_port("[::1]:80"), "[::1]:80");
    assert_eq!(with_port("host:x"), format!("host:x:{DEFAULT_PORT}"));
    assert_eq!(with_port("host:70000"), format!("host:70000:{DEFAULT_PORT}"));
}

#[test]
fn with_port_adds_default_to_bare_ipv6_addresses() {
    assert_eq!(with_port("::1"), format!("[::1]:{DEFAULT_PORT}"));
    assert_eq!(with_port("fe80::1:2"), format!("[fe80::1:2]:{DEFAULT_PORT}"));
    assert_eq!(with_port("[::1]"), format!("[::1]:{DEFAULT_PORT}"));
}

#[test]
fn with_port_keeps_link_local_ipv6_addresses_with_a_zone_resolvable() {
    use std::net::ToSocketAddrs;
    let want = "[fe80::1%1]:21987".parse::<std::net::SocketAddr>().unwrap();
    for addr in ["fe80::1%lo0", "fe80::1%1", "fe80::1%lo0:21987"] {
        assert_eq!(with_port(addr).to_socket_addrs().unwrap().collect::<Vec<_>>(), [want], "{addr}");
    }
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

#[test]
fn set_node_keeps_aliases_and_set_aliases_bumps_the_clock() {
    let mut c = Config::default();
    c.set_node("me", "p1", "h:1", false);
    c.set_aliases("me", "p1", vec!["l:1".into()]);
    assert_eq!(c.shared.nodes["p1"].stamp, Stamp(2, "me".into()));
    c.set_node("me", "p1", "h:2", false);
    let n = c.trusted("p1").unwrap();
    assert_eq!(n.addrs().collect::<Vec<_>>(), ["h:2", "l:1"]);
}

#[test]
fn load_accepts_a_config_file_without_aliases() {
    let dir = test_dir("cfg-no-aliases");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("config.json");
    let json = r#"{"port":1,"clock":1,"shared":{"nodes":{"p1":{"addr":"h:1","removed":false,"stamp":[1,"me"]}},"roots":{}}}"#;
    std::fs::write(&path, json).unwrap();
    let c = Config::load(&path).unwrap();
    assert_eq!(c.trusted("p1").unwrap().addrs().collect::<Vec<_>>(), ["h:1"]);
    std::fs::remove_dir_all(&dir).unwrap();
}
