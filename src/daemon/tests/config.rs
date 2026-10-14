use super::*;

#[test]
fn messages_from_a_stale_connection_are_ignored() {
    let mut d = daemon();
    let me = d.me.clone();
    d.cfg.set_node(&me, "aa", "h:1", false);
    let mut rx = up(&mut d, "aa", 1);
    while rx.try_recv().is_ok() {}
    let request = || Msg::Request { path: "~/x".into(), hash: [1; 32] };
    d.handle(Event::Peer { id: "aa".into(), conn: 2, msg: request() });
    assert!(rx.try_recv().is_err());
    d.handle(Event::Peer { id: "aa".into(), conn: 1, msg: request() });
    assert!(matches!(rx.try_recv(), Ok(Msg::Unavailable { .. })));
}

#[test]
fn connected_node_gets_config_then_only_index_entries_in_shared_folders() {
    let mut d = daemon();
    let me = d.me.clone();
    d.cfg.set_node(&me, "aa", "h:1", false);
    d.cfg.set_root(&me, "~/s", true, false);
    d.index.insert("~/s/f".into(), meta(0, 0));
    d.index.insert("~/x".into(), meta(0, 0));
    let mut rx = up(&mut d, "aa", 1);
    assert!(matches!(rx.try_recv(), Ok(Msg::Config(_))));
    let Ok(Msg::Index(entries)) = rx.try_recv() else { panic!("no index sent") };
    assert_eq!(entries.iter().map(|e| e.path.as_str()).collect::<Vec<_>>(), ["~/s/f"]);
    assert!(rx.try_recv().is_err());
}

#[test]
fn a_config_too_large_for_one_message_is_sent_in_pieces_that_fit() {
    let mut d = daemon();
    let me = d.me.clone();
    d.cfg.set_node(&me, "aa", "h:1", false);
    for i in 0..2000 {
        d.cfg.set_root(&me, &format!("~/{}/{i}", "x".repeat(40)), true, true);
    }
    let received = |rx: &mut mpsc::UnboundedReceiver<Msg>| {
        let mut got = Config::default();
        while let Ok(m) = rx.try_recv() {
            if let Msg::Config(s) = m {
                let len = postcard::to_stdvec(&Msg::Config(s.clone())).unwrap().len();
                assert!(len <= net::MAX_MSG, "{len}");
                got.merge(&s, "zz");
            }
        }
        got.shared
    };
    let mut rx = up(&mut d, "aa", 1);
    let got = received(&mut rx);
    assert_eq!((got.nodes.len(), got.roots.len()), (1, 2000));
    d.on_ctl(&Req::AddPath { path: "~/new".into(), is_dir: true });
    let got = received(&mut rx);
    assert_eq!((got.nodes.len(), got.roots.len()), (1, 2001));
}

#[test]
fn peer_config_removing_a_node_drops_its_connection_and_downloads_and_an_old_config_changes_nothing() {
    let mut d = daemon();
    let me = d.me.clone();
    let mut rxs = Vec::new();
    for (id, conn) in [("aa", 1), ("bb", 2)] {
        d.cfg.set_node(&me, id, "h:1", false);
        let rx = up(&mut d, id, conn);
        rxs.push(rx);
    }
    d.queued.insert("~/s/f".into(), ("bb".into(), meta(0, 0), Vec::new()));
    let old = d.cfg.shared.clone();
    let mut theirs = Config { clock: d.cfg.clock, ..Config::default() };
    theirs.set_node("aa", "bb", "h:1", true);
    for rx in &mut rxs {
        while rx.try_recv().is_ok() {}
    }
    d.handle(Event::Peer { id: "aa".into(), conn: 1, msg: Msg::Config(theirs.shared) });
    assert_eq!(d.peers.keys().collect::<Vec<_>>(), ["aa"]);
    assert!(d.queued.is_empty());
    assert!(matches!(rxs[0].try_recv(), Ok(Msg::Config(c)) if c.nodes["bb"].removed));
    d.handle(Event::Peer { id: "aa".into(), conn: 1, msg: Msg::Config(old) });
    assert!(d.cfg.trusted("bb").is_none());
    assert!(rxs[0].try_recv().is_err());
}

#[tokio::test]
async fn node_list_shows_a_peer_shared_node_id_that_is_not_ascii() {
    let mut d = daemon();
    let me = d.me.clone();
    d.cfg.set_node(&me, "aa", "h:1", false);
    let _rx = up(&mut d, "aa", 1);
    let mut theirs = Config { clock: d.cfg.clock, ..Config::default() };
    theirs.set_node("aa", "aéééé", "h:2", false);
    d.handle(Event::Peer { id: "aa".into(), conn: 1, msg: Msg::Config(theirs.shared) });
    let resp = d.on_ctl(&Req::ListNodes);
    assert!(resp.text.contains("AÉÉÉÉ  h:2"), "{}", resp.text);
}
