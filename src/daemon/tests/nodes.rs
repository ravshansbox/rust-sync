use super::*;
use crate::daemon::download::MAX_DOWNLOADS;
use crate::daemon::peers::{MAX_BACKOFF, MAX_PENDING};

#[test]
fn untrusted_nodes_are_listed_until_newer_ones_push_the_oldest_out() {
    let mut d = daemon();
    for i in 0..=MAX_PENDING {
        let (reply, mut rx) = oneshot::channel();
        d.handle(Event::Auth { id: format!("{i:02}"), addr: Some(format!("h:{i}")), reply });
        assert!(!rx.try_recv().unwrap());
    }
    let text = d.on_ctl(&Req::ListNodes).text;
    assert!(!text.contains("  00  h:0\n"), "{text}");
    assert!(text.contains("  01  h:1\n"), "{text}");
    assert!(text.contains(&format!("  {MAX_PENDING}  h:{MAX_PENDING}\n")), "{text}");
    assert_eq!(text.matches("  h:").count(), MAX_PENDING);
}

#[test]
fn untrusted_node_trying_again_is_listed_with_its_new_address_and_kept_as_newest() {
    let mut d = daemon();
    let mut auth = |id: String, addr: String| {
        d.handle(Event::Auth { id, addr: Some(addr), reply: oneshot::channel().0 });
    };
    for i in 0..MAX_PENDING {
        auth(format!("{i:02}"), format!("h:{i}"));
    }
    auth("00".into(), "h:new".into());
    auth("99".into(), "h:99".into());
    let text = d.on_ctl(&Req::ListNodes).text;
    assert!(text.contains("  00  h:new\n"), "{text}");
    assert!(!text.contains("  01  h:1\n"), "{text}");
    assert!(text.contains("  99  h:99\n"), "{text}");
    assert_eq!(text.matches("  h:").count(), MAX_PENDING);
}

#[test]
fn trusted_node_is_accepted_and_not_listed_as_waiting() {
    let mut d = daemon();
    let me = d.me.clone();
    d.cfg.set_node(&me, "aa", "h:1", false);
    let (reply, mut rx) = oneshot::channel();
    d.handle(Event::Auth { id: "aa".into(), addr: Some("h:2".into()), reply });
    assert!(rx.try_recv().unwrap());
    let text = d.on_ctl(&Req::ListNodes).text;
    assert!(!text.contains("Waiting to be trusted"), "{text}");
}

#[tokio::test]
async fn trusting_a_waiting_node_stops_listing_it_and_dials_it_without_waiting_to_retry() {
    let mut d = daemon();
    d.handle(Event::Auth { id: "aa".into(), addr: Some("h:1".into()), reply: oneshot::channel().0 });
    d.backoff.insert("aa".into(), Duration::from_secs(8));
    let resp = d.on_ctl(&Req::AddNode { id: "aa".into(), addr: "127.0.0.1:1".into() });
    assert!(resp.ok, "{}", resp.text);
    let text = d.on_ctl(&Req::ListNodes).text;
    assert!(!text.contains("Waiting to be trusted"), "{text}");
    assert!(d.backoff.is_empty());
    assert_eq!(d.dialling, HashSet::from(["aa".to_string()]));
}

#[test]
fn waiting_node_trusted_by_a_peer_is_no_longer_listed_once_connected() {
    let mut d = daemon();
    d.handle(Event::Auth { id: "aa".into(), addr: Some("h:1".into()), reply: oneshot::channel().0 });
    d.cfg.set_node("peer", "aa", "h:1", false);
    up(&mut d, "aa", 1);
    let text = d.on_ctl(&Req::ListNodes).text;
    assert!(!text.contains("Waiting to be trusted"), "{text}");
}

#[test]
fn connection_from_a_node_removed_after_auth_is_not_used() {
    let mut d = daemon();
    let me = d.me.clone();
    d.cfg.set_node(&me, "aa", "h:1", false);
    d.cfg.set_node(&me, "aa", "h:1", true);
    let mut rx = up(&mut d, "aa", 1);
    assert!(rx.try_recv().is_err());
    assert!(d.peers.is_empty());
}

#[test]
fn duplicate_connection_is_kept_only_if_dialled_by_the_lower_id() {
    let mut d = daemon();
    let me = d.me.clone();
    for (id, keep) in [("00", false), ("ff", true)] {
        d.cfg.set_node(&me, id, "h:1", false);
        let mut up = |conn, outbound| {
            let (ctl, mut rx) = mpsc::unbounded_channel();
            let data = mpsc::channel(1).0;
            d.handle(Event::Up { id: id.into(), conn, outbound, ctl, data });
            matches!(rx.try_recv(), Ok(Msg::Config(_)))
        };
        assert!(up(1, !keep));
        assert!(!up(2, !keep));
        assert!(up(3, keep));
    }
}

#[test]
fn duplicate_connection_that_is_kept_cancels_downloads_over_the_old_one() {
    let mut d = daemon();
    let me = d.me.clone();
    d.cfg.set_node(&me, "ff", "h:1", false);
    let _rx = up(&mut d, "ff", 1);
    let tmp = test_dir("dup-down");
    d.downloads.insert("~/s/f".into(), download("ff", meta(0, 0), &tmp));
    let (ctl, _rx2) = mpsc::unbounded_channel();
    d.handle(Event::Up { id: "ff".into(), conn: 2, outbound: false, ctl, data: mpsc::channel(1).0 });
    assert!(tmp.exists());
    let _rx3 = up(&mut d, "ff", 3);
    assert!(!tmp.exists());
    assert!(d.downloads.is_empty());
}

#[tokio::test]
async fn tick_dials_offline_nodes_not_waiting_to_retry() {
    let mut d = daemon();
    let me = d.me.clone();
    for id in ["aa", "bb", "cc"] {
        d.cfg.set_node(&me, id, "127.0.0.1:1", false);
    }
    let mut rx = up(&mut d, "aa", 1);
    while rx.try_recv().is_ok() {}
    d.backoff.insert("bb".into(), Duration::from_secs(1));
    d.handle(Event::Tick);
    assert_eq!(d.dialling, HashSet::from(["cc".to_string()]));
    d.handle(Event::Redial { id: "bb".into() });
    assert_eq!(d.dialling, HashSet::from(["bb".to_string(), "cc".to_string()]));
}

#[tokio::test]
async fn node_removed_while_waiting_to_retry_is_dialled_when_a_peer_trusts_it_again() {
    let mut d = daemon();
    let me = d.me.clone();
    d.cfg.set_node(&me, "aa", "127.0.0.1:1", false);
    d.backoff.insert("aa".into(), Duration::from_secs(8));
    assert!(d.on_ctl(&Req::RemoveNode { id: "aa".into() }).ok);
    d.handle(Event::Redial { id: "aa".into() });
    let mut theirs = Config { clock: d.cfg.clock, ..Config::default() };
    theirs.set_node("bb", "aa", "127.0.0.1:1", false);
    d.on_config(&theirs.shared);
    assert!(d.cfg.trusted("aa").is_some());
    assert_eq!(d.dialling, HashSet::from(["aa".to_string()]));
}

#[tokio::test]
async fn node_being_dialled_is_not_dialled_again_until_the_dial_is_done() {
    let mut d = daemon();
    let me = d.me.clone();
    d.cfg.set_node(&me, "aa", "127.0.0.1:1", false);
    d.backoff.insert("aa".into(), Duration::from_secs(1));
    let (tx, mut rx) = mpsc::unbounded_channel();
    d.tx = tx;
    d.handle(Event::Redial { id: "aa".into() });
    d.handle(Event::Redial { id: "aa".into() });
    let done = rx.recv().await.unwrap();
    assert!(matches!(done, Event::DialDone { .. }));
    assert!(timeout(Duration::from_millis(200), rx.recv()).await.is_err());
    d.handle(done);
    d.handle(Event::Redial { id: "aa".into() });
    assert!(matches!(rx.recv().await, Some(Event::DialDone { .. })));
}

#[test]
fn tick_saves_the_index_only_after_it_changed() {
    let mut d = daemon();
    let dir = test_dir("tick-save");
    std::fs::create_dir_all(&dir).unwrap();
    d.index_file = dir.join("index.bin");
    d.handle(Event::Tick);
    assert!(!d.index_file.exists());
    d.set_entry("~/s/a", meta(0, 0));
    d.handle(Event::Tick);
    assert_eq!(index::load(&d.index_file).get("~/s/a"), Some(&meta(0, 0)));
    std::fs::remove_file(&d.index_file).unwrap();
    d.handle(Event::Tick);
    assert!(!d.index_file.exists());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn tick_rescans_active_roots_only_every_rescan_ticks() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("tick-rescan");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("f.txt"), "x").unwrap();
    d.index_file = dir.join("index.bin");
    d.cfg.set_root(&me, dir.to_str().unwrap(), true, false);
    for _ in 1..RESCAN_TICKS {
        d.handle(Event::Tick);
    }
    assert!(d.index.is_empty());
    d.handle(Event::Tick);
    assert!(d.index.contains_key(dir.join("f.txt").to_str().unwrap()));
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn only_the_current_connection_going_down_marks_the_node_offline() {
    let mut d = daemon();
    let me = d.me.clone();
    d.cfg.set_node(&me, "aa", "h:1", false);
    let _rx = up(&mut d, "aa", 1);
    d.handle(Event::Down { id: "aa".into(), conn: 2 });
    assert!(d.on_ctl(&Req::ListNodes).text.contains("  connected"));
    d.handle(Event::Down { id: "aa".into(), conn: 1 });
    assert!(d.on_ctl(&Req::ListNodes).text.contains("  offline"));
}

#[tokio::test]
async fn node_that_goes_down_is_redialled_after_a_second() {
    let mut d = daemon();
    let me = d.me.clone();
    d.cfg.set_node(&me, "aa", "h:1", false);
    let _rx = up(&mut d, "aa", 1);
    let (tx, mut rx) = mpsc::unbounded_channel();
    d.tx = tx;
    d.handle(Event::Down { id: "aa".into(), conn: 1 });
    assert!(timeout(Duration::from_millis(900), rx.recv()).await.is_err());
    let ev = timeout(Duration::from_millis(500), rx.recv()).await.unwrap();
    assert!(matches!(ev, Some(Event::Redial { id }) if id == "aa"));
}

#[tokio::test]
async fn dialled_node_that_goes_down_is_redialled_once_after_a_second() {
    let mut d = daemon();
    let me = d.me.clone();
    d.cfg.set_node(&me, "aa", "h:1", false);
    d.dialling.insert("aa".into());
    let _rx = up(&mut d, "aa", 1);
    let (tx, mut rx) = mpsc::unbounded_channel();
    d.tx = tx;
    d.handle(Event::Down { id: "aa".into(), conn: 1 });
    d.handle(Event::DialDone { id: "aa".into() });
    assert!(timeout(Duration::from_millis(900), rx.recv()).await.is_err());
    let ev = timeout(Duration::from_millis(500), rx.recv()).await.unwrap();
    assert!(matches!(ev, Some(Event::Redial { id }) if id == "aa"));
    assert!(timeout(Duration::from_millis(1000), rx.recv()).await.is_err());
}

#[tokio::test]
async fn failed_dials_wait_twice_as_long_each_time_up_to_a_limit_until_connected() {
    let mut d = daemon();
    let me = d.me.clone();
    d.cfg.set_node(&me, "aa", "h:1", false);
    let mut waits = Vec::new();
    for _ in 0..7 {
        d.handle(Event::DialDone { id: "aa".into() });
        waits.push(d.backoff["aa"].as_secs());
    }
    assert_eq!(waits, [1, 2, 4, 5, 5, 5, 5]);
    assert_eq!(MAX_BACKOFF, Duration::from_secs(5));
    up(&mut d, "aa", 1);
    d.handle(Event::DialDone { id: "aa".into() });
    assert!(d.backoff.is_empty());
}

#[tokio::test]
async fn going_down_cancels_only_the_downloads_from_that_node() {
    let mut d = daemon();
    let me = d.me.clone();
    let mut tmps = Vec::new();
    for (id, conn) in [("aa", 1), ("bb", 2)] {
        d.cfg.set_node(&me, id, "h:1", false);
        up(&mut d, id, conn);
        let tmp = test_dir(&format!("down-{id}"));
        d.downloads.insert(format!("~/s/{id}"), download(id, meta(0, 0), &tmp));
        tmps.push(tmp);
    }
    d.handle(Event::Down { id: "aa".into(), conn: 1 });
    assert!(!tmps[0].exists());
    assert!(tmps[1].exists());
    d.cancel_download("~/s/bb");
}

#[tokio::test]
async fn download_from_a_node_that_goes_down_is_requested_from_another_node_that_offered_it() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("download-other-source");
    let root = dir.join("s");
    std::fs::create_dir_all(&root).unwrap();
    let rp = root.to_str().unwrap().to_string();
    d.cfg.set_root(&me, &rp, true, false);
    for id in ["aa", "bb"] {
        d.cfg.set_node(&me, id, "h:1", false);
    }
    let (_a, mut b) = (up(&mut d, "aa", 1), up(&mut d, "bb", 2));
    while b.try_recv().is_ok() {}
    let p = format!("{rp}/f");
    let r = FileMeta { vv: Vv(vec![(1, 1)]), ..meta(0, 1) };
    d.on_entry("aa", Entry { path: p.clone(), meta: r.clone() });
    d.on_entry("bb", Entry { path: p.clone(), meta: r });
    assert!(b.try_recv().is_err());
    d.handle(Event::Down { id: "aa".into(), conn: 1 });
    d.pump_queue();
    assert!(matches!(b.try_recv(), Ok(Msg::Request { path, .. }) if path == p));
    assert_eq!(d.downloads[&p].from, "bb");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn queued_download_from_a_node_that_goes_down_is_requested_from_another_node_that_offered_it() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("queued-other-source");
    let root = dir.join("s");
    std::fs::create_dir_all(&root).unwrap();
    let rp = root.to_str().unwrap().to_string();
    d.cfg.set_root(&me, &rp, true, false);
    for id in ["aa", "bb"] {
        d.cfg.set_node(&me, id, "h:1", false);
    }
    let (mut a, mut b) = (up(&mut d, "aa", 1), up(&mut d, "bb", 2));
    while a.try_recv().is_ok() {}
    while b.try_recv().is_ok() {}
    for n in 0..MAX_DOWNLOADS {
        d.downloads.insert(format!("{rp}/{n}"), download("aa", meta(0, 0), &dir.join(format!("t{n}"))));
    }
    let p = format!("{rp}/f");
    let r = FileMeta { vv: Vv(vec![(1, 1)]), ..meta(0, 1) };
    for from in ["aa", "bb"] {
        d.on_entry(from, Entry { path: p.clone(), meta: r.clone() });
    }
    d.downloads.clear();
    d.handle(Event::Down { id: "bb".into(), conn: 2 });
    d.pump_queue();
    assert!(matches!(a.try_recv(), Ok(Msg::Request { path, .. }) if path == p));
    assert_eq!(d.downloads[&p].from, "aa");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn queued_download_is_kept_when_another_node_offers_a_concurrent_version_with_the_same_content_and_goes_down() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("queued-same-content");
    let root = dir.join("s");
    std::fs::create_dir_all(&root).unwrap();
    let rp = root.to_str().unwrap().to_string();
    d.cfg.set_root(&me, &rp, true, false);
    for id in ["aa", "bb"] {
        d.cfg.set_node(&me, id, "h:1", false);
    }
    let (mut a, mut b) = (up(&mut d, "aa", 1), up(&mut d, "bb", 2));
    while a.try_recv().is_ok() {}
    while b.try_recv().is_ok() {}
    for n in 0..MAX_DOWNLOADS {
        d.downloads.insert(format!("{rp}/{n}"), download("aa", meta(0, 0), &dir.join(format!("t{n}"))));
    }
    let p = format!("{rp}/f");
    d.on_entry("aa", Entry { path: p.clone(), meta: FileMeta { vv: Vv(vec![(1, 1)]), ..meta(0, 1) } });
    d.on_entry("bb", Entry { path: p.clone(), meta: FileMeta { vv: Vv(vec![(2, 1)]), ..meta(5, 2) } });
    d.downloads.clear();
    d.handle(Event::Down { id: "bb".into(), conn: 2 });
    d.pump_queue();
    assert!(matches!(a.try_recv(), Ok(Msg::Request { path, .. }) if path == p));
    assert_eq!(d.downloads[&p].from, "aa");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn queued_download_that_starts_and_loses_its_node_is_requested_from_another_node_that_offered_it() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("started-other-source");
    let root = dir.join("s");
    std::fs::create_dir_all(&root).unwrap();
    let rp = root.to_str().unwrap().to_string();
    d.cfg.set_root(&me, &rp, true, false);
    for id in ["aa", "bb"] {
        d.cfg.set_node(&me, id, "h:1", false);
    }
    let (mut a, mut b) = (up(&mut d, "aa", 1), up(&mut d, "bb", 2));
    while a.try_recv().is_ok() {}
    while b.try_recv().is_ok() {}
    for n in 0..MAX_DOWNLOADS {
        d.downloads.insert(format!("{rp}/{n}"), download("aa", meta(0, 0), &dir.join(format!("t{n}"))));
    }
    let p = format!("{rp}/f");
    let r = FileMeta { vv: Vv(vec![(1, 1)]), ..meta(0, 1) };
    for from in ["aa", "bb"] {
        d.on_entry(from, Entry { path: p.clone(), meta: r.clone() });
    }
    d.downloads.clear();
    d.pump_queue();
    assert!(matches!(a.try_recv(), Ok(Msg::Request { path, .. }) if path == p));
    d.handle(Event::Down { id: "aa".into(), conn: 1 });
    d.pump_queue();
    assert!(matches!(b.try_recv(), Ok(Msg::Request { path, .. }) if path == p));
    assert_eq!(d.downloads[&p].from, "bb");
    std::fs::remove_dir_all(&dir).unwrap();
}
