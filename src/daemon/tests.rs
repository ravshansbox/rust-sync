use super::*;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use crate::index::Vv;
use tokio::time::timeout;

mod apply;
mod conn;
mod ctl;
mod download;
mod nodes;
mod scan;
mod upload;
mod watch;

fn meta(mtime: i64, by: u64) -> FileMeta {
    FileMeta { hash: Some([1; 32]), size: 1, mtime, mode: 0o644, vv: Vv::default(), by }
}

fn download(from: &str, meta: FileMeta, tmp: &Path) -> Download {
    let file = File::create(tmp).unwrap();
    Download { from: from.into(), meta, tmp: tmp.to_path_buf(), file, hasher: blake3::Hasher::new(), written: 0, alts: Vec::new() }
}

fn test_dir(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("rust-sync-{name}-{}", std::process::id()))
}

fn up(d: &mut Daemon, id: &str, conn: u64) -> mpsc::UnboundedReceiver<Msg> {
    let (ctl, rx) = mpsc::unbounded_channel();
    d.handle(Event::Up { id: id.into(), conn, outbound: true, ctl, data: mpsc::channel(1).0 });
    rx
}

fn daemon() -> Daemon {
    let keys = Arc::new(Keys { private: vec![0; 32], public: vec![1; 32] });
    let me = keys.id();
    Daemon {
        me_short: net::short_id(&me),
        me,
        keys,
        cfg: Config::default(),
        config_file: test_dir("no-state").join("config.json"),
        index: Index::new(),
        index_file: test_dir("no-state").join("index.bin"),
        peers: HashMap::new(),
        dialling: HashSet::new(),
        backoff: HashMap::new(),
        pending: BTreeMap::new(),
        pending_order: VecDeque::new(),
        downloads: HashMap::new(),
        queued: BTreeMap::new(),
        uploads: Arc::new(Semaphore::new(MAX_UPLOADS)),
        watcher: notify::recommended_watcher(|_: notify::Result<notify::Event>| {}).unwrap(),
        watched: HashMap::new(),
        tx: mpsc::unbounded_channel().0,
        outbox: Vec::new(),
        index_dirty: false,
        ticks: 0,
    }
}

#[test]
fn file_with_a_name_of_the_longest_allowed_length_is_downloaded() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("download-long-name");
    let root = dir.join("s");
    std::fs::create_dir_all(&root).unwrap();
    let rp = root.to_str().unwrap().to_string();
    d.cfg.set_root(&me, &rp, true, false);
    d.cfg.set_node(&me, "aa", "h:1", false);
    let mut a = up(&mut d, "aa", 1);
    while a.try_recv().is_ok() {}
    let p = format!("{rp}/{}", "n".repeat(255));
    let hash = *blake3::hash(b"abc").as_bytes();
    let r = FileMeta { hash: Some(hash), size: 3, vv: Vv(vec![(1, 1)]), ..meta(0, 1) };
    d.on_entry("aa", Entry { path: p.clone(), meta: r });
    assert!(matches!(a.try_recv(), Ok(Msg::Request { path, .. }) if path == p));
    let msg = Msg::Data { path: p.clone(), hash, offset: 0, bytes: b"abc".to_vec(), eof: true };
    d.handle(Event::Peer { id: "aa".into(), conn: 1, msg });
    assert_eq!(std::fs::read(&p).unwrap(), b"abc");
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn index_from_a_peer_requests_every_new_file_it_lists() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("index-msg");
    std::fs::create_dir_all(&dir).unwrap();
    let rp = dir.to_str().unwrap().to_string();
    d.cfg.set_root(&me, &rp, true, false);
    d.cfg.set_node(&me, "aa", "h:1", false);
    let mut a = up(&mut d, "aa", 1);
    while a.try_recv().is_ok() {}
    let paths = [format!("{rp}/a"), format!("{rp}/b")];
    let r = FileMeta { vv: Vv(vec![(1, 1)]), ..meta(0, 1) };
    let entries = paths.iter().map(|p| Entry { path: p.clone(), meta: r.clone() }).collect();
    d.handle(Event::Peer { id: "aa".into(), conn: 1, msg: Msg::Index(entries) });
    for p in &paths {
        assert!(matches!(a.try_recv(), Ok(Msg::Request { path, hash }) if path == *p && hash == [1; 32]));
    }
    assert!(a.try_recv().is_err());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn remote_file_inside_a_folder_that_is_a_local_file_is_not_requested() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("file-blocks-folder");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("a"), "local").unwrap();
    let rp = dir.to_str().unwrap().to_string();
    d.cfg.set_root(&me, &rp, true, false);
    d.cfg.set_node(&me, "aa", "h:1", false);
    let mut a = up(&mut d, "aa", 1);
    while a.try_recv().is_ok() {}
    let r = FileMeta { vv: Vv(vec![(1, 1)]), ..meta(0, 1) };
    d.handle(Event::Peer { id: "aa".into(), conn: 1, msg: Msg::Index(vec![Entry { path: format!("{rp}/a/b"), meta: r }]) });
    assert!(a.try_recv().is_err());
    assert!(d.downloads.is_empty());
    assert_eq!(std::fs::read_to_string(dir.join("a")).unwrap(), "local");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn cancelled_download_removes_the_folders_it_created() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("cancel-removes-folders");
    std::fs::create_dir_all(&dir).unwrap();
    let rp = dir.to_str().unwrap().to_string();
    d.cfg.set_root(&me, &rp, true, false);
    d.cfg.set_node(&me, "aa", "h:1", false);
    let mut a = up(&mut d, "aa", 1);
    while a.try_recv().is_ok() {}
    let r = FileMeta { vv: Vv(vec![(1, 1)]), ..meta(0, 1) };
    d.handle(Event::Peer { id: "aa".into(), conn: 1, msg: Msg::Index(vec![Entry { path: format!("{rp}/a/b/f"), meta: r }]) });
    assert!(dir.join("a/b").is_dir());
    let resp = d.on_ctl(&Req::AddPath { path: format!("{rp}/a"), is_dir: false });
    assert!(resp.ok, "{}", resp.text);
    assert!(d.downloads.is_empty());
    assert!(!dir.join("a/b").exists());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn shutdown_removes_running_downloads_and_the_folders_they_created() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("shutdown-removes-folders");
    std::fs::create_dir_all(&dir).unwrap();
    let rp = dir.to_str().unwrap().to_string();
    d.cfg.set_root(&me, &rp, true, false);
    d.cfg.set_node(&me, "aa", "h:1", false);
    let mut a = up(&mut d, "aa", 1);
    while a.try_recv().is_ok() {}
    let r = FileMeta { vv: Vv(vec![(1, 1)]), ..meta(0, 1) };
    d.handle(Event::Peer { id: "aa".into(), conn: 1, msg: Msg::Index(vec![Entry { path: format!("{rp}/a/b/f"), meta: r }]) });
    assert!(dir.join("a/b/.f.rsync-tmp").is_file());
    d.handle(Event::Shutdown);
    assert!(d.downloads.is_empty());
    assert!(!dir.join("a").exists());
    assert!(dir.is_dir());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn request_is_refused_outside_shared_folders_or_for_another_version() {
    let mut d = daemon();
    let me = d.me.clone();
    d.cfg.set_node(&me, "aa", "h:1", false);
    d.cfg.set_root(&me, "~/s", true, false);
    d.index.insert("~/s/f".into(), meta(0, 0));
    d.index.insert("~/x".into(), meta(0, 0));
    let mut rx = up(&mut d, "aa", 1);
    while rx.try_recv().is_ok() {}
    for (path, hash) in [("~/s/f", [2; 32]), ("~/x", [1; 32])] {
        d.handle(Event::Peer { id: "aa".into(), conn: 1, msg: Msg::Request { path: path.into(), hash } });
        assert!(matches!(rx.try_recv(), Ok(Msg::Unavailable { path: p, .. }) if p == path));
    }
}

#[tokio::test]
async fn request_is_refused_for_a_file_below_a_synced_file_path() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("request-below-file-root");
    std::fs::create_dir_all(dir.join("s/f")).unwrap();
    std::fs::write(dir.join("s/f/x"), "abc").unwrap();
    let rp = dir.join("s").to_str().unwrap().to_string();
    d.cfg.set_root(&me, &rp, true, false);
    d.cfg.set_root(&me, &format!("{rp}/f"), false, false);
    d.cfg.set_node(&me, "aa", "h:1", false);
    let hash = *blake3::hash(b"abc").as_bytes();
    let path = format!("{rp}/f/x");
    d.index.insert(path.clone(), FileMeta { hash: Some(hash), size: 3, ..meta(0, 0) });
    let mut rx = up(&mut d, "aa", 1);
    while rx.try_recv().is_ok() {}
    d.handle(Event::Peer { id: "aa".into(), conn: 1, msg: Msg::Request { path: path.clone(), hash } });
    assert!(matches!(rx.try_recv(), Ok(Msg::Unavailable { path: p, .. }) if p == path));
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn request_is_refused_for_a_file_reached_through_a_symlink() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("request-symlink");
    let root = dir.join("s");
    std::fs::create_dir_all(root.join("d")).unwrap();
    std::fs::write(dir.join("secret"), "abc").unwrap();
    let rp = root.to_str().unwrap().to_string();
    d.cfg.set_root(&me, &rp, true, false);
    d.cfg.set_node(&me, "aa", "h:1", false);
    let hash = *blake3::hash(b"abc").as_bytes();
    let synced = FileMeta { hash: Some(hash), size: 3, ..meta(0, 0) };
    d.index.insert(format!("{rp}/d/f"), synced.clone());
    d.index.insert(format!("{rp}/g"), synced);
    std::fs::rename(root.join("d"), dir.join("out")).unwrap();
    std::fs::copy(dir.join("secret"), dir.join("out/f")).unwrap();
    std::os::unix::fs::symlink(dir.join("out"), root.join("d")).unwrap();
    std::os::unix::fs::symlink(dir.join("secret"), root.join("g")).unwrap();
    let mut rx = up(&mut d, "aa", 1);
    while rx.try_recv().is_ok() {}
    for path in [format!("{rp}/d/f"), format!("{rp}/g")] {
        d.handle(Event::Peer { id: "aa".into(), conn: 1, msg: Msg::Request { path: path.clone(), hash } });
        assert!(matches!(rx.try_recv(), Ok(Msg::Unavailable { path: p, .. }) if p == path), "{path}");
    }
    std::fs::remove_dir_all(&dir).unwrap();
}

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
fn status_counts_only_live_files_in_shared_folders_and_queued_transfers() {
    let mut d = daemon();
    let me = d.me.clone();
    d.cfg.set_root(&me, "~/s", true, false);
    d.index.insert("~/s/a".into(), meta(0, 0));
    d.index.insert("~/s/b".into(), meta(0, 0));
    d.index.insert("~/s/gone".into(), FileMeta { hash: None, ..meta(0, 0) });
    d.index.insert("~/x".into(), meta(0, 0));
    d.queued.insert("~/s/c".into(), ("aa".into(), meta(0, 0), Vec::new()));
    let text = d.on_ctl(&Req::Status).text;
    assert!(text.contains("\nFiles tracked: 2\nTransfers: 0 active, 1 queued\n"), "{text}");
}

#[test]
fn status_does_not_count_queued_transfers_for_paths_no_longer_synced() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("status-queued-unsynced");
    let (s, t) = (format!("{}/s", dir.display()), format!("{}/t", dir.display()));
    d.cfg.set_root(&me, &s, true, false);
    d.cfg.set_root(&me, &t, true, false);
    for p in [format!("{s}/a"), format!("{s}/d/b"), format!("{t}/c")] {
        d.queued.insert(p, ("aa".into(), meta(0, 0), Vec::new()));
    }
    assert!(d.on_ctl(&Req::AddPath { path: format!("{s}/d"), is_dir: false }).ok);
    assert!(d.on_ctl(&Req::RemovePath { path: t }).ok);
    let text = d.on_ctl(&Req::Status).text;
    assert!(text.contains("\nTransfers: 0 active, 1 queued\n"), "{text}");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn status_does_not_count_entries_left_where_nested_synced_paths_now_are() {
    let mut d = daemon();
    let me = d.me.clone();
    d.cfg.set_root(&me, "~/s", true, false);
    d.cfg.set_root(&me, "~/s/d", true, false);
    d.cfg.set_root(&me, "~/s/f", false, false);
    d.index.insert("~/s/a".into(), meta(0, 0));
    d.index.insert("~/s/d".into(), meta(0, 0));
    d.index.insert("~/s/f".into(), meta(0, 0));
    d.index.insert("~/s/f/x".into(), meta(0, 0));
    let text = d.on_ctl(&Req::Status).text;
    assert!(text.contains("\nFiles tracked: 2\n"), "{text}");
}

#[test]
fn rejected_request_replies_with_the_error_and_changes_nothing() {
    let mut d = daemon();
    let me = d.me.clone();
    d.cfg.set_node(&me, "aa", "h:1", false);
    let mut rx = up(&mut d, "aa", 1);
    while rx.try_recv().is_ok() {}
    let clock = d.cfg.clock;
    let resp = d.on_ctl(&Req::RemovePath { path: "~/x".into() });
    assert_eq!((resp.ok, resp.text.as_str()), (false, "~/x is not synced"));
    let resp = d.on_ctl(&Req::RemoveNode { id: "bb".into() });
    assert_eq!((resp.ok, resp.text.as_str()), (false, "No such node"));
    assert_eq!(d.cfg.clock, clock);
    assert!(rx.try_recv().is_err());
}

#[test]
fn added_path_is_saved_shared_with_peers_and_its_known_entries_are_sent() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("add-path");
    std::fs::create_dir_all(dir.join("s")).unwrap();
    d.config_file = dir.join("config.json");
    let root = dir.join("s").to_str().unwrap().to_string();
    let known = dir.join("s/gone.txt").to_str().unwrap().to_string();
    d.cfg.set_node(&me, "aa", "h:1", false);
    d.index.insert(known.clone(), FileMeta { hash: None, ..meta(0, 2) });
    d.index.insert(dir.join("x").to_str().unwrap().into(), meta(0, 2));
    let mut rx = up(&mut d, "aa", 1);
    while rx.try_recv().is_ok() {}
    let resp = d.on_ctl(&Req::AddPath { path: root.clone(), is_dir: true });
    assert!(resp.ok, "{}", resp.text);
    assert!(Config::load(&d.config_file).unwrap().active_root(&root).is_some());
    assert!(matches!(rx.try_recv(), Ok(Msg::Config(c)) if c.roots.contains_key(&root)));
    assert_eq!(d.outbox.iter().map(|e| e.path.as_str()).collect::<Vec<_>>(), [known.as_str()]);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn files_in_a_folder_are_recorded_when_a_synced_file_path_is_added_again_as_a_folder() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("file-root-becomes-folder");
    std::fs::create_dir_all(&dir).unwrap();
    d.config_file = dir.join("config.json");
    let root = dir.join("x").to_str().unwrap().to_string();
    std::fs::write(&root, "a").unwrap();
    d.cfg.set_root(&me, &root, false, false);
    d.check_tree(&root);
    std::fs::remove_file(&root).unwrap();
    std::fs::create_dir(&root).unwrap();
    std::fs::write(dir.join("x/f"), "b").unwrap();
    d.check_tree(&root);
    let resp = d.on_ctl(&Req::AddPath { path: root.clone(), is_dir: true });
    assert!(resp.ok, "{}", resp.text);
    let f = dir.join("x/f").to_str().unwrap().to_string();
    assert!(d.index.get(&f).is_some_and(|m| !m.deleted()));
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn files_in_a_folder_are_recorded_and_sent_when_a_synced_file_path_inside_a_synced_folder_is_removed() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("nested-file-root-removed");
    std::fs::create_dir_all(dir.join("s/f")).unwrap();
    std::fs::write(dir.join("s/f/x"), "x").unwrap();
    d.config_file = dir.join("config.json");
    let s = dir.join("s").to_str().unwrap().to_string();
    let f = dir.join("s/f").to_str().unwrap().to_string();
    d.cfg.set_root(&me, &s, true, false);
    d.cfg.set_root(&me, &f, false, false);
    d.check_tree(&s);
    assert!(d.index.is_empty());
    assert!(d.on_ctl(&Req::RemovePath { path: f.clone() }).ok);
    let x = format!("{f}/x");
    assert!(d.index.get(&x).is_some_and(|m| !m.deleted()));
    assert!(d.outbox.iter().any(|e| e.path == x));
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn deletion_of_a_synced_file_is_sent_before_the_files_of_the_folder_that_replaced_it() {
    let mut d = daemon();
    let me = d.me.clone();
    d.cfg.set_node(&me, "aa", "h:1", false);
    let mut rx = up(&mut d, "aa", 1);
    let dir = test_dir("file-replaced-by-folder-order");
    std::fs::create_dir_all(&dir).unwrap();
    let root = dir.join("x").to_str().unwrap().to_string();
    std::fs::write(&root, "a").unwrap();
    d.cfg.set_root(&me, &root, false, false);
    d.check_tree(&root);
    d.outbox.clear();
    std::fs::remove_file(&root).unwrap();
    std::fs::create_dir(&root).unwrap();
    std::fs::write(dir.join("x/f"), "b").unwrap();
    d.cfg.set_root(&me, &root, true, false);
    d.check_tree(&root);
    while rx.try_recv().is_ok() {}
    d.flush();
    let mut sent = Vec::new();
    while let Ok(Msg::Index(b)) = rx.try_recv() {
        sent.extend(b.into_iter().map(|e| e.path));
    }
    assert_eq!(sent, [root.clone(), format!("{root}/f")]);
    std::fs::remove_dir_all(&dir).unwrap();
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
