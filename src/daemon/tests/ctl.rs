use super::*;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;

#[tokio::test]
async fn ctl_loop_passes_request_to_daemon_and_sends_back_its_reply() {
    let sock = std::env::temp_dir().join(format!("rust-sync-ctl-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&sock);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let task = tokio::spawn(ctl_loop(UnixListener::bind(&sock).unwrap(), tx));
    let (r, mut w) = tokio::net::UnixStream::connect(&sock).await.unwrap().into_split();
    w.write_all(b"{\"RemoveNode\":{\"id\":\"abcd\"}}\n").await.unwrap();
    let Some(Event::Ctl(Req::RemoveNode { id }, reply)) = rx.recv().await else { panic!() };
    assert_eq!(id, "abcd");
    reply.send(Resp { ok: true, text: "done".into() }).unwrap();
    let mut line = String::new();
    BufReader::new(r).read_line(&mut line).await.unwrap();
    assert_eq!(line, "{\"ok\":true,\"text\":\"done\"}\n");
    task.abort();
    std::fs::remove_file(&sock).unwrap();
}

#[tokio::test]
async fn ctl_loop_rejects_a_request_it_cannot_read_instead_of_closing() {
    let sock = std::env::temp_dir().join(format!("rust-sync-ctl-bad-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&sock);
    let (tx, _rx) = mpsc::unbounded_channel();
    let task = tokio::spawn(ctl_loop(UnixListener::bind(&sock).unwrap(), tx));
    let (r, mut w) = tokio::net::UnixStream::connect(&sock).await.unwrap().into_split();
    w.write_all(b"{\"Pause\":{}}\n").await.unwrap();
    let mut line = String::new();
    BufReader::new(r).read_line(&mut line).await.unwrap();
    let resp: Resp = serde_json::from_str(&line).unwrap();
    assert!(!resp.ok);
    assert!(resp.text.starts_with("the daemon cannot read this request"), "{}", resp.text);
    task.abort();
    std::fs::remove_file(&sock).unwrap();
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
