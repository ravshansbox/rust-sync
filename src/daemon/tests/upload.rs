use super::*;
use crate::daemon::upload::upload;

#[tokio::test]
async fn upload_sends_file_in_chunks_then_eof_or_unavailable_when_missing() {
    let lp = test_dir("upload");
    let data: Vec<u8> = (0..net::CHUNK + 5).map(|i| i as u8).collect();
    std::fs::write(&lp, &data).unwrap();
    let (tx, mut rx) = mpsc::channel(8);
    upload(lp.clone(), None, "~/f".into(), [2; 32], tx.clone(), Arc::new(Semaphore::new(1))).await;
    let mut got = Vec::new();
    for (want_offset, want_len, want_eof) in [(0, net::CHUNK, false), (net::CHUNK, 5, false), (net::CHUNK + 5, 0, true)] {
        let Some(Msg::Data { path, hash, offset, bytes, eof }) = rx.recv().await else { panic!() };
        assert_eq!((path.as_str(), hash, offset, bytes.len(), eof), ("~/f", [2; 32], want_offset as u64, want_len, want_eof));
        got.extend(bytes);
    }
    assert_eq!(got, data);
    std::fs::remove_file(&lp).unwrap();
    upload(lp, None, "~/f".into(), [2; 32], tx, Arc::new(Semaphore::new(1))).await;
    assert!(matches!(rx.recv().await, Some(Msg::Unavailable { path, .. }) if path == "~/f"));
    assert!(rx.recv().await.is_none());
}

#[tokio::test]
async fn upload_waits_for_a_free_slot() {
    let lp = test_dir("upload-slot");
    std::fs::write(&lp, "x").unwrap();
    let slots = Arc::new(Semaphore::new(0));
    let (tx, mut rx) = mpsc::channel(8);
    let task = tokio::spawn(upload(lp.clone(), None, "~/f".into(), [2; 32], tx, slots.clone()));
    assert!(timeout(Duration::from_millis(100), rx.recv()).await.is_err());
    slots.add_permits(1);
    assert!(matches!(rx.recv().await, Some(Msg::Data { offset: 0, eof: false, .. })));
    task.await.unwrap();
    std::fs::remove_file(&lp).unwrap();
}

#[tokio::test]
async fn waiting_upload_of_a_file_replaced_by_a_symlink_is_refused() {
    let dir = test_dir("upload-swap");
    let (root, outside) = (dir.join("s"), dir.join("secret"));
    std::fs::create_dir_all(root.join("d")).unwrap();
    std::fs::write(&outside, "secret").unwrap();
    for lp in [root.join("f"), root.join("d/g")] {
        std::fs::write(&lp, "x").unwrap();
        let slots = Arc::new(Semaphore::new(0));
        let (tx, mut rx) = mpsc::channel(8);
        let task = tokio::spawn(upload(lp.clone(), Some(root.clone()), "~/f".into(), [2; 32], tx, slots.clone()));
        tokio::task::yield_now().await;
        if lp.ends_with("f") {
            std::fs::remove_file(&lp).unwrap();
            std::os::unix::fs::symlink(&outside, &lp).unwrap();
        } else {
            std::fs::rename(root.join("d"), dir.join("moved")).unwrap();
            std::os::unix::fs::symlink(&dir, root.join("d")).unwrap();
            std::fs::write(dir.join("g"), "secret").unwrap();
        }
        slots.add_permits(1);
        assert!(matches!(rx.recv().await, Some(Msg::Unavailable { .. })), "{}", lp.display());
        task.await.unwrap();
    }
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
