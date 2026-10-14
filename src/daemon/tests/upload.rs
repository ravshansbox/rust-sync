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
