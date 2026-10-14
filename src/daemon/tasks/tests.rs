use super::*;

#[tokio::test]
async fn debounce_merges_paths_and_turns_none_into_rescan() {
    let (in_tx, in_rx) = mpsc::unbounded_channel();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel();
    let task = tokio::spawn(debounce(in_rx, out_tx));
    for p in ["/a", "/b", "/a"] {
        in_tx.send(Some(PathBuf::from(p))).unwrap();
    }
    let Some(Event::Fs(mut got)) = out_rx.recv().await else { panic!() };
    got.sort();
    assert_eq!(got, [PathBuf::from("/a"), PathBuf::from("/b")]);
    in_tx.send(Some(PathBuf::from("/c"))).unwrap();
    in_tx.send(None).unwrap();
    assert!(matches!(out_rx.recv().await, Some(Event::Rescan)));
    drop(in_tx);
    task.await.unwrap();
    assert!(out_rx.recv().await.is_none());
}

#[tokio::test]
async fn debounce_sends_paths_after_the_longest_wait_even_while_changes_keep_coming() {
    let (in_tx, in_rx) = mpsc::unbounded_channel();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel();
    drop(tokio::spawn(debounce(in_rx, out_tx)));
    let feed = tokio::spawn(async move {
        while in_tx.send(Some(PathBuf::from("/a"))).is_ok() {
            tokio::time::sleep(SETTLE / 5).await;
        }
    });
    let got = timeout(SETTLE_MAX + SETTLE, out_rx.recv()).await;
    feed.abort();
    assert!(matches!(got, Ok(Some(Event::Fs(_)))));
}
