use super::{Event, TICK};
use crate::ctl::Resp;
use notify::RecommendedWatcher;
use std::collections::HashSet;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

/// Wait this long after the last change before reading a file.
pub(super) const SETTLE: Duration = Duration::from_millis(500);
/// ...but never longer than this, for files that change all the time.
pub(super) const SETTLE_MAX: Duration = Duration::from_secs(3);

/// Collect watcher events until things go quiet, then pass them on as one batch.
pub(super) async fn debounce(mut rx: mpsc::UnboundedReceiver<Option<PathBuf>>, tx: mpsc::UnboundedSender<Event>) {
    while let Some(first) = rx.recv().await {
        let (mut paths, mut rescan) = (HashSet::new(), false);
        let mut add = |p: Option<PathBuf>| match p {
            Some(p) => drop(paths.insert(p)),
            None => rescan = true,
        };
        add(first);
        let deadline = Instant::now() + SETTLE_MAX;
        while Instant::now() < deadline {
            match timeout(SETTLE, rx.recv()).await {
                Ok(Some(p)) => add(p),
                Ok(None) => return,
                Err(_) => break,
            }
        }
        let ev = if rescan { Event::Rescan } else { Event::Fs(paths.into_iter().collect()) };
        if tx.send(ev).is_err() {
            return;
        }
    }
}

/// Start the file watcher. It runs on its own thread and hands raw paths to `debounce`.
pub(super) fn watch(tx: mpsc::UnboundedSender<Event>) -> notify::Result<RecommendedWatcher> {
    let (fs_tx, fs_rx) = mpsc::unbounded_channel::<Option<PathBuf>>();
    let watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| match res {
        Ok(ev) if !ev.need_rescan() => ev.paths.into_iter().for_each(|p| drop(fs_tx.send(Some(p)))),
        _ => drop(fs_tx.send(None)),
    })?;
    tokio::spawn(debounce(fs_rx, tx));
    Ok(watcher)
}

/// Bind the control socket, replacing any stale one, readable only by this user.
pub(super) fn ctl_listen(sock: &Path) -> std::io::Result<UnixListener> {
    let _ = std::fs::remove_file(sock);
    let l = UnixListener::bind(sock)?;
    std::fs::set_permissions(sock, std::fs::Permissions::from_mode(0o600))?;
    Ok(l)
}

/// Answer CLI requests, one JSON line per connection.
pub(super) async fn ctl_loop(l: UnixListener, tx: mpsc::UnboundedSender<Event>) {
    loop {
        let Ok((s, _)) = l.accept().await else {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        };
        tokio::spawn(ctl_conn(s, tx.clone()));
    }
}

async fn ctl_conn(s: UnixStream, tx: mpsc::UnboundedSender<Event>) -> anyhow::Result<()> {
    let (r, mut w) = s.into_split();
    let mut line = String::new();
    BufReader::new(r).read_line(&mut line).await?;
    let resp = match serde_json::from_str(&line) {
        Ok(req) => {
            let (reply, rx) = oneshot::channel();
            tx.send(Event::Ctl(req, reply))?;
            rx.await?
        }
        Err(e) => Resp {
            ok: false,
            text: format!(
                "the daemon cannot read this request ({e}). Is it running a different version? Try `rust-sync service restart`."
            ),
        },
    };
    let mut out = serde_json::to_string(&resp)?;
    out.push('\n');
    w.write_all(out.as_bytes()).await?;
    Ok(())
}

/// Send a tick every `TICK`, starting one `TICK` from now.
pub(super) async fn tick_loop(tx: mpsc::UnboundedSender<Event>) {
    let mut iv = tokio::time::interval(TICK);
    iv.tick().await;
    while tx.send(Event::Tick).is_ok() {
        iv.tick().await;
    }
}

/// Ask the daemon to stop on Ctrl-C or SIGTERM.
pub(super) async fn shutdown_on_signal(tx: mpsc::UnboundedSender<Event>) {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("signal handler");
    tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
    let _ = tx.send(Event::Shutdown);
}

#[cfg(test)]
mod tests {
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
}
