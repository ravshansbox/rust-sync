use super::Event;
use crate::ctl::Resp;
use crate::net::{self, Msg};
use crate::paths;
use std::collections::HashSet;
use std::fs::File;
use std::io::Read;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::sync::{Semaphore, mpsc, oneshot};
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

/// Answer CLI requests, one JSON line per connection.
pub(super) async fn ctl_loop(l: UnixListener, tx: mpsc::UnboundedSender<Event>) {
    loop {
        let Ok((s, _)) = l.accept().await else {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        };
        let tx = tx.clone();
        tokio::spawn(async move {
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
            anyhow::Ok(())
        });
    }
}

/// Send one file in chunks. Waits when the peer's send queue is full.
pub(super) async fn upload(lp: PathBuf, root: Option<PathBuf>, path: String, hash: [u8; 32], tx: mpsc::Sender<Msg>, slots: Arc<Semaphore>) {
    let Ok(_slot) = slots.acquire().await else { return };
    let opened = File::open(&lp).ok().filter(|f| {
        let (Ok(a), Ok(b)) = (f.metadata(), std::fs::symlink_metadata(&lp)) else { return false };
        b.is_file() && (a.dev(), a.ino()) == (b.dev(), b.ino()) && !root.as_ref().is_some_and(|r| paths::through_symlink(&lp, r))
    });
    let Some(mut f) = opened else {
        let _ = tx.send(Msg::Unavailable { path, hash }).await;
        return;
    };
    let mut buf = vec![0u8; net::CHUNK];
    let mut offset = 0u64;
    loop {
        let msg = match f.read(&mut buf) {
            Ok(0) => Msg::Data { path: path.clone(), hash, offset, bytes: Vec::new(), eof: true },
            Ok(n) => Msg::Data { path: path.clone(), hash, offset, bytes: buf[..n].to_vec(), eof: false },
            Err(_) => Msg::Unavailable { path: path.clone(), hash },
        };
        let done = !matches!(msg, Msg::Data { eof: false, .. });
        if let Msg::Data { bytes, .. } = &msg {
            offset += bytes.len() as u64;
        }
        if tx.send(msg).await.is_err() || done {
            return;
        }
    }
}
