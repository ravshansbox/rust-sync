use super::Daemon;
use crate::index;
use crate::net::{self, Msg};
use crate::paths;
use std::fs::File;
use std::io::Read;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{Semaphore, mpsc};

impl Daemon {
    pub(super) fn on_request(&self, from: &str, path: String, hash: [u8; 32]) {
        let Some(peer) = self.peers.get(from) else { return };
        let root = self.local_root(&path);
        let ok = root.is_some() && !self.not_a_synced_file(&path) && self.index.get(&path).is_some_and(|m| m.hash == Some(hash));
        let safe = |lp: &Path| index::stat(lp).is_some() && !root.as_ref().is_some_and(|r| paths::through_symlink(lp, r));
        match paths::to_local(&path) {
            Some(lp) if ok && safe(&lp) => drop(tokio::spawn(upload(lp, root, path, hash, peer.data.clone(), self.uploads.clone()))),
            _ => drop(peer.ctl.send(Msg::Unavailable { path, hash })),
        }
    }}

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
