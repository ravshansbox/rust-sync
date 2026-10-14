//! Commands from the CLI to a running daemon, over a Unix socket.
//! One JSON line in, one JSON line out.

use crate::config::Config;
use serde::{Deserialize, Serialize};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, MetadataExt};
use std::path::{Path, PathBuf};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

/// A request from the CLI to the daemon.
#[derive(Serialize, Deserialize, Debug)]
pub enum Req {
    /// Start syncing a file or folder, given as a portable path.
    AddPath { path: String, is_dir: bool },
    /// Stop syncing a portable path. The files stay on disk.
    RemovePath { path: String },
    /// Trust the node with this ID, reachable at `addr`.
    AddNode { id: String, addr: String },
    /// Stop trusting the node with this ID.
    RemoveNode { id: String },
    /// List the trusted nodes.
    ListNodes,
    /// Report the daemon's status. Also used to check that a daemon is running.
    Status,
}

/// The daemon's reply: whether the request worked, and the text to show.
#[derive(Serialize, Deserialize, Debug)]
pub struct Resp {
    /// Whether the request worked.
    pub ok: bool,
    /// The text to show to the user.
    pub text: String,
}

/// `<state dir>/ctl.sock`. Unix socket paths must be short (104 bytes on macOS),
/// so for a long state folder path we use a private folder in /tmp, as tmux does.
pub fn socket_path() -> anyhow::Result<PathBuf> {
    socket_path_in(crate::paths::state_dir(), Path::new("/tmp"))
}

/// The socket path for state folder `state`, using `tmp` for the fallback folder.
fn socket_path_in(state: &Path, tmp: &Path) -> anyhow::Result<PathBuf> {
    let p = state.join("ctl.sock");
    if p.as_os_str().len() < 100 {
        return Ok(p);
    }
    std::fs::create_dir_all(state)?;
    let uid = std::fs::metadata(state)?.uid();
    let dir = tmp.join(format!("rust-sync-{uid}"));
    let _ = std::fs::DirBuilder::new().mode(0o700).create(&dir);
    let m = std::fs::symlink_metadata(&dir)?;
    if !m.is_dir() || m.uid() != uid || m.mode() & 0o077 != 0 {
        anyhow::bail!("{} is not a private folder owned by you", dir.display());
    }
    let tag = crate::net::hex(&blake3::hash(state.as_os_str().as_bytes()).as_bytes()[..8]);
    Ok(dir.join(format!("{tag}.sock")))
}

/// Send a request to the daemon. `Ok(None)` means no daemon is running.
pub async fn call(req: &Req) -> anyhow::Result<Option<Resp>> {
    call_at(&socket_path()?, req).await
}

/// Send a request to the daemon listening on `sock`. `Ok(None)` means no daemon answered.
async fn call_at(sock: &Path, req: &Req) -> anyhow::Result<Option<Resp>> {
    let Ok(s) = UnixStream::connect(sock).await else { return Ok(None) };
    let (r, mut w) = s.into_split();
    let mut line = serde_json::to_string(req)?;
    line.push('\n');
    w.write_all(line.as_bytes()).await?;
    let mut reply = String::new();
    BufReader::new(r).read_line(&mut reply).await?;
    if reply.is_empty() {
        // The daemon closed the connection without answering: it is shutting down.
        return Ok(None);
    }
    Ok(Some(serde_json::from_str(&reply)?))
}

/// Apply a change request to the config and return the reply text, or an error text.
/// Requests that only read, `Status` and `ListNodes`, must be handled by the caller.
pub fn apply(cfg: &mut Config, me: &str, req: &Req) -> Result<String, String> {
    match req {
        Req::AddPath { path, is_dir } => {
            cfg.set_root(me, path, *is_dir, false);
            Ok(format!("Now syncing {path}"))
        }
        Req::RemovePath { path } => {
            let Some(is_dir) = cfg.active_root(path).map(|r| r.is_dir) else {
                return Err(format!("{path} is not synced"));
            };
            cfg.set_root(me, path, is_dir, true);
            Ok(format!("Stopped syncing {path}. The files stay on disk."))
        }
        Req::AddNode { id, addr } => {
            cfg.set_node(me, id, addr, false);
            Ok(format!("Added node {}", crate::net::pretty_id(id)))
        }
        Req::RemoveNode { id } => {
            let Some(addr) = cfg.trusted(id).map(|n| n.addr.clone()) else { return Err("No such node".into()) };
            cfg.set_node(me, id, &addr, true);
            Ok(format!("Removed node {}", crate::net::pretty_id(id)))
        }
        Req::ListNodes | Req::Status => unreachable!("read-only request"),
    }
}

#[cfg(test)]
mod tests;
