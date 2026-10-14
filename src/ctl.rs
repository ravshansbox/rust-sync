//! Commands from the CLI to a running daemon, over a Unix socket.
//! One JSON line in, one JSON line out.

use crate::config::Config;
use serde::{Deserialize, Serialize};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, MetadataExt};
use std::path::{Path, PathBuf};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

#[derive(Serialize, Deserialize, Debug)]
pub enum Req {
    AddPath { path: String, is_dir: bool },
    RemovePath { path: String },
    AddNode { id: String, addr: String },
    RemoveNode { id: String },
    ListNodes,
    Status,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Resp {
    pub ok: bool,
    pub text: String,
}

/// `<state dir>/ctl.sock`. Unix socket paths must be short (104 bytes on macOS),
/// so for a long state folder path we use a private folder in /tmp, as tmux does.
pub fn socket_path() -> anyhow::Result<PathBuf> {
    socket_path_in(crate::paths::state_dir())
}

fn socket_path_in(state: &Path) -> anyhow::Result<PathBuf> {
    let p = state.join("ctl.sock");
    if p.as_os_str().len() < 100 {
        return Ok(p);
    }
    std::fs::create_dir_all(state)?;
    let uid = std::fs::metadata(state)?.uid();
    let dir = PathBuf::from(format!("/tmp/rust-sync-{uid}"));
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
mod tests {
    use super::*;

    #[test]
    fn apply_edits_config_and_rejects_unknown_paths_and_nodes() {
        let mut c = Config::default();
        let add = Req::AddPath { path: "~/a".into(), is_dir: true };
        assert_eq!(apply(&mut c, "me", &add).unwrap(), "Now syncing ~/a");
        let rm = Req::RemovePath { path: "~/a".into() };
        assert_eq!(apply(&mut c, "me", &rm).unwrap(), "Stopped syncing ~/a. The files stay on disk.");
        assert!(c.shared.roots["~/a"].removed && c.shared.roots["~/a"].is_dir);
        assert_eq!(apply(&mut c, "me", &rm).unwrap_err(), "~/a is not synced");

        let add = Req::AddNode { id: "abcdefgh12".into(), addr: "h:1".into() };
        assert_eq!(apply(&mut c, "me", &add).unwrap(), "Added node ABCDEFGH-12");
        let rm = Req::RemoveNode { id: "abcdefgh12".into() };
        assert_eq!(apply(&mut c, "me", &rm).unwrap(), "Removed node ABCDEFGH-12");
        assert!(c.trusted("abcdefgh12").is_none());
        assert_eq!(c.shared.nodes["abcdefgh12"].addr, "h:1");
        assert_eq!(apply(&mut c, "me", &rm).unwrap_err(), "No such node");
    }

    #[test]
    fn remove_path_keeps_file_root_as_file() {
        let mut c = Config::default();
        apply(&mut c, "me", &Req::AddPath { path: "~/f".into(), is_dir: false }).unwrap();
        apply(&mut c, "me", &Req::RemovePath { path: "~/f".into() }).unwrap();
        assert!(c.shared.roots["~/f"].removed && !c.shared.roots["~/f"].is_dir);
    }

    #[test]
    fn short_state_folder_keeps_socket_inside() {
        let state = PathBuf::from(format!("/{}", "s".repeat(89)));
        let p = socket_path_in(&state).unwrap();
        assert_eq!(p.as_os_str().len(), 99);
        assert_eq!(p, state.join("ctl.sock"));
        assert!(!state.exists());
    }

    #[test]
    fn socket_path_of_100_bytes_moves_to_tmp() {
        let base = std::env::temp_dir().join(format!("rust-sync-sock100-{}", std::process::id()));
        let pad = 100 - base.as_os_str().len() - "/".len() - "/ctl.sock".len();
        let state = base.join("a".repeat(pad));
        assert_eq!(state.join("ctl.sock").as_os_str().len(), 100);
        let p = socket_path_in(&state).unwrap();
        let uid = std::fs::metadata(&state).unwrap().uid();
        assert_eq!(p.parent().unwrap(), Path::new(&format!("/tmp/rust-sync-{uid}")));
        std::fs::remove_dir_all(&base).unwrap();
    }

    #[test]
    fn long_state_folder_gets_private_socket_in_tmp() {
        let base = std::env::temp_dir().join(format!("rust-sync-sock-{}", std::process::id()));
        let state = base.join("a".repeat(100));
        let p = socket_path_in(&state).unwrap();
        assert!(p.as_os_str().len() < 104);
        assert_eq!(p, socket_path_in(&state).unwrap());
        assert_ne!(p, socket_path_in(&base.join("b".repeat(100))).unwrap());
        let dir = p.parent().unwrap();
        assert_eq!(dir, Path::new(&format!("/tmp/rust-sync-{}", std::fs::metadata(&state).unwrap().uid())));
        assert_eq!(std::fs::metadata(dir).unwrap().mode() & 0o777, 0o700);
        let name = p.file_name().unwrap().to_str().unwrap();
        assert!(name.len() == 21 && name.ends_with(".sock"));
        std::fs::remove_dir_all(&base).unwrap();
    }

    #[tokio::test]
    async fn call_at_returns_reply_or_none_when_daemon_is_absent_or_closes() {
        let sock = std::env::temp_dir().join(format!("rust-sync-call-{}.sock", std::process::id()));
        let _ = std::fs::remove_file(&sock);
        assert!(call_at(&sock, &Req::Status).await.unwrap().is_none());

        let l = tokio::net::UnixListener::bind(&sock).unwrap();
        let server = tokio::spawn(async move {
            let (s, _) = l.accept().await.unwrap();
            let (r, mut w) = s.into_split();
            let mut line = String::new();
            BufReader::new(r).read_line(&mut line).await.unwrap();
            w.write_all(b"{\"ok\":false,\"text\":\"no\"}\n").await.unwrap();
            let (s, _) = l.accept().await.unwrap();
            drop(s);
            line
        });
        let resp = call_at(&sock, &Req::RemoveNode { id: "abcd".into() }).await.unwrap().unwrap();
        assert!(!resp.ok);
        assert_eq!(resp.text, "no");
        assert!(call_at(&sock, &Req::Status).await.unwrap().is_none());
        assert_eq!(server.await.unwrap(), "{\"RemoveNode\":{\"id\":\"abcd\"}}\n");
        std::fs::remove_file(&sock).unwrap();
    }
}
