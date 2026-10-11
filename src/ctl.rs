//! Commands from the CLI to a running daemon, over a Unix socket.
//! One JSON line in, one JSON line out.

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
    let state = crate::paths::state_dir();
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn socket_path_fits_unix_limit_and_is_stable() {
        let p = socket_path().unwrap();
        assert!(p.as_os_str().len() < 104);
        assert_eq!(p, socket_path().unwrap());
        let short = crate::paths::state_dir().join("ctl.sock");
        if short.as_os_str().len() < 100 {
            assert_eq!(p, short);
        } else {
            assert!(p.starts_with("/tmp"));
            assert_eq!(p.extension().unwrap(), "sock");
        }
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
