//! Running the command-line commands.

use crate::cli::{Cmd, NodeAdd, NodeCmd, PathCmd, ServiceCmd};
use crate::config::{self, Config};
use crate::ctl::{self, Req};
use crate::net::{self, Msg};
use crate::{daemon, paths, service};
use anyhow::{Context, bail};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::timeout;

pub async fn run(cmd: Cmd) -> anyhow::Result<()> {
    match cmd {
        Cmd::Daemon { port, log_file } => daemon::run(port, log_file).await,
        Cmd::Id => {
            outln!("{}", net::pretty_id(&my_keys()?.id()));
            Ok(())
        }
        Cmd::Path { cmd: PathCmd::Add { path } } => path_add(path).await,
        Cmd::Path { cmd: PathCmd::List } => {
            // The daemon saves every change to the config file, so this is current either way.
            out!("{}", daemon::paths_text(&Config::load(&paths::config_file())?));
            Ok(())
        }
        Cmd::Path { cmd: PathCmd::Remove { path } } => {
            send(Req::RemovePath { path: removal_path(path)? }).await
        }
        Cmd::Node { cmd: NodeCmd::Add(a) } => node_add(a).await,
        Cmd::Node { cmd: NodeCmd::List } => send(Req::ListNodes).await,
        Cmd::Node { cmd: NodeCmd::Remove { id } } => {
            send(Req::RemoveNode { id: net::parse_id(&id).context("not a valid node ID")? }).await
        }
        Cmd::Service { cmd: ServiceCmd::Install { system } } => service::install(system).await,
        Cmd::Service { cmd: ServiceCmd::Uninstall } => service::uninstall(),
        Cmd::Service { cmd: ServiceCmd::Start } => service::start().await,
        Cmd::Service { cmd: ServiceCmd::Stop } => service::stop().await,
        Cmd::Service { cmd: ServiceCmd::Restart } => service::restart().await,
        Cmd::Status => {
            send(Req::Status).await?;
            match service::starts_at() {
                Some(when) => outln!("Starts at {when}: yes"),
                None => outln!("Starts at login: no (`rust-sync service install`)"),
            }
            Ok(())
        }
    }
}

async fn path_add(path: PathBuf) -> anyhow::Result<()> {
    let abs = std::fs::canonicalize(&path).with_context(|| format!("cannot find {}", path.display()))?;
    let is_dir = abs.is_dir();
    if !is_dir && !abs.is_file() {
        bail!("{} is not a regular file or folder", abs.display());
    }
    let portable = paths::to_portable(&abs)
        .filter(|p| paths::to_local(p).is_some())
        .with_context(|| format!("cannot sync {}", abs.display()))?;
    send(Req::AddPath { path: portable, is_dir }).await
}

fn removal_path(path: PathBuf) -> anyhow::Result<String> {
    // The path may be gone already, so do not require it to exist.
    let abs = std::fs::canonicalize(&path).unwrap_or_else(|_| paths::resolve_parent(path));
    paths::to_portable(&abs).context("path is not valid UTF-8")
}

fn my_keys() -> anyhow::Result<net::Keys> {
    net::load_or_create_keys(paths::state_dir())
}

/// Send a change to the running daemon. If none is running, edit the config file.
async fn send(req: Req) -> anyhow::Result<()> {
    if let Some(resp) = ctl::call(&req).await? {
        if !resp.ok {
            bail!("{}", resp.text);
        }
        outln!("{}", resp.text.trim_end());
        return Ok(());
    }
    let me = my_keys()?.id();
    let path = paths::config_file();
    let mut cfg = Config::load(&path)?;
    match req {
        Req::Status => {
            out!("{}", daemon::status_text(&cfg, &me, |_| None, &BTreeMap::default()));
            return Ok(());
        }
        Req::ListNodes => {
            out!("{}", daemon::nodes_text(&cfg, |_| None, &BTreeMap::default()));
            outln!("\n(The daemon is not running, so connection state is unknown.)");
            return Ok(());
        }
        _ => {}
    }
    let text = ctl::apply(&mut cfg, &me, &req).map_err(anyhow::Error::msg)?;
    cfg.save(&path)?;
    outln!("{text}\n(The daemon is not running. This takes effect when it starts.)");
    Ok(())
}

/// Connect to the node, show its ID, and trust it once the user confirms.
async fn node_add(a: NodeAdd) -> anyhow::Result<()> {
    let keys = my_keys()?;
    let cfg = Config::load(&paths::config_file())?;
    let addr = config::with_port(&a.addr);
    let expected = a.id.as_deref().map(|s| net::parse_id(s).context("not a valid node ID")).transpose()?;

    outln!("Connecting to {addr}...");
    let stream = timeout(Duration::from_secs(10), tokio::net::TcpStream::connect(&addr))
        .await
        .context("timed out")?
        .with_context(|| format!("cannot connect to {addr}. Is `rust-sync daemon` running there?"))?;
    let (mut r, mut w, id) = net::handshake(stream, &keys, true).await?;
    if id == keys.id() {
        bail!("that is this node");
    }
    w.send(&Msg::Hello { port: cfg.port, probe: true, trusts_you: cfg.trusted(&id).is_some() }).await?;
    let trusts_us = matches!(
        timeout(Duration::from_secs(10), r.recv()).await,
        Ok(Ok(Msg::Hello { trusts_you: true, .. }))
    );

    outln!("Node ID: {}", net::pretty_id(&id));
    match expected {
        Some(e) if e != id => bail!("that is not the ID you gave ({}). Node not added.", net::pretty_id(&e)),
        Some(_) => {}
        None if a.yes => {}
        None => {
            out!("Check this matches `rust-sync id` on that machine. Trust it? [y/N] ");
            let mut answer = String::new();
            std::io::stdin().read_line(&mut answer)?;
            if !answer.trim().eq_ignore_ascii_case("y") {
                outln!("Node not added.");
                return Ok(());
            }
        }
    }
    send(Req::AddNode { id, addr }).await?;
    if !trusts_us {
        outln!(
            "\nThat node does not trust this one yet. On that machine, run\n  \
             rust-sync node add <address of this machine>\nand check it shows this ID:\n  {}",
            net::pretty_id(&keys.id())
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn path_add_rejects_missing_special_and_state_paths() {
        let add = |p: &str| run(Cmd::Path { cmd: PathCmd::Add { path: PathBuf::from(p) } });
        assert_eq!(add("/no/such/path").await.unwrap_err().to_string(), "cannot find /no/such/path");
        assert_eq!(add("/dev/null").await.unwrap_err().to_string(), "/dev/null is not a regular file or folder");
        std::fs::create_dir_all(paths::state_dir()).unwrap();
        let state = paths::state_dir().to_str().unwrap();
        assert_eq!(add(state).await.unwrap_err().to_string(), format!("cannot sync {state}"));
    }

    #[tokio::test]
    async fn path_add_saves_canonical_path_and_whether_it_is_a_folder() {
        let _key = STATE_KEY.lock().await;
        let tmp = std::env::temp_dir().join(format!("rust-sync-path-add-{}", std::process::id()));
        std::fs::create_dir_all(tmp.join("d")).unwrap();
        std::fs::write(tmp.join("f"), "x").unwrap();
        let add = |p: PathBuf| run(Cmd::Path { cmd: PathCmd::Add { path: p } });
        add(tmp.join("d/../f")).await.unwrap();
        add(tmp.join("d")).await.unwrap();
        let cfg = Config::load(&paths::config_file()).unwrap();
        let real = tmp.canonicalize().unwrap();
        assert!(!cfg.active_root(&paths::to_portable(&real.join("f")).unwrap()).unwrap().is_dir);
        assert!(cfg.active_root(&paths::to_portable(&real.join("d")).unwrap()).unwrap().is_dir);
        std::fs::remove_file(paths::config_file()).unwrap();
        std::fs::remove_file(paths::state_dir().join("key")).unwrap();
        std::fs::remove_dir_all(&tmp).unwrap();
    }

    #[tokio::test]
    async fn path_remove_accepts_paths_that_no_longer_exist() {
        let _key = STATE_KEY.lock().await;
        let remove = |p: &str| run(Cmd::Path { cmd: PathCmd::Remove { path: PathBuf::from(p) } });
        assert_eq!(remove("/no/such/path").await.unwrap_err().to_string(), "/no/such/path is not synced");
    }

    #[tokio::test]
    async fn node_remove_rejects_invalid_id() {
        let remove = |id: &str| run(Cmd::Node { cmd: NodeCmd::Remove { id: id.into() } });
        assert_eq!(remove("not-an-id").await.unwrap_err().to_string(), "not a valid node ID");
        assert_eq!(remove(&"a".repeat(63)).await.unwrap_err().to_string(), "not a valid node ID");
    }

    #[tokio::test]
    async fn node_add_rejects_invalid_id_before_connecting() {
        let a = NodeAdd { addr: "127.0.0.1:1".into(), id: Some("not-an-id".into()), yes: true };
        assert_eq!(node_add(a).await.unwrap_err().to_string(), "not a valid node ID");
    }

    static STATE_KEY: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[tokio::test]
    async fn node_add_rejects_this_node() {
        let _key = STATE_KEY.lock().await;
        let keys = my_keys().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = async {
            let (s, _) = listener.accept().await.unwrap();
            let (mut r, _w, _) = net::handshake(s, &keys, false).await.unwrap();
            let _ = r.recv().await;
        };
        let a = NodeAdd { addr, id: None, yes: true };
        let ((), result) = tokio::join!(server, node_add(a));
        assert_eq!(result.unwrap_err().to_string(), "that is this node");
        std::fs::remove_file(paths::state_dir().join("key")).unwrap();
    }

    #[tokio::test]
    async fn node_add_rejects_node_with_a_different_id_than_given() {
        let dir = std::env::temp_dir().join(format!("rust-sync-node-add-{}", std::process::id()));
        let keys = net::load_or_create_keys(&dir).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = async {
            let (s, _) = listener.accept().await.unwrap();
            let (mut r, _w, _) = net::handshake(s, &keys, false).await.unwrap();
            let _ = r.recv().await;
        };
        let expected = "ab".repeat(32);
        let a = NodeAdd { addr, id: Some(expected.clone()), yes: false };
        let ((), result) = tokio::join!(server, node_add(a));
        let want = format!("that is not the ID you gave ({}). Node not added.", net::pretty_id(&expected));
        assert_eq!(result.unwrap_err().to_string(), want);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn node_add_with_the_given_id_probes_the_node_and_trusts_it_at_its_address() {
        let _key = STATE_KEY.lock().await;
        let dir = std::env::temp_dir().join(format!("rust-sync-node-add-ok-{}", std::process::id()));
        let keys = net::load_or_create_keys(&dir).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = async {
            let (s, _) = listener.accept().await.unwrap();
            let (mut r, mut w, _) = net::handshake(s, &keys, false).await.unwrap();
            let hello = r.recv().await.unwrap();
            w.send(&Msg::Hello { port: 1, probe: false, trusts_you: false }).await.unwrap();
            hello
        };
        let a = NodeAdd { addr: addr.clone(), id: Some(net::pretty_id(&keys.id())), yes: false };
        let (hello, result) = tokio::join!(server, node_add(a));
        result.unwrap();
        assert!(matches!(hello, Msg::Hello { probe: true, trusts_you: false, .. }));
        let cfg = Config::load(&paths::config_file()).unwrap();
        assert_eq!(cfg.trusted(&keys.id()).unwrap().addr, addr);
        std::fs::remove_file(paths::config_file()).unwrap();
        std::fs::remove_file(paths::state_dir().join("key")).unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn removal_path_of_a_deleted_path_resolves_symlinks_and_dot_dot() {
        let tmp = std::env::temp_dir().join(format!("rust-sync-rm-path-{}", std::process::id()));
        std::fs::create_dir_all(tmp.join("real")).unwrap();
        let _ = std::fs::remove_file(tmp.join("link"));
        std::os::unix::fs::symlink(tmp.join("real"), tmp.join("link")).unwrap();
        let want = paths::to_portable(&tmp.canonicalize().unwrap().join("real/gone")).unwrap();
        assert_eq!(removal_path(tmp.join("link/gone")).unwrap(), want);
        assert_eq!(removal_path(tmp.join("link/../real/gone")).unwrap(), want);
        std::fs::remove_dir_all(&tmp).unwrap();
    }

    #[tokio::test]
    async fn send_without_daemon_saves_change_to_config_file() {
        let _key = STATE_KEY.lock().await;
        send(Req::AddPath { path: "~/notes".into(), is_dir: true }).await.unwrap();
        let cfg = Config::load(&paths::config_file()).unwrap();
        assert!(cfg.active_root("~/notes").unwrap().is_dir);
        let err = send(Req::RemovePath { path: "~/missing".into() }).await.unwrap_err();
        assert_eq!(err.to_string(), "~/missing is not synced");
        assert!(Config::load(&paths::config_file()).unwrap().active_root("~/notes").is_some());
        std::fs::remove_file(paths::config_file()).unwrap();
        std::fs::remove_file(paths::state_dir().join("key")).unwrap();
    }

    #[tokio::test]
    async fn send_to_a_running_daemon_reports_its_reply_and_leaves_the_config_file_alone() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
        let _key = STATE_KEY.lock().await;
        std::fs::create_dir_all(paths::state_dir()).unwrap();
        let sock = ctl::socket_path().unwrap();
        let _ = std::fs::remove_file(&sock);
        let listener = tokio::net::UnixListener::bind(&sock).unwrap();
        let daemon = tokio::spawn(async move {
            for (ok, text) in [(false, "refused"), (true, "done")] {
                let (s, _) = listener.accept().await.unwrap();
                let (r, mut w) = s.into_split();
                let mut line = String::new();
                tokio::io::BufReader::new(r).read_line(&mut line).await.unwrap();
                assert!(matches!(serde_json::from_str(&line).unwrap(), Req::AddPath { path, is_dir: true } if path == "~/notes"));
                let reply = serde_json::to_string(&ctl::Resp { ok, text: text.into() }).unwrap();
                w.write_all(format!("{reply}\n").as_bytes()).await.unwrap();
            }
        });
        let add = || send(Req::AddPath { path: "~/notes".into(), is_dir: true });
        assert_eq!(add().await.unwrap_err().to_string(), "refused");
        add().await.unwrap();
        daemon.await.unwrap();
        assert!(!paths::config_file().exists());
        std::fs::remove_file(&sock).unwrap();
    }
}
