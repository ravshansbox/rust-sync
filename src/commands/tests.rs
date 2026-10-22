use super::*;
use crate::cli::{AliasCmd, NodeAdd};
use crate::config;
use crate::net::Msg;
use crate::paths::test_dir;
use std::path::PathBuf;
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
    let tmp = test_dir("path-add");
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
async fn node_remove_rejects_what_is_neither_an_id_nor_a_name_of_a_node() {
    let _key = STATE_KEY.lock().await;
    let remove = |id: &str| run(Cmd::Node { cmd: NodeCmd::Remove { id: id.into() } });
    let want = |s: &str| format!("{s} is not a node ID or the name of a trusted node");
    assert_eq!(remove("not-an-id").await.unwrap_err().to_string(), want("not-an-id"));
    assert_eq!(remove(&"a".repeat(63)).await.unwrap_err().to_string(), want(&"a".repeat(63)));
}

#[tokio::test]
async fn node_commands_find_the_node_by_its_name_or_alias() {
    let _key = STATE_KEY.lock().await;
    let (a, b) = ("ab".repeat(32), "cd".repeat(32));
    send(Req::AddNode { id: a.clone(), addr: config::with_port("macbook") }).await.unwrap();
    send(Req::AddAlias { id: a.clone(), addr: "macbook.local:1".into() }).await.unwrap();
    send(Req::AddNode { id: b.clone(), addr: "shared:1".into() }).await.unwrap();
    send(Req::AddNode { id: "ef".repeat(32), addr: "shared:1".into() }).await.unwrap();
    let alias_rm = |id: &str, addr: &str| {
        run(Cmd::Node { cmd: NodeCmd::Alias { cmd: AliasCmd::Remove { id: id.into(), addr: addr.into() } } })
    };
    alias_rm("macbook.local:1", "macbook.local:1").await.unwrap();
    let err = alias_rm("shared:1", "x").await.unwrap_err();
    assert_eq!(err.to_string(), "shared:1 is the name of more than one node. Use its ID.");
    run(Cmd::Node { cmd: NodeCmd::Remove { id: "macbook".into() } }).await.unwrap();
    let cfg = Config::load(&paths::config_file()).unwrap();
    assert!(cfg.trusted(&a).is_none());
    assert!(cfg.trusted(&b).is_some());
    std::fs::remove_file(paths::config_file()).unwrap();
    std::fs::remove_file(paths::state_dir().join("key")).unwrap();
}

#[tokio::test]
async fn node_add_rejects_invalid_id_before_connecting() {
    let a = NodeAdd { addr: "127.0.0.1:1".into(), id: Some("not-an-id".into()), yes: true };
    assert_eq!(node_add(a).await.unwrap_err().to_string(), "not a valid node ID");
}

static STATE_KEY: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn node_add_to_silent_node(keys: &net::Keys, id: Option<String>, yes: bool) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let server = async {
        let (s, _) = listener.accept().await.unwrap();
        let (mut r, _w, _) = net::handshake(s, keys, false).await.unwrap();
        let _ = r.recv().await;
    };
    let ((), result) = tokio::join!(server, node_add(NodeAdd { addr, id, yes }));
    result
}

#[tokio::test]
async fn node_add_rejects_this_node() {
    let _key = STATE_KEY.lock().await;
    let result = node_add_to_silent_node(&my_keys().unwrap(), None, true).await;
    assert_eq!(result.unwrap_err().to_string(), "that is this node");
    std::fs::remove_file(paths::state_dir().join("key")).unwrap();
}

#[tokio::test]
async fn node_add_rejects_node_with_a_different_id_than_given() {
    let dir = test_dir("node-add");
    let keys = net::load_or_create_keys(&dir).unwrap();
    let expected = "ab".repeat(32);
    let result = node_add_to_silent_node(&keys, Some(expected.clone()), false).await;
    let want = format!("that is not the ID you gave ({}). Node not added.", net::pretty_id(&expected));
    assert_eq!(result.unwrap_err().to_string(), want);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn node_add_with_the_given_id_probes_the_node_and_trusts_it_at_its_address() {
    let _key = STATE_KEY.lock().await;
    let dir = test_dir("node-add-ok");
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
    let tmp = test_dir("rm-path");
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

async fn alias_add_to(keys: &net::Keys, id: &str) -> (String, anyhow::Result<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let server = async {
        let (s, _) = listener.accept().await.unwrap();
        let (mut r, mut w, _) = net::handshake(s, keys, false).await.unwrap();
        let _ = r.recv().await;
        let _ = w.send(&Msg::Hello { port: 1, probe: false, trusts_you: true }).await;
    };
    let alias = AliasCmd::Add { id: id.into(), addr: addr.clone() };
    let ((), result) = tokio::join!(server, run(Cmd::Node { cmd: NodeCmd::Alias { cmd: alias } }));
    (addr, result)
}

#[tokio::test]
async fn node_alias_add_checks_the_id_at_the_alias_before_saving_it() {
    let _key = STATE_KEY.lock().await;
    let dir = test_dir("alias-add");
    let keys = net::load_or_create_keys(&dir).unwrap();
    let other = "ab".repeat(32);
    send(Req::AddNode { id: other.clone(), addr: "h:1".into() }).await.unwrap();
    let (_, result) = alias_add_to(&keys, &other).await;
    let want = format!("that is not the ID you gave ({}). Alias not added.", net::pretty_id(&other));
    assert_eq!(result.unwrap_err().to_string(), want);
    send(Req::AddNode { id: keys.id(), addr: "h:2".into() }).await.unwrap();
    let (addr, result) = alias_add_to(&keys, "h:2").await;
    result.unwrap();
    let cfg = Config::load(&paths::config_file()).unwrap();
    assert_eq!(cfg.trusted(&other).unwrap().addrs().collect::<Vec<_>>(), ["h:1"]);
    assert_eq!(cfg.trusted(&keys.id()).unwrap().addrs().collect::<Vec<_>>(), ["h:2", addr.as_str()]);
    std::fs::remove_file(paths::config_file()).unwrap();
    std::fs::remove_file(paths::state_dir().join("key")).unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn node_alias_add_and_remove_add_the_default_port() {
    let _key = STATE_KEY.lock().await;
    let id = "ab".repeat(32);
    send(Req::AddNode { id: id.clone(), addr: "h:1".into() }).await.unwrap();
    send(Req::AddAlias { id: id.clone(), addr: "l:1".into() }).await.unwrap();
    let rm = AliasCmd::Remove { id: id.clone(), addr: "l:1".into() };
    run(Cmd::Node { cmd: NodeCmd::Alias { cmd: rm } }).await.unwrap();
    let rm = AliasCmd::Remove { id: "not-an-id".into(), addr: "l".into() };
    let err = run(Cmd::Node { cmd: NodeCmd::Alias { cmd: rm } }).await.unwrap_err();
    assert_eq!(err.to_string(), "not-an-id is not a node ID or the name of a trusted node");
    let rm = AliasCmd::Remove { id: id.clone(), addr: "l".into() };
    let err = run(Cmd::Node { cmd: NodeCmd::Alias { cmd: rm } }).await.unwrap_err();
    assert_eq!(err.to_string(), format!("{} is not an alias of node {}", config::with_port("l"), net::pretty_id(&id)));
    std::fs::remove_file(paths::config_file()).unwrap();
    std::fs::remove_file(paths::state_dir().join("key")).unwrap();
}
