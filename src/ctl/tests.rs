use super::*;
use crate::paths::test_dir;

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
    let p = socket_path_in(&state, Path::new("/tmp")).unwrap();
    assert_eq!(p.as_os_str().len(), 99);
    assert_eq!(p, state.join("ctl.sock"));
    assert!(!state.exists());
}

#[test]
fn socket_path_of_100_bytes_moves_to_tmp() {
    let base = test_dir("sock100");
    let pad = 100 - base.as_os_str().len() - "/".len() - "/ctl.sock".len();
    let state = base.join("a".repeat(pad));
    assert_eq!(state.join("ctl.sock").as_os_str().len(), 100);
    let p = socket_path_in(&state, Path::new("/tmp")).unwrap();
    let uid = std::fs::metadata(&state).unwrap().uid();
    assert_eq!(p.parent().unwrap(), Path::new(&format!("/tmp/rust-sync-{uid}")));
    std::fs::remove_dir_all(&base).unwrap();
}

#[test]
fn long_state_folder_gets_private_socket_in_tmp() {
    let base = test_dir("sock");
    let state = base.join("a".repeat(100));
    let p = socket_path_in(&state, Path::new("/tmp")).unwrap();
    assert!(p.as_os_str().len() < 104);
    assert_eq!(p, socket_path_in(&state, Path::new("/tmp")).unwrap());
    assert_ne!(p, socket_path_in(&base.join("b".repeat(100)), Path::new("/tmp")).unwrap());
    let dir = p.parent().unwrap();
    assert_eq!(dir, Path::new(&format!("/tmp/rust-sync-{}", std::fs::metadata(&state).unwrap().uid())));
    assert_eq!(std::fs::metadata(dir).unwrap().mode() & 0o777, 0o700);
    let name = p.file_name().unwrap().to_str().unwrap();
    assert!(name.len() == 21 && name.ends_with(".sock"));
    std::fs::remove_dir_all(&base).unwrap();
}

#[test]
fn tmp_socket_folder_that_is_not_private_is_rejected() {
    use std::fs::Permissions;
    use std::os::unix::fs::PermissionsExt;
    let base = test_dir("sock-shared");
    let state = base.join("a".repeat(100));
    std::fs::create_dir_all(&state).unwrap();
    let dir = base.join(format!("rust-sync-{}", std::fs::metadata(&state).unwrap().uid()));
    std::fs::create_dir(&dir).unwrap();
    std::fs::set_permissions(&dir, Permissions::from_mode(0o755)).unwrap();
    let want = format!("{} is not a private folder owned by you", dir.display());
    assert_eq!(socket_path_in(&state, &base).unwrap_err().to_string(), want);
    std::fs::remove_dir(&dir).unwrap();
    std::fs::create_dir(base.join("real")).unwrap();
    std::fs::set_permissions(base.join("real"), Permissions::from_mode(0o700)).unwrap();
    std::os::unix::fs::symlink(base.join("real"), &dir).unwrap();
    assert_eq!(socket_path_in(&state, &base).unwrap_err().to_string(), want);
    std::fs::remove_dir_all(&base).unwrap();
}

#[tokio::test]
async fn call_at_returns_reply_or_none_when_daemon_is_absent_or_closes() {
    let sock = test_dir("call").with_extension("sock");
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

#[test]
fn apply_adds_and_removes_aliases_of_trusted_nodes() {
    let mut c = Config::default();
    let add = |addr: &str| Req::AddAlias { id: "abcdefgh12".into(), addr: addr.into() };
    let rm = |addr: &str| Req::RemoveAlias { id: "abcdefgh12".into(), addr: addr.into() };
    assert_eq!(apply(&mut c, "me", &add("l:1")).unwrap_err(), "No such node");
    apply(&mut c, "me", &Req::AddNode { id: "abcdefgh12".into(), addr: "h:1".into() }).unwrap();
    assert_eq!(apply(&mut c, "me", &add("l:1")).unwrap(), "Added alias l:1 to node ABCDEFGH-12");
    assert_eq!(apply(&mut c, "me", &add("l:1")).unwrap_err(), "l:1 is already an address of node ABCDEFGH-12");
    assert_eq!(apply(&mut c, "me", &add("h:1")).unwrap_err(), "h:1 is already an address of node ABCDEFGH-12");
    assert_eq!(c.trusted("abcdefgh12").unwrap().addrs().collect::<Vec<_>>(), ["h:1", "l:1"]);
    assert_eq!(apply(&mut c, "me", &rm("h:1")).unwrap_err(), "h:1 is not an alias of node ABCDEFGH-12");
    assert_eq!(apply(&mut c, "me", &rm("l:1")).unwrap(), "Removed alias l:1 from node ABCDEFGH-12");
    assert_eq!(c.trusted("abcdefgh12").unwrap().addrs().collect::<Vec<_>>(), ["h:1"]);
    apply(&mut c, "me", &Req::RemoveNode { id: "abcdefgh12".into() }).unwrap();
    assert_eq!(apply(&mut c, "me", &rm("l:1")).unwrap_err(), "No such node");
}
