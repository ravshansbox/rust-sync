use super::*;
use crate::daemon::conn::{connect_any, connection_inner, dial};
use tokio::net::{TcpListener, TcpStream};

#[tokio::test]
async fn connect_any_falls_back_to_an_address_that_listens_and_fails_when_none_does() {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    let s = connect_any(&[format!("localhost:{port}")]).await.unwrap();
    assert_eq!(s.peer_addr().unwrap(), l.local_addr().unwrap());
    drop((s, l));
    let e = connect_any(&["127.0.0.1:1".into()]).await.unwrap_err();
    assert_eq!(e.kind(), std::io::ErrorKind::ConnectionRefused);
}

#[tokio::test]
async fn connect_any_falls_back_to_an_alias_when_the_first_name_does_not_connect() {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    let s = connect_any(&["127.0.0.1:1".into(), format!("localhost:{port}")]).await.unwrap();
    assert_eq!(s.peer_addr().unwrap(), l.local_addr().unwrap());
}

#[tokio::test]
async fn dial_reports_done_when_the_peer_cannot_be_reached() {
    let dir = test_dir("dial");
    let keys = Arc::new(net::load_or_create_keys(&dir).unwrap());
    let addr = TcpListener::bind("127.0.0.1:0").await.unwrap().local_addr().unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    dial(vec![addr.to_string()], "ab".repeat(32), true, keys, tx, 0).await;
    let Some(Event::DialDone { id }) = rx.recv().await else { panic!("expected DialDone") };
    assert_eq!(id, "ab".repeat(32));
    assert!(rx.recv().await.is_none());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn connection_rejects_a_peer_whose_id_is_not_the_one_dialled() {
    let dir = test_dir("conn");
    let keys = Arc::new(net::load_or_create_keys(&dir.join("a")).unwrap());
    let other = net::load_or_create_keys(&dir.join("b")).unwrap();
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let server = async {
        let (s, _) = l.accept().await.unwrap();
        let _ = net::handshake(s, &other, false).await;
    };
    let dialled = "ab".repeat(32);
    let s = TcpStream::connect(addr).await.unwrap();
    let client = connection_inner(s, addr.ip(), Some(dialled.clone()), keys, tx, 0);
    let ((), res, quiet) = tokio::join!(server, client, async { rx.recv().await.is_none() });
    let want = format!("expected node {} but found {}", net::pretty_id(&dialled), net::pretty_id(&other.id()));
    assert_eq!(res.unwrap_err().to_string(), want);
    assert!(quiet);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn connection_does_not_come_up_when_the_listener_does_not_trust_the_dialler() {
    let dir = test_dir("trust");
    let a = Arc::new(net::load_or_create_keys(&dir.join("a")).unwrap());
    let b = Arc::new(net::load_or_create_keys(&dir.join("b")).unwrap());
    for (bind, seen) in [("127.0.0.1:0", "127.0.0.1:4000"), ("[::1]:0", "[::1]:4000")] {
        let l = TcpListener::bind(bind).await.unwrap();
        let addr = l.local_addr().unwrap();
        let (a_tx, a_rx) = mpsc::unbounded_channel();
        let (b_tx, b_rx) = mpsc::unbounded_channel();
        let answer = async |mut rx: mpsc::UnboundedReceiver<Event>, trust: bool| {
            let Some(Event::Auth { id, addr, reply }) = rx.recv().await else { panic!() };
            reply.send(trust).unwrap();
            (id, addr, rx.recv().await.is_none())
        };
        let dialler = async {
            let s = TcpStream::connect(addr).await.unwrap();
            connection_inner(s, addr.ip(), Some(b.id()), a.clone(), a_tx, 4000).await
        };
        let listener = async {
            let (s, peer) = l.accept().await.unwrap();
            connection_inner(s, peer.ip(), None, b.clone(), b_tx, 5000).await
        };
        let (a_res, b_res, a_seen, b_seen) =
            tokio::join!(dialler, listener, answer(a_rx, true), answer(b_rx, false));
        a_res.unwrap();
        b_res.unwrap();
        assert_eq!(a_seen, (b.id(), None, true));
        assert_eq!(b_seen, (a.id(), Some(seen.into()), true));
    }
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn connection_does_not_come_up_when_the_dialler_does_not_trust_the_listener() {
    let dir = test_dir("distrust");
    let a = Arc::new(net::load_or_create_keys(&dir.join("a")).unwrap());
    let b = Arc::new(net::load_or_create_keys(&dir.join("b")).unwrap());
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let (a_tx, a_rx) = mpsc::unbounded_channel();
    let (b_tx, b_rx) = mpsc::unbounded_channel();
    let answer = async |mut rx: mpsc::UnboundedReceiver<Event>, trust: bool| {
        let Some(Event::Auth { reply, .. }) = rx.recv().await else { panic!() };
        reply.send(trust).unwrap();
        rx.recv().await.is_none()
    };
    let dialler = async {
        let s = TcpStream::connect(addr).await.unwrap();
        connection_inner(s, addr.ip(), Some(b.id()), a.clone(), a_tx, 4000).await
    };
    let listener = async {
        let (s, peer) = l.accept().await.unwrap();
        connection_inner(s, peer.ip(), None, b.clone(), b_tx, 5000).await
    };
    let (a_res, b_res, a_quiet, b_quiet) = tokio::join!(dialler, listener, answer(a_rx, false), answer(b_rx, true));
    a_res.unwrap();
    b_res.unwrap();
    assert_eq!((a_quiet, b_quiet), (true, true));
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn trusted_connection_comes_up_carries_control_and_data_messages_and_goes_down() {
    let dir = test_dir("up");
    let a = Arc::new(net::load_or_create_keys(&dir.join("a")).unwrap());
    let b = Arc::new(net::load_or_create_keys(&dir.join("b")).unwrap());
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let (a_tx, mut a_rx) = mpsc::unbounded_channel();
    let (b_tx, mut b_rx) = mpsc::unbounded_channel();
    let dialler = async {
        let s = TcpStream::connect(addr).await.unwrap();
        connection_inner(s, addr.ip(), Some(b.id()), a.clone(), a_tx, 4000).await
    };
    let listener = async {
        let (s, peer) = l.accept().await.unwrap();
        connection_inner(s, peer.ip(), None, b.clone(), b_tx, 5000).await
    };
    let (got_tx, got_rx) = oneshot::channel();
    let a_side = async {
        let Some(Event::Auth { reply, .. }) = a_rx.recv().await else { panic!() };
        reply.send(true).unwrap();
        let Some(Event::Up { id, conn, outbound: true, ctl, data }) = a_rx.recv().await else { panic!() };
        assert_eq!(id, b.id());
        ctl.send(Msg::Unavailable { path: "x".into(), hash: [0; 32] }).unwrap();
        data.send(Msg::Unavailable { path: "y".into(), hash: [0; 32] }).await.unwrap();
        got_rx.await.unwrap();
        drop(ctl);
        let Some(Event::Down { id, conn: down }) = a_rx.recv().await else { panic!() };
        assert_eq!((id, down), (b.id(), conn));
        assert!(a_rx.recv().await.is_none());
    };
    let b_side = async {
        let Some(Event::Auth { reply, .. }) = b_rx.recv().await else { panic!() };
        reply.send(true).unwrap();
        let Some(Event::Up { id, conn, outbound: false, ctl: _ctl, .. }) = b_rx.recv().await else { panic!() };
        assert_eq!(id, a.id());
        let Some(Event::Peer { conn: from, msg: Msg::Unavailable { path, .. }, .. }) = b_rx.recv().await else { panic!() };
        assert_eq!((from, path.as_str()), (conn, "x"));
        let Ok(Some(Event::Peer { msg: Msg::Unavailable { path, .. }, .. })) =
            timeout(Duration::from_secs(5), b_rx.recv()).await
        else {
            panic!("expected the data message")
        };
        assert_eq!(path, "y");
        got_tx.send(()).unwrap();
        let Some(Event::Down { id, conn: down }) = b_rx.recv().await else { panic!() };
        assert_eq!((id, down), (a.id(), conn));
        assert!(b_rx.recv().await.is_none());
    };
    let (a_res, b_res, (), ()) = tokio::join!(dialler, listener, a_side, b_side);
    a_res.unwrap();
    assert!(b_res.is_err());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn probe_gets_a_hello_back_but_does_not_bring_the_connection_up() {
    let dir = test_dir("probe");
    let a = Arc::new(net::load_or_create_keys(&dir.join("a")).unwrap());
    let b = Arc::new(net::load_or_create_keys(&dir.join("b")).unwrap());
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let prober = async {
        let s = TcpStream::connect(addr).await.unwrap();
        let (mut r, mut w, _) = net::handshake(s, &a, true).await.unwrap();
        w.send(&Msg::Hello { port: 4000, probe: true, trusts_you: true }).await.unwrap();
        r.recv().await.unwrap()
    };
    let listener = async {
        let (s, peer) = l.accept().await.unwrap();
        connection_inner(s, peer.ip(), None, b.clone(), tx, 5000).await
    };
    let answer = async {
        let Some(Event::Auth { id, addr, reply }) = rx.recv().await else { panic!() };
        reply.send(true).unwrap();
        (id, addr, rx.recv().await.is_none())
    };
    let (reply, res, seen) = tokio::join!(prober, listener, answer);
    res.unwrap();
    let Msg::Hello { port: 5000, probe: false, trusts_you: true } = reply else { panic!() };
    assert_eq!(seen, (a.id(), Some("127.0.0.1:4000".into()), true));
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn connection_ends_on_the_next_message_once_the_daemon_has_stopped() {
    let dir = test_dir("stopped");
    let a = Arc::new(net::load_or_create_keys(&dir.join("a")).unwrap());
    let b = Arc::new(net::load_or_create_keys(&dir.join("b")).unwrap());
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let (stopped_tx, stopped_rx) = oneshot::channel();
    let peer = async {
        let s = TcpStream::connect(addr).await.unwrap();
        let (mut r, mut w, _) = net::handshake(s, &a, true).await.unwrap();
        w.send(&Msg::Hello { port: 4000, probe: false, trusts_you: true }).await.unwrap();
        r.recv().await.unwrap();
        stopped_rx.await.unwrap();
        w.send(&Msg::Unavailable { path: "x".into(), hash: [0; 32] }).await.unwrap();
        (r, w)
    };
    let listener = async {
        let (s, peer) = l.accept().await.unwrap();
        timeout(Duration::from_secs(5), connection_inner(s, peer.ip(), None, b.clone(), tx, 5000)).await
    };
    let daemon = async {
        let Some(Event::Auth { reply, .. }) = rx.recv().await else { panic!() };
        reply.send(true).unwrap();
        let Some(Event::Up { ctl, .. }) = rx.recv().await else { panic!() };
        drop(rx);
        stopped_tx.send(()).unwrap();
        ctl
    };
    let (_peer, res, _ctl) = tokio::join!(peer, listener, daemon);
    res.expect("connection should end at once").unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn accept_loop_serves_each_incoming_connection_with_its_ipv4_address() {
    let dir = test_dir("accept");
    let a = Arc::new(net::load_or_create_keys(&dir.join("a")).unwrap());
    let b = Arc::new(net::load_or_create_keys(&dir.join("b")).unwrap());
    let l = TcpListener::bind("[::]:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let task = tokio::spawn(accept_loop(l, b, tx, 5000));
    for their_port in [4000, 4001] {
        let s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let (mut r, mut w, _) = net::handshake(s, &a, true).await.unwrap();
        w.send(&Msg::Hello { port: their_port, probe: true, trusts_you: true }).await.unwrap();
        let Some(Event::Auth { id, addr, reply }) = rx.recv().await else { panic!() };
        assert_eq!((id, addr), (a.id(), Some(format!("127.0.0.1:{their_port}"))));
        reply.send(false).unwrap();
        let Msg::Hello { port: 5000, probe: false, trusts_you: false } = r.recv().await.unwrap() else { panic!() };
    }
    task.abort();
    std::fs::remove_dir_all(&dir).unwrap();
}
