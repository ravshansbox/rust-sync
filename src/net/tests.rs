use super::keys::generate_keys;
use super::*;
use tokio::net::TcpListener;

async fn connect(a: &Keys, b: &Keys) -> ((Reader, Writer, String), (Reader, Writer, String)) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = async {
        let (s, _) = listener.accept().await.unwrap();
        handshake(s, b, false).await.unwrap()
    };
    let client = async { handshake(TcpStream::connect(addr).await.unwrap(), a, true).await.unwrap() };
    tokio::join!(server, client)
}

#[tokio::test]
async fn frames_round_trip_and_truncated_frame_fails() {
    let mut out = Vec::new();
    write_frame(&mut out, b"hi").await.unwrap();
    write_frame(&mut out, &vec![7; MAX_FRAME]).await.unwrap();
    assert_eq!(&out[..4], [0, 2, b'h', b'i']);
    let mut r = &out[..];
    assert_eq!(read_frame(&mut r).await.unwrap(), b"hi");
    assert_eq!(read_frame(&mut r).await.unwrap(), vec![7; MAX_FRAME]);
    assert!(read_frame(&mut r).await.is_err());
    let mut short = &out[..3];
    assert!(read_frame(&mut short).await.is_err());
}

#[tokio::test]
async fn handshake_exchanges_ids_and_messages() {
    let (a, b) = (generate_keys().unwrap(), generate_keys().unwrap());
    let ((mut br, _bw, b_sees), (_ar, mut aw, a_sees)) = connect(&a, &b).await;
    assert_eq!(b_sees, a.id());
    assert_eq!(a_sees, b.id());
    aw.send(&Msg::Unavailable { path: "~/x".into(), hash: [3; 32] }).await.unwrap();
    assert!(matches!(br.recv().await.unwrap(), Msg::Unavailable { path, hash } if path == "~/x" && hash == [3; 32]));
}

#[tokio::test]
async fn handshake_turns_off_nagle_on_both_sides() {
    let ((br, _bw, _), (ar, _aw, _)) = connect(&generate_keys().unwrap(), &generate_keys().unwrap()).await;
    assert!(br.r.as_ref().nodelay().unwrap());
    assert!(ar.r.as_ref().nodelay().unwrap());
}

#[tokio::test]
async fn handshake_fails_when_peer_sends_garbage_or_closes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = async {
        let (mut s, _) = listener.accept().await.unwrap();
        read_frame(&mut s).await.unwrap();
        write_frame(&mut s, b"not noise").await.unwrap();
        let (s, _) = listener.accept().await.unwrap();
        drop(s);
    };
    let keys = generate_keys().unwrap();
    let client = async {
        let garbage = handshake(TcpStream::connect(addr).await.unwrap(), &keys, true).await;
        let closed = handshake(TcpStream::connect(addr).await.unwrap(), &keys, true).await;
        (garbage.is_err(), closed.is_err())
    };
    let ((), errs) = tokio::join!(server, client);
    assert_eq!(errs, (true, true));
}

#[tokio::test]
async fn recv_rejects_frame_not_encrypted_by_peer() {
    let ((mut br, _bw, _), (_ar, mut aw, _)) = connect(&generate_keys().unwrap(), &generate_keys().unwrap()).await;
    write_frame(&mut aw.w, &[0; 64]).await.unwrap();
    assert!(br.recv().await.is_err());
}

#[tokio::test]
async fn send_accepts_largest_message_and_rejects_one_byte_more() {
    let ((mut br, _bw, _), (_ar, mut aw, _)) = connect(&generate_keys().unwrap(), &generate_keys().unwrap()).await;
    let data = |n| Msg::Data { path: "~/x".into(), hash: [0; 32], offset: 0, bytes: vec![0; n], eof: true };
    let n = MAX_MSG - 42;
    assert_eq!(postcard::to_stdvec(&data(n)).unwrap().len(), MAX_MSG);
    aw.send(&data(n)).await.unwrap();
    assert!(matches!(br.recv().await.unwrap(), Msg::Data { bytes, .. } if bytes.len() == n));
    assert!(aw.send(&data(n + 1)).await.is_err());
}

#[tokio::test]
async fn send_delivers_full_chunk_with_long_path() {
    let ((mut br, _bw, _), (_ar, mut aw, _)) = connect(&generate_keys().unwrap(), &generate_keys().unwrap()).await;
    let path = format!("~/{}", "p".repeat(4096));
    let msg = Msg::Data { path: path.clone(), hash: [0; 32], offset: u64::MAX, bytes: vec![9; CHUNK], eof: true };
    aw.send(&msg).await.unwrap();
    let got = br.recv().await.unwrap();
    assert!(matches!(got, Msg::Data { path: p, bytes, .. } if p == path && bytes == vec![9; CHUNK]));
}
