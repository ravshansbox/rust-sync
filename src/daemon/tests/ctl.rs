use super::*;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;

#[tokio::test]
async fn ctl_loop_passes_request_to_daemon_and_sends_back_its_reply() {
    let sock = std::env::temp_dir().join(format!("rust-sync-ctl-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&sock);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let task = tokio::spawn(ctl_loop(UnixListener::bind(&sock).unwrap(), tx));
    let (r, mut w) = tokio::net::UnixStream::connect(&sock).await.unwrap().into_split();
    w.write_all(b"{\"RemoveNode\":{\"id\":\"abcd\"}}\n").await.unwrap();
    let Some(Event::Ctl(Req::RemoveNode { id }, reply)) = rx.recv().await else { panic!() };
    assert_eq!(id, "abcd");
    reply.send(Resp { ok: true, text: "done".into() }).unwrap();
    let mut line = String::new();
    BufReader::new(r).read_line(&mut line).await.unwrap();
    assert_eq!(line, "{\"ok\":true,\"text\":\"done\"}\n");
    task.abort();
    std::fs::remove_file(&sock).unwrap();
}

#[tokio::test]
async fn ctl_loop_rejects_a_request_it_cannot_read_instead_of_closing() {
    let sock = std::env::temp_dir().join(format!("rust-sync-ctl-bad-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&sock);
    let (tx, _rx) = mpsc::unbounded_channel();
    let task = tokio::spawn(ctl_loop(UnixListener::bind(&sock).unwrap(), tx));
    let (r, mut w) = tokio::net::UnixStream::connect(&sock).await.unwrap().into_split();
    w.write_all(b"{\"Pause\":{}}\n").await.unwrap();
    let mut line = String::new();
    BufReader::new(r).read_line(&mut line).await.unwrap();
    let resp: Resp = serde_json::from_str(&line).unwrap();
    assert!(!resp.ok);
    assert!(resp.text.starts_with("the daemon cannot read this request"), "{}", resp.text);
    task.abort();
    std::fs::remove_file(&sock).unwrap();
}
