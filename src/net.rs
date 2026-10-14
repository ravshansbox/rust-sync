//! Encrypted connections between nodes.
//!
//! Each node has an X25519 key pair. The public key is the node's ID. Connections
//! use the Noise XX handshake, so both sides learn and check each other's key.
//! Every message is one Noise frame: a 2-byte length, then the ciphertext.

use crate::config::Shared;
use crate::index::Entry;
use anyhow::{Context, bail};
use serde::{Deserialize, Serialize};
use snow::{Builder, TransportState};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

const PARAMS: &str = "Noise_XX_25519_ChaChaPoly_BLAKE2s";
const MAX_FRAME: usize = 65535;
const TAG: usize = 16;
/// Largest file chunk we send in one message, leaving room for the header.
pub const CHUNK: usize = 32 * 1024;
/// Largest serialised message we allow.
pub const MAX_MSG: usize = MAX_FRAME - TAG;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Msg {
    Hello { port: u16, probe: bool, trusts_you: bool },
    Config(Shared),
    Index(Vec<Entry>),
    Request { path: String, hash: [u8; 32] },
    Data { path: String, hash: [u8; 32], offset: u64, bytes: Vec<u8>, eof: bool },
    Unavailable { path: String, hash: [u8; 32] },
    /// Keeps the connection alive, so a dead peer is noticed.
    Ping,
}

mod keys;

pub use keys::{Keys, hex, load_or_create_keys, parse_id, pretty_id, short_id};

async fn write_frame(w: &mut (impl AsyncWriteExt + Unpin), buf: &[u8]) -> std::io::Result<()> {
    let mut out = Vec::with_capacity(buf.len() + 2);
    out.extend_from_slice(&(buf.len() as u16).to_be_bytes());
    out.extend_from_slice(buf);
    w.write_all(&out).await
}

async fn read_frame(r: &mut (impl AsyncReadExt + Unpin)) -> std::io::Result<Vec<u8>> {
    let mut len = [0u8; 2];
    r.read_exact(&mut len).await?;
    let mut buf = vec![0u8; u16::from_be_bytes(len) as usize];
    r.read_exact(&mut buf).await?;
    Ok(buf)
}

pub struct Reader {
    r: OwnedReadHalf,
    t: Arc<Mutex<TransportState>>,
}

pub struct Writer {
    w: OwnedWriteHalf,
    t: Arc<Mutex<TransportState>>,
}

impl Reader {
    pub async fn recv(&mut self) -> anyhow::Result<Msg> {
        let frame = read_frame(&mut self.r).await?;
        let mut plain = vec![0u8; frame.len()];
        let n = self.t.lock().unwrap().read_message(&frame, &mut plain)?;
        Ok(postcard::from_bytes(&plain[..n])?)
    }
}

impl Writer {
    pub async fn send(&mut self, msg: &Msg) -> anyhow::Result<()> {
        let plain = postcard::to_stdvec(msg)?;
        if plain.len() > MAX_MSG {
            bail!("message too large ({} bytes)", plain.len());
        }
        let mut frame = vec![0u8; plain.len() + TAG];
        let n = self.t.lock().unwrap().write_message(&plain, &mut frame)?;
        Ok(write_frame(&mut self.w, &frame[..n]).await?)
    }
}

/// Run the Noise handshake. Returns the two halves of the connection and the peer's ID.
pub async fn handshake(
    stream: TcpStream,
    keys: &Keys,
    initiator: bool,
) -> anyhow::Result<(Reader, Writer, String)> {
    tokio::time::timeout(Duration::from_secs(10), handshake_inner(stream, keys, initiator))
        .await
        .context("handshake timed out")?
}

async fn handshake_inner(
    mut stream: TcpStream,
    keys: &Keys,
    initiator: bool,
) -> anyhow::Result<(Reader, Writer, String)> {
    stream.set_nodelay(true)?;
    let builder = Builder::new(PARAMS.parse()?).local_private_key(&keys.private)?;
    let mut hs = if initiator { builder.build_initiator()? } else { builder.build_responder()? };
    let mut buf = vec![0u8; MAX_FRAME];
    let mut sink = vec![0u8; MAX_FRAME];
    // XX pattern: -> e, <- e ee s es, -> s se
    for step in 0..3 {
        if (step % 2 == 0) == initiator {
            let n = hs.write_message(&[], &mut buf)?;
            write_frame(&mut stream, &buf[..n]).await?;
        } else {
            let frame = read_frame(&mut stream).await?;
            hs.read_message(&frame, &mut sink)?;
        }
    }
    let remote = hex(hs.get_remote_static().context("peer sent no key")?);
    let t = Arc::new(Mutex::new(hs.into_transport_mode()?));
    let (r, w) = stream.into_split();
    Ok((Reader { r, t: t.clone() }, Writer { w, t }, remote))
}

#[cfg(test)]
mod tests {
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
}
