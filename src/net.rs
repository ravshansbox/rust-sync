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

/// Noise protocol pattern and ciphers used for every connection.
const PARAMS: &str = "Noise_XX_25519_ChaChaPoly_BLAKE2s";
/// Largest Noise frame, the most a 2-byte length can describe.
const MAX_FRAME: usize = 65535;
/// Bytes the cipher's authentication tag adds to each frame.
const TAG: usize = 16;
/// Largest file chunk we send in one message, leaving room for the header.
pub const CHUNK: usize = 32 * 1024;
/// Largest serialised message we allow.
pub const MAX_MSG: usize = MAX_FRAME - TAG;
/// Give up on a handshake that has not finished within this long.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// A message sent between nodes over an encrypted connection.
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

/// Write one frame: a 2-byte big-endian length, then the bytes.
async fn write_frame(w: &mut (impl AsyncWriteExt + Unpin), buf: &[u8]) -> std::io::Result<()> {
    let mut out = Vec::with_capacity(buf.len() + 2);
    out.extend_from_slice(&(buf.len() as u16).to_be_bytes());
    out.extend_from_slice(buf);
    w.write_all(&out).await
}

/// Read one frame written by `write_frame` and return its bytes.
async fn read_frame(r: &mut (impl AsyncReadExt + Unpin)) -> std::io::Result<Vec<u8>> {
    let mut len = [0u8; 2];
    r.read_exact(&mut len).await?;
    let mut buf = vec![0u8; u16::from_be_bytes(len) as usize];
    r.read_exact(&mut buf).await?;
    Ok(buf)
}

/// Receiving half of an encrypted connection.
pub struct Reader {
    r: OwnedReadHalf,
    t: Arc<Mutex<TransportState>>,
}

/// Sending half of an encrypted connection.
pub struct Writer {
    w: OwnedWriteHalf,
    t: Arc<Mutex<TransportState>>,
}

impl Reader {
    /// Read the next frame, decrypt it and decode the message.
    pub async fn recv(&mut self) -> anyhow::Result<Msg> {
        let frame = read_frame(&mut self.r).await?;
        let mut plain = vec![0u8; frame.len()];
        let n = self.t.lock().unwrap().read_message(&frame, &mut plain)?;
        Ok(postcard::from_bytes(&plain[..n])?)
    }
}

impl Writer {
    /// Encode the message, encrypt it and write it as one frame. Fails if it is larger than `MAX_MSG`.
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
    tokio::time::timeout(HANDSHAKE_TIMEOUT, handshake_inner(stream, keys, initiator))
        .await
        .context("handshake timed out")?
}

/// Run the three XX handshake steps without a time limit, then split the stream.
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
mod tests;
