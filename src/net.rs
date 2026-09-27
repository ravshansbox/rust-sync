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
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
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
    Data { path: String, offset: u64, bytes: Vec<u8>, eof: bool },
    Unavailable { path: String },
    /// Keeps the connection alive, so a dead peer is noticed.
    Ping,
}

pub struct Keys {
    pub private: Vec<u8>,
    pub public: Vec<u8>,
}

impl Keys {
    pub fn id(&self) -> String {
        hex(&self.public)
    }
}

pub fn load_or_create_keys(dir: &Path) -> anyhow::Result<Keys> {
    let path = dir.join("key");
    if let Ok(b) = std::fs::read(&path) {
        if b.len() == 64 {
            return Ok(Keys { private: b[..32].to_vec(), public: b[32..].to_vec() });
        }
        bail!("{} is damaged", path.display());
    }
    let kp = Builder::new(PARAMS.parse()?).generate_keypair()?;
    std::fs::create_dir_all(dir)?;
    std::fs::write(&path, [kp.private.as_slice(), kp.public.as_slice()].concat())?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    Ok(Keys { private: kp.private, public: kp.public })
}

pub fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Node ID in groups of 8 characters, for reading aloud or comparing by eye.
pub fn pretty_id(id: &str) -> String {
    id.as_bytes()
        .chunks(8)
        .map(|c| std::str::from_utf8(c).unwrap().to_uppercase())
        .collect::<Vec<_>>()
        .join("-")
}

/// Accept an ID typed with or without dashes, in any case.
pub fn parse_id(s: &str) -> Option<String> {
    let id: String = s.chars().filter(|c| c.is_ascii_hexdigit()).collect::<String>().to_lowercase();
    (id.len() == 64 && s.chars().all(|c| c.is_ascii_hexdigit() || c == '-' || c == ' ')).then_some(id)
}

/// Short ID used in version vectors: the first 8 bytes of the public key.
pub fn short_id(id: &str) -> u64 {
    u64::from_str_radix(&id[..16], 16).unwrap_or(0)
}

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
        write_frame(&mut self.w, &frame[..n]).await?;
        Ok(())
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
    mut s: TcpStream,
    keys: &Keys,
    initiator: bool,
) -> anyhow::Result<(Reader, Writer, String)> {
    s.set_nodelay(true)?;
    let b = Builder::new(PARAMS.parse()?).local_private_key(&keys.private)?;
    let mut hs = if initiator { b.build_initiator()? } else { b.build_responder()? };
    let mut buf = vec![0u8; MAX_FRAME];
    let mut sink = vec![0u8; MAX_FRAME];
    // XX pattern: -> e, <- e ee s es, -> s se
    for step in 0..3 {
        if (step % 2 == 0) == initiator {
            let n = hs.write_message(&[], &mut buf)?;
            write_frame(&mut s, &buf[..n]).await?;
        } else {
            let frame = read_frame(&mut s).await?;
            hs.read_message(&frame, &mut sink)?;
        }
    }
    let remote = hex(hs.get_remote_static().context("peer sent no key")?);
    let t = Arc::new(Mutex::new(hs.into_transport_mode()?));
    let (r, w) = s.into_split();
    Ok((Reader { r, t: t.clone() }, Writer { w, t }, remote))
}
