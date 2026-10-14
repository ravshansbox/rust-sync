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
    Data { path: String, hash: [u8; 32], offset: u64, bytes: Vec<u8>, eof: bool },
    Unavailable { path: String, hash: [u8; 32] },
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
    match std::fs::read(&path) {
        Ok(b) if b.len() == 64 => return Ok(Keys { private: b[..32].to_vec(), public: b[32..].to_vec() }),
        Ok(_) => bail!("{} is damaged", path.display()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e).with_context(|| format!("cannot read {}", path.display())),
    }
    let keys = generate_keys()?;
    std::fs::create_dir_all(dir)?;
    // Another process may be creating a key now. Linking fails if one exists, so all use the same key.
    let tmp = dir.join(format!("key.{}.tmp", hex(&keys.public[..8])));
    std::fs::write(&tmp, [keys.private.as_slice(), keys.public.as_slice()].concat())?;
    std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
    let linked = std::fs::hard_link(&tmp, &path);
    std::fs::remove_file(&tmp)?;
    match linked {
        Ok(()) => Ok(keys),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => load_or_create_keys(dir),
        Err(e) => Err(e.into()),
    }
}

fn generate_keys() -> anyhow::Result<Keys> {
    let kp = Builder::new(PARAMS.parse()?).generate_keypair()?;
    Ok(Keys { private: kp.private, public: kp.public })
}

pub fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Node ID in groups of 8 characters, for reading aloud or comparing by eye.
pub fn pretty_id(id: &str) -> String {
    id.chars()
        .collect::<Vec<_>>()
        .chunks(8)
        .map(|c| c.iter().collect::<String>().to_uppercase())
        .collect::<Vec<_>>()
        .join("-")
}

/// Accept an ID typed with or without dashes, in any case.
pub fn parse_id(s: &str) -> Option<String> {
    let id: String = s.chars().filter(char::is_ascii_hexdigit).map(|c| c.to_ascii_lowercase()).collect();
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
    use super::*;
    use tokio::net::TcpListener;

    const ID: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

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

    #[test]
    fn hex_is_lowercase_two_digits_per_byte() {
        assert_eq!(hex(&[0x00, 0x0a, 0xff]), "000aff");
        assert_eq!(hex(&[]), "");
    }

    #[test]
    fn pretty_id_groups_in_eights_upper_case() {
        assert_eq!(
            pretty_id(ID),
            "01234567-89ABCDEF-01234567-89ABCDEF-01234567-89ABCDEF-01234567-89ABCDEF"
        );
    }

    #[test]
    fn parse_id_accepts_pretty_and_plain_forms() {
        assert_eq!(parse_id(ID).as_deref(), Some(ID));
        assert_eq!(parse_id(&pretty_id(ID)).as_deref(), Some(ID));
        assert_eq!(parse_id(&pretty_id(ID).replace('-', " ")).as_deref(), Some(ID));
    }

    #[test]
    fn parse_id_rejects_wrong_length_or_characters() {
        assert_eq!(parse_id(&ID[..62]), None);
        assert_eq!(parse_id(&format!("{ID}00")), None);
        assert_eq!(parse_id(&format!("{}g", &ID[..63])), None);
        assert_eq!(parse_id(&format!("{ID}x")), None);
    }

    #[test]
    fn short_id_reads_first_eight_bytes() {
        assert_eq!(short_id(ID), 0x0123_4567_89ab_cdef);
    }

    #[test]
    fn load_or_create_keys_persists_private_key_and_rejects_damaged_file() {
        let dir = std::env::temp_dir().join(format!("rust-sync-keys-{}", std::process::id()));
        let created = load_or_create_keys(&dir).unwrap();
        let path = dir.join("key");
        assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
        let loaded = load_or_create_keys(&dir).unwrap();
        assert_eq!(loaded.id(), created.id());
        assert_eq!(loaded.private, created.private);
        std::fs::write(&path, [0u8; 10]).unwrap();
        assert!(load_or_create_keys(&dir).is_err());
        std::fs::write(&path, [0u8; 65]).unwrap();
        assert!(load_or_create_keys(&dir).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn load_or_create_keys_fails_when_key_exists_but_cannot_be_read() {
        let dir = std::env::temp_dir().join(format!("rust-sync-keys-unreadable-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("key")).unwrap();
        assert!(load_or_create_keys(&dir).is_err());
        assert!(dir.join("key").is_dir());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn keys_created_at_the_same_time_are_the_same_key() {
        let dir = std::env::temp_dir().join(format!("rust-sync-keys-race-{}", std::process::id()));
        for _ in 0..20 {
            let _ = std::fs::remove_dir_all(&dir);
            let ids: Vec<String> = std::thread::scope(|s| {
                let handles: Vec<_> = (0..8).map(|_| s.spawn(|| load_or_create_keys(&dir).unwrap().id())).collect();
                handles.into_iter().map(|h| h.join().unwrap()).collect()
            });
            assert!(ids.iter().all(|id| *id == ids[0]), "{ids:?}");
            assert_eq!(load_or_create_keys(&dir).unwrap().id(), ids[0]);
        }
        std::fs::remove_dir_all(&dir).unwrap();
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
