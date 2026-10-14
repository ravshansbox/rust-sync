//! Loading or creating this node's key pair, and formatting and parsing node IDs.

use super::PARAMS;
use anyhow::{Context, bail};
use snow::Builder;
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;

/// This node's Noise static key pair; the public key is the node ID.
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
    std::fs::OpenOptions::new().write(true).create(true).truncate(true).mode(0o600).open(&tmp)?.write_all(&[keys.private.as_slice(), keys.public.as_slice()].concat())?;
    std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
    let linked = std::fs::hard_link(&tmp, &path);
    std::fs::remove_file(&tmp)?;
    match linked {
        Ok(()) => Ok(keys),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => load_or_create_keys(dir),
        Err(e) => Err(e.into()),
    }
}

pub(super) fn generate_keys() -> anyhow::Result<Keys> {
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

#[cfg(test)]
mod tests;
