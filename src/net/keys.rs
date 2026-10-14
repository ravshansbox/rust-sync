use super::PARAMS;
use anyhow::{Context, bail};
use snow::Builder;
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;

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
mod tests {
    use super::*;
    use crate::paths::test_dir;

    const ID: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

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
        let dir = test_dir("keys");
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
        let dir = test_dir("keys-unreadable");
        std::fs::create_dir_all(dir.join("key")).unwrap();
        assert!(load_or_create_keys(&dir).is_err());
        assert!(dir.join("key").is_dir());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn keys_created_at_the_same_time_are_the_same_key() {
        let dir = test_dir("keys-race");
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
}
