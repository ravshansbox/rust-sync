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
