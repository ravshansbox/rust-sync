use super::*;
use crate::index::Vv;

fn meta(mtime: i64, by: u64) -> FileMeta {
    FileMeta { hash: Some([1; 32]), size: 1, mtime, mode: 0o644, vv: Vv::default(), by }
}

#[test]
fn conflict_name_keeps_extension_and_tags_loser() {
    let loser = meta(1_700_000_000_123_456_789, 0xdead_beef_0000_0001);
    assert_eq!(
        conflict_name(Path::new("/d/notes.md"), &loser),
        PathBuf::from("/d/notes.sync-conflict-1700000000-deadbeef.md")
    );
    assert_eq!(
        conflict_name(Path::new("/d/Makefile"), &loser),
        PathBuf::from("/d/Makefile.sync-conflict-1700000000-deadbeef")
    );
    assert_eq!(
        conflict_name(Path::new("/d/a.tar.gz"), &meta(0, 1 << 32)),
        PathBuf::from("/d/a.tar.sync-conflict-0-00000001.gz")
    );
}

#[test]
fn conflict_name_of_a_longest_allowed_name_is_shortened_at_a_character_boundary() {
    let loser = meta(1_700_000_000_123_456_789, 0xdead_beef_0000_0001);
    let tag = "sync-conflict-1700000000-deadbeef";
    let long_stem = Path::new("/d").join(format!("{}.md", "é".repeat(126)));
    assert_eq!(conflict_name(&long_stem, &loser), Path::new("/d").join(format!("{}.{tag}.md", "é".repeat(109))));
    let long_ext = Path::new("/d").join(format!("a.{}", "x".repeat(253)));
    assert_eq!(conflict_name(&long_ext, &loser), Path::new("/d").join(format!("a.{}.{tag}", "x".repeat(219))));
}
