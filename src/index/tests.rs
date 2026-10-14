use super::*;

use crate::paths::test_dir;

fn vv(pairs: &[(u64, u64)]) -> Vv {
    Vv(pairs.to_vec())
}

fn meta(hash: Option<u8>, mtime: i64, by: u64, v: Vv) -> FileMeta {
    FileMeta { hash: hash.map(|b| [b; 32]), size: 0, mtime, mode: 0o644, vv: v, by }
}

#[test]
fn bumped_increments_existing_and_adds_new_nodes_sorted() {
    assert_eq!(vv(&[(2, 1)]).bumped(2), vv(&[(2, 2)]));
    assert_eq!(vv(&[(2, 1)]).bumped(1), vv(&[(1, 1), (2, 1)]));
}

#[test]
fn merged_takes_the_maximum_per_node() {
    let a = vv(&[(1, 3), (2, 1)]);
    let b = vv(&[(2, 4), (3, 1)]);
    assert_eq!(a.merged(&b), vv(&[(1, 3), (2, 4), (3, 1)]));
}

#[test]
fn merged_keeps_our_count_when_it_is_higher() {
    assert_eq!(vv(&[(1, 5)]).merged(&vv(&[(1, 2)])), vv(&[(1, 5)]));
}

#[test]
fn compare_orders_version_vectors() {
    assert_eq!(vv(&[(1, 1)]).compare(&vv(&[(1, 1)])), Order::Equal);
    assert_eq!(vv(&[(1, 2)]).compare(&vv(&[(1, 1)])), Order::Newer);
    assert_eq!(vv(&[(1, 1)]).compare(&vv(&[(1, 1), (2, 1)])), Order::Older);
    assert_eq!(vv(&[(1, 2)]).compare(&vv(&[(1, 1), (2, 1)])), Order::Concurrent);
}

#[test]
fn compare_counts_nodes_only_we_know() {
    assert_eq!(vv(&[(1, 1), (2, 1)]).compare(&vv(&[(1, 1)])), Order::Newer);
}

#[test]
fn decide_takes_remote_when_unknown_locally_or_newer() {
    let r = meta(Some(1), 0, 1, vv(&[(1, 2)]));
    assert_eq!(decide(None, &r), Action::Take);
    assert_eq!(decide(Some(&meta(Some(2), 0, 1, vv(&[(1, 1)]))), &r), Action::Take);
}

#[test]
fn decide_ignores_equal_or_older_remote() {
    let l = meta(Some(1), 0, 1, vv(&[(1, 2)]));
    assert_eq!(decide(Some(&l), &meta(Some(1), 0, 1, vv(&[(1, 2)]))), Action::Nothing);
    assert_eq!(decide(Some(&l), &meta(Some(2), 0, 1, vv(&[(1, 1)]))), Action::Nothing);
}

#[test]
fn decide_concurrent_same_content_is_same() {
    let l = meta(Some(1), 5, 1, vv(&[(1, 1)]));
    let r = meta(Some(1), 9, 2, vv(&[(2, 1)]));
    assert_eq!(decide(Some(&l), &r), Action::Same);
    let l = meta(None, 5, 1, vv(&[(1, 1)]));
    let r = meta(None, 9, 2, vv(&[(2, 1)]));
    assert_eq!(decide(Some(&l), &r), Action::Same);
}

#[test]
fn decide_concurrent_newer_mtime_wins_then_higher_node() {
    let l = meta(Some(1), 5, 1, vv(&[(1, 1)]));
    assert_eq!(decide(Some(&l), &meta(Some(2), 9, 2, vv(&[(2, 1)]))), Action::Conflict);
    assert_eq!(decide(Some(&l), &meta(Some(2), 1, 2, vv(&[(2, 1)]))), Action::LocalWins);
    assert_eq!(decide(Some(&l), &meta(Some(2), 5, 2, vv(&[(2, 1)]))), Action::Conflict);
    assert_eq!(decide(Some(&l), &meta(Some(2), 5, 0, vv(&[(0, 1)]))), Action::LocalWins);
    assert_eq!(decide(Some(&l), &meta(Some(2), 5, 1, vv(&[(2, 1)]))), Action::LocalWins);
}

#[test]
fn decide_concurrent_change_beats_deletion() {
    let changed = meta(Some(1), 1, 1, vv(&[(1, 1)]));
    let deleted = meta(None, 9, 2, vv(&[(2, 1)]));
    assert_eq!(decide(Some(&changed), &deleted), Action::LocalWins);
    assert_eq!(decide(Some(&deleted), &changed), Action::Conflict);
}

#[test]
fn under_matches_prefix_and_its_children_only_with_or_without_trailing_slash() {
    let m = meta(Some(1), 0, 1, Vv::default());
    let index: Index = ["~/a", "~/a/b", "~/a/c/d", "~/ab", "~/b"].map(|k| (k.to_string(), m.clone())).into();
    let got: Vec<_> = under(&index, "~/a").map(|(k, _)| k.clone()).collect();
    assert_eq!(got, ["~/a", "~/a/b", "~/a/c/d"]);
    let got: Vec<_> = under(&index, "~/a/").map(|(k, _)| k.clone()).collect();
    assert_eq!(got, ["~/a/b", "~/a/c/d"]);
}

#[test]
fn load_gives_empty_for_missing_or_damaged_file_and_save_round_trips() {
    let dir = test_dir("index");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("index.bin");
    assert!(load(&path).is_empty());
    let index: Index = [("~/a".to_string(), meta(Some(7), 42, 3, vv(&[(3, 2)])))].into();
    save(&index, &path).unwrap();
    assert_eq!(load(&path), index);
    assert!(!path.with_extension("tmp").exists());
    std::fs::write(&path, [0xff; 4]).unwrap();
    assert!(load(&path).is_empty());
    std::fs::remove_dir_all(&dir).unwrap();
}
