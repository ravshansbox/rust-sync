use super::*;
use crate::daemon::remote::conflict_name;
use crate::index::Action;

#[test]
fn downloaded_conflict_winner_replaces_the_file_and_keeps_the_local_copy() {
    let mut d = daemon();
    let dir = test_dir("conflict");
    std::fs::create_dir_all(&dir).unwrap();
    let lp = dir.join("f.txt");
    let tmp = dir.join("tmp");
    std::fs::write(&lp, "mine").unwrap();
    std::fs::write(&tmp, "theirs").unwrap();
    let mut local = meta(0, 1);
    local.vv = Vv(vec![(1, 1)]);
    d.index.insert("~/s/f.txt".into(), local.clone());
    let mut r = meta(1, 2);
    r.hash = Some([2; 32]);
    r.vv = Vv(vec![(2, 1)]);
    d.apply("aa", "~/s/f.txt", &lp, r.clone(), Some(tmp.clone()));
    assert_eq!(std::fs::read_to_string(&lp).unwrap(), "theirs");
    assert_eq!(std::fs::read_to_string(conflict_name(&lp, &local)).unwrap(), "mine");
    assert!(!tmp.exists());
    assert_eq!(d.index["~/s/f.txt"].hash, r.hash);
    assert_eq!(d.outbox.len(), 1);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn downloaded_conflict_winner_that_cannot_be_moved_into_place_leaves_the_local_file_where_it_was() {
    let mut d = daemon();
    let dir = test_dir("conflict-unwritable");
    std::fs::create_dir_all(&dir).unwrap();
    let lp = dir.join("f.txt");
    std::fs::write(&lp, "mine").unwrap();
    let mut local = meta(0, 1);
    local.vv = Vv(vec![(1, 1)]);
    d.index.insert("~/s/f.txt".into(), local.clone());
    let mut r = meta(1, 2);
    r.hash = Some([2; 32]);
    r.vv = Vv(vec![(2, 1)]);
    d.apply("aa", "~/s/f.txt", &lp, r, Some(dir.join("gone")));
    assert_eq!(std::fs::read_to_string(&lp).unwrap(), "mine");
    assert!(!conflict_name(&lp, &local).exists());
    assert_eq!(d.index["~/s/f.txt"], local);
    assert!(d.outbox.is_empty());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn downloaded_conflict_winner_is_dropped_when_the_local_version_cannot_be_kept_as_a_copy() {
    let mut d = daemon();
    let dir = test_dir("conflict-copy-blocked");
    std::fs::create_dir_all(&dir).unwrap();
    let (lp, tmp) = (dir.join("f.txt"), dir.join(".f.txt.rsync-tmp"));
    std::fs::write(&lp, "mine").unwrap();
    std::fs::write(&tmp, "theirs").unwrap();
    let mut local = meta(0, 1);
    local.vv = Vv(vec![(1, 1)]);
    d.index.insert("~/s/f.txt".into(), local.clone());
    let copy = conflict_name(&lp, &local);
    std::fs::create_dir_all(copy.join("sub")).unwrap();
    let mut r = meta(1, 2);
    r.hash = Some([2; 32]);
    r.vv = Vv(vec![(2, 1)]);
    d.apply("aa", "~/s/f.txt", &lp, r, Some(tmp.clone()));
    assert_eq!(std::fs::read_to_string(&lp).unwrap(), "mine");
    assert!(copy.join("sub").is_dir());
    assert!(!tmp.exists());
    assert_eq!(d.index["~/s/f.txt"], local);
    assert!(d.outbox.is_empty());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn downloaded_conflict_winner_with_the_longest_allowed_name_keeps_the_local_copy() {
    let mut d = daemon();
    let dir = test_dir("conflict-long-name");
    std::fs::create_dir_all(&dir).unwrap();
    let lp = dir.join(format!("{}.txt", "n".repeat(251)));
    let tmp = dir.join("tmp");
    std::fs::write(&lp, "mine").unwrap();
    std::fs::write(&tmp, "theirs").unwrap();
    let mut local = meta(0, 1);
    local.vv = Vv(vec![(1, 1)]);
    d.index.insert("~/s/f.txt".into(), local);
    let mut r = meta(1, 2);
    r.hash = Some([2; 32]);
    r.vv = Vv(vec![(2, 1)]);
    d.apply("aa", "~/s/f.txt", &lp, r.clone(), Some(tmp.clone()));
    assert_eq!(std::fs::read_to_string(&lp).unwrap(), "theirs");
    let copies: Vec<_> = std::fs::read_dir(&dir).unwrap().map(|e| e.unwrap().path()).filter(|p| *p != lp).collect();
    assert_eq!(copies.len(), 1);
    assert_eq!(std::fs::read_to_string(&copies[0]).unwrap(), "mine");
    assert_eq!(d.index["~/s/f.txt"].hash, r.hash);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn downloaded_conflict_winner_is_sent_on_as_newer_than_both_versions() {
    let mut d = daemon();
    let dir = test_dir("conflict-history");
    std::fs::create_dir_all(&dir).unwrap();
    let (lp, tmp) = (dir.join("f.txt"), dir.join(".f.txt.rsync-tmp"));
    std::fs::write(&lp, "mine").unwrap();
    std::fs::write(&tmp, "theirs").unwrap();
    let mut local = meta(0, 1);
    local.vv = Vv(vec![(1, 3)]);
    d.index.insert("~/s/f.txt".into(), local.clone());
    let mut r = meta(1, 2);
    r.hash = Some([2; 32]);
    r.vv = Vv(vec![(2, 1)]);
    d.apply("aa", "~/s/f.txt", &lp, r.clone(), Some(tmp));
    let sent = &d.outbox[0].meta;
    assert_eq!(index::decide(Some(&local), sent), Action::Take);
    assert_eq!(index::decide(Some(&r), sent), Action::Take);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn downloaded_change_that_beats_a_local_deletion_restores_the_file() {
    let mut d = daemon();
    let dir = test_dir("restore");
    std::fs::create_dir_all(&dir).unwrap();
    let (lp, tmp) = (dir.join("f.txt"), dir.join(".f.txt.rsync-tmp"));
    std::fs::write(&tmp, "theirs").unwrap();
    let mut local = meta(9, 1);
    (local.hash, local.vv) = (None, Vv(vec![(1, 1)]));
    d.index.insert("~/s/f.txt".into(), local);
    let mut r = meta(5, 2);
    r.vv = Vv(vec![(2, 1)]);
    d.apply("aa", "~/s/f.txt", &lp, r.clone(), Some(tmp));
    assert_eq!(std::fs::read_to_string(&lp).unwrap(), "theirs");
    assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1);
    r.vv = Vv(vec![(1, 1), (2, 1)]);
    assert_eq!(d.index["~/s/f.txt"], r);
    assert_eq!(d.outbox.len(), 1);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn newer_remote_deletion_removes_the_local_file() {
    let mut d = daemon();
    let dir = test_dir("delete");
    std::fs::create_dir_all(&dir).unwrap();
    let lp = dir.join("f.txt");
    std::fs::write(&lp, "mine").unwrap();
    let mut local = meta(0, 1);
    local.vv = Vv(vec![(1, 1)]);
    d.index.insert("~/s/f.txt".into(), local);
    let mut r = meta(1, 1);
    r.hash = None;
    r.vv = Vv(vec![(1, 2)]);
    d.apply("aa", "~/s/f.txt", &lp, r, None);
    assert!(!lp.exists());
    assert!(d.index["~/s/f.txt"].deleted());
    assert_eq!(d.outbox.len(), 1);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn newer_remote_deletion_of_a_file_already_gone_is_still_recorded() {
    let mut d = daemon();
    let lp = Path::new("/nonexistent/f.txt");
    let mut local = meta(0, 1);
    local.vv = Vv(vec![(1, 1)]);
    d.index.insert("~/s/f.txt".into(), local);
    let mut r = meta(1, 1);
    r.hash = None;
    r.vv = Vv(vec![(1, 2)]);
    d.apply("aa", "~/s/f.txt", lp, r, None);
    assert!(d.index["~/s/f.txt"].deleted());
    assert_eq!(d.outbox.len(), 1);
}

#[test]
fn newer_remote_deletion_that_cannot_remove_the_local_file_is_not_recorded() {
    let mut d = daemon();
    let dir = test_dir("delete-locked");
    std::fs::create_dir_all(&dir).unwrap();
    let lp = dir.join("f.txt");
    std::fs::write(&lp, "mine").unwrap();
    let mut local = meta(0, 1);
    local.vv = Vv(vec![(1, 1)]);
    d.index.insert("~/s/f.txt".into(), local.clone());
    let mut r = meta(1, 1);
    r.hash = None;
    r.vv = Vv(vec![(1, 2)]);
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555)).unwrap();
    d.apply("aa", "~/s/f.txt", &lp, r, None);
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(std::fs::read_to_string(&lp).unwrap(), "mine");
    assert_eq!(d.index["~/s/f.txt"], local);
    assert!(d.outbox.is_empty());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn download_that_cannot_be_moved_into_place_is_dropped_and_not_recorded() {
    let mut d = daemon();
    let dir = test_dir("unwritable");
    let (lp, tmp) = (dir.join("f.txt"), dir.join(".f.txt.rsync-tmp"));
    std::fs::create_dir_all(lp.join("sub")).unwrap();
    std::fs::write(&tmp, "theirs").unwrap();
    d.apply("aa", "~/s/f.txt", &lp, meta(1, 2), Some(tmp.clone()));
    assert!(!tmp.exists());
    assert!(lp.is_dir());
    assert!(d.index.is_empty());
    assert!(d.outbox.is_empty());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn downloaded_change_that_beats_a_local_deletion_does_not_move_a_folder_now_at_its_path() {
    let mut d = daemon();
    let dir = test_dir("deleted-became-folder");
    let (lp, tmp) = (dir.join("f.txt"), dir.join(".f.txt.rsync-tmp"));
    std::fs::create_dir_all(&lp).unwrap();
    std::fs::write(lp.join("a"), "mine").unwrap();
    std::fs::write(&tmp, "theirs").unwrap();
    let mut local = meta(5, 1);
    (local.hash, local.vv) = (None, Vv(vec![(1, 1)]));
    d.index.insert("~/s/f.txt".into(), local.clone());
    let mut r = meta(1, 2);
    r.vv = Vv(vec![(2, 1)]);
    d.apply("aa", "~/s/f.txt", &lp, r, Some(tmp.clone()));
    assert_eq!(std::fs::read_to_string(lp.join("a")).unwrap(), "mine");
    assert!(!conflict_name(&lp, &local).exists());
    assert!(!tmp.exists());
    assert_eq!(d.index["~/s/f.txt"], local);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn same_content_from_remote_merges_history_and_keeps_local_size_and_mtime() {
    let mut d = daemon();
    let lp = Path::new("/nonexistent/f.txt");
    for (remote_vv, want) in [(vec![(2, 1)], vec![(1, 1), (2, 1)]), (vec![(1, 2)], vec![(1, 2)])] {
        let mut local = meta(5, 1);
        local.vv = Vv(vec![(1, 1)]);
        d.index.insert("~/s/f.txt".into(), local);
        d.outbox.clear();
        let mut r = meta(9, 2);
        r.size = 7;
        r.vv = Vv(remote_vv);
        d.apply("aa", "~/s/f.txt", lp, r, None);
        let got = &d.index["~/s/f.txt"];
        assert_eq!(got.vv, Vv(want));
        assert_eq!((got.size, got.mtime), (1, 5));
        assert_eq!(d.outbox.len(), 1);
        assert!(d.downloads.is_empty());
    }
}

#[test]
fn newer_remote_deletion_of_a_file_already_deleted_is_taken_as_is() {
    let mut d = daemon();
    let mut local = meta(5, 1);
    (local.hash, local.size, local.vv) = (None, 0, Vv(vec![(1, 1)]));
    d.index.insert("~/s/f.txt".into(), local);
    let mut r = meta(9, 2);
    (r.hash, r.size, r.vv) = (None, 3, Vv(vec![(1, 2)]));
    d.apply("aa", "~/s/f.txt", Path::new("/nonexistent/f.txt"), r.clone(), None);
    assert_eq!(d.index["~/s/f.txt"], r);
    assert_eq!(d.outbox.len(), 1);
}

#[test]
fn same_content_with_a_new_mode_from_remote_changes_only_the_permissions() {
    let mut d = daemon();
    let dir = test_dir("mode");
    std::fs::create_dir_all(&dir).unwrap();
    let lp = dir.join("f.sh");
    std::fs::write(&lp, "mine").unwrap();
    std::fs::set_permissions(&lp, std::fs::Permissions::from_mode(0o644)).unwrap();
    let mut local = meta(0, 1);
    local.vv = Vv(vec![(1, 1)]);
    d.index.insert("~/s/f.sh".into(), local);
    let mut r = meta(0, 1);
    r.mode = 0o755;
    r.vv = Vv(vec![(1, 2)]);
    d.apply("aa", "~/s/f.sh", &lp, r, None);
    assert_eq!(std::fs::metadata(&lp).unwrap().permissions().mode() & 0o777, 0o755);
    assert_eq!(std::fs::read_to_string(&lp).unwrap(), "mine");
    assert_eq!(d.index["~/s/f.sh"].mode, 0o755);
    assert!(d.downloads.is_empty());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn concurrent_same_content_with_another_mode_keeps_the_mode_of_the_conflict_winner() {
    let dir = test_dir("concurrent-mode");
    std::fs::create_dir_all(&dir).unwrap();
    let lp = dir.join("f.sh");
    std::fs::write(&lp, "same").unwrap();
    for (local_mtime, remote_mtime, want) in [(9, 5, 0o755), (5, 9, 0o644)] {
        let mut d = daemon();
        std::fs::set_permissions(&lp, std::fs::Permissions::from_mode(0o755)).unwrap();
        let mut local = meta(local_mtime, 1);
        (local.mode, local.vv) = (0o755, Vv(vec![(1, 1)]));
        d.index.insert("~/s/f.sh".into(), local);
        let mut r = meta(remote_mtime, 2);
        r.vv = Vv(vec![(2, 1)]);
        d.apply("aa", "~/s/f.sh", &lp, r, None);
        assert_eq!(std::fs::metadata(&lp).unwrap().permissions().mode() & 0o777, want);
        assert_eq!(d.index["~/s/f.sh"].mode, want);
    }
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn downloaded_conflict_loser_changes_nothing_and_removes_the_temp_file() {
    let mut d = daemon();
    let dir = test_dir("local-wins");
    std::fs::create_dir_all(&dir).unwrap();
    let (lp, tmp) = (dir.join("f.txt"), dir.join(".f.txt.rsync-tmp"));
    std::fs::write(&lp, "mine").unwrap();
    std::fs::write(&tmp, "theirs").unwrap();
    let mut local = meta(9, 1);
    local.vv = Vv(vec![(1, 1)]);
    d.index.insert("~/s/f.txt".into(), local.clone());
    let mut r = meta(5, 2);
    r.hash = Some([2; 32]);
    r.vv = Vv(vec![(2, 1)]);
    d.apply("aa", "~/s/f.txt", &lp, r, Some(tmp.clone()));
    assert_eq!(std::fs::read_to_string(&lp).unwrap(), "mine");
    assert!(!tmp.exists());
    assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1);
    assert_eq!(d.index["~/s/f.txt"], local);
    assert!(d.outbox.is_empty());
    std::fs::remove_dir_all(&dir).unwrap();
}
