use super::*;

#[test]
fn local_file_changes_are_recorded_but_a_touch_is_not_a_new_version() {
    let mut d = daemon();
    let me = d.me_short;
    let dir = test_dir("check");
    std::fs::create_dir_all(&dir).unwrap();
    let lp = dir.join("f.txt");
    let p = "~/s/f.txt";
    d.check_file(p, &lp);
    assert!(d.index.is_empty());
    std::fs::write(&lp, "one").unwrap();
    d.check_file(p, &lp);
    assert_eq!(d.index[p].vv, Vv(vec![(me, 1)]));
    assert_eq!(d.index[p].hash, Some(index::hash_file(&lp).unwrap().0));
    assert_eq!(d.outbox.len(), 1);
    d.check_file(p, &lp);
    assert_eq!(d.outbox.len(), 1);
    let f = std::fs::File::options().write(true).open(&lp).unwrap();
    f.set_modified(std::time::SystemTime::UNIX_EPOCH).unwrap();
    d.check_file(p, &lp);
    assert_eq!((d.index[p].mtime, d.index[p].vv.clone()), (0, Vv(vec![(me, 1)])));
    assert_eq!(d.outbox.len(), 1);
    std::fs::write(&lp, "two").unwrap();
    d.check_file(p, &lp);
    assert_eq!(d.index[p].vv, Vv(vec![(me, 2)]));
    assert_eq!(d.index[p].size, 3);
    assert_eq!(d.outbox.len(), 2);
    std::fs::remove_file(&lp).unwrap();
    d.check_file(p, &lp);
    assert!(d.index[p].deleted());
    assert_eq!(d.index[p].vv, Vv(vec![(me, 3)]));
    assert_eq!(d.outbox.len(), 3);
    d.check_file(p, &lp);
    assert_eq!(d.outbox.len(), 3);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn file_that_grows_while_being_hashed_is_announced_with_the_size_of_the_hashed_content() {
    let mut d = daemon();
    let dir = test_dir("grows");
    std::fs::create_dir_all(&dir).unwrap();
    let lp = dir.join("f.txt");
    let p = "~/s/f.txt";
    std::fs::write(&lp, "one").unwrap();
    let before = index::stat(&lp).unwrap();
    std::fs::write(&lp, "one two").unwrap();
    d.check_stat(p, &lp, Some(before));
    assert_eq!(d.index[p].hash, Some(index::hash_file(&lp).unwrap().0));
    assert_eq!(d.index[p].size, 7);
    assert_eq!(d.outbox[0].meta.size, 7);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn file_recreated_with_the_same_stat_as_its_deletion_is_a_new_version() {
    let mut d = daemon();
    let me = d.me_short;
    let dir = test_dir("recreate");
    std::fs::create_dir_all(&dir).unwrap();
    let lp = dir.join("f.txt");
    let p = "~/s/f.txt";
    let f = std::fs::File::create(&lp).unwrap();
    f.set_modified(std::time::SystemTime::UNIX_EPOCH).unwrap();
    std::fs::set_permissions(&lp, std::fs::Permissions::from_mode(0o644)).unwrap();
    let gone = FileMeta { hash: None, size: 0, mtime: 0, mode: 0o644, vv: Vv(vec![(me, 1)]), by: me };
    d.index.insert(p.into(), gone);
    d.check_file(p, &lp);
    assert_eq!(d.index[p].hash, Some(*blake3::hash(b"").as_bytes()));
    assert_eq!(d.index[p].vv, Vv(vec![(me, 2)]));
    assert_eq!(d.outbox.len(), 1);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn local_permission_change_is_a_new_version() {
    let mut d = daemon();
    let me = d.me_short;
    let dir = test_dir("chmod");
    std::fs::create_dir_all(&dir).unwrap();
    let lp = dir.join("f.sh");
    let p = "~/s/f.sh";
    std::fs::write(&lp, "x").unwrap();
    std::fs::set_permissions(&lp, std::fs::Permissions::from_mode(0o644)).unwrap();
    d.check_file(p, &lp);
    std::fs::set_permissions(&lp, std::fs::Permissions::from_mode(0o755)).unwrap();
    d.check_file(p, &lp);
    assert_eq!(d.index[p].vv, Vv(vec![(me, 2)]));
    assert_eq!(d.index[p].mode, 0o755);
    assert_eq!(d.outbox.len(), 2);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn remote_entries_outside_roots_or_for_ignored_names_are_skipped_and_unseen_local_changes_count() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("entry");
    let root = dir.join("in");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(dir.join("out")).unwrap();
    d.cfg.set_root(&me, root.to_str().unwrap(), true, false);
    let mut gone = meta(0, 2);
    gone.hash = None;
    gone.vv = Vv(vec![(2, 1)]);
    for lp in [dir.join("out/f.txt"), root.join(".DS_Store"), root.join("f.txt")] {
        std::fs::write(&lp, "x").unwrap();
        d.on_entry("aa", Entry { path: lp.to_str().unwrap().into(), meta: gone.clone() });
        assert!(lp.exists(), "{}", lp.display());
    }
    let p = root.join("f.txt").to_str().unwrap().to_string();
    assert_eq!(d.index.keys().collect::<Vec<_>>(), [&p]);
    assert_eq!(d.index[&p].vv, Vv(vec![(d.me_short, 1)]));
    assert_eq!(d.outbox.len(), 1);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn remote_entries_through_a_symlinked_folder_are_skipped() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("entry-symlink");
    let root = dir.join("in");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(dir.join("out")).unwrap();
    std::os::unix::fs::symlink(dir.join("out"), root.join("link")).unwrap();
    d.cfg.set_root(&me, root.to_str().unwrap(), true, false);
    std::fs::write(dir.join("out/f.txt"), "x").unwrap();
    let mut gone = meta(0, 2);
    gone.hash = None;
    gone.vv = Vv(vec![(2, 1), (d.me_short, 1)]);
    d.on_entry("aa", Entry { path: root.join("link/f.txt").to_str().unwrap().into(), meta: gone });
    assert!(dir.join("out/f.txt").exists());
    assert!(d.index.is_empty());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn synced_folder_replaced_by_a_symlink_is_recorded_as_deleted() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("folder-to-symlink");
    let root = dir.join("in");
    std::fs::create_dir_all(root.join("d")).unwrap();
    std::fs::write(root.join("d/f.txt"), "x").unwrap();
    let rp = root.to_str().unwrap().to_string();
    d.cfg.set_root(&me, &rp, true, false);
    d.check_tree(&rp);
    std::fs::remove_dir_all(root.join("d")).unwrap();
    std::fs::create_dir_all(dir.join("out")).unwrap();
    std::fs::write(dir.join("out/f.txt"), "secret").unwrap();
    std::os::unix::fs::symlink(dir.join("out"), root.join("d")).unwrap();
    d.check_tree(&rp);
    assert!(d.index[&format!("{rp}/d/f.txt")].deleted());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn files_in_a_folder_that_replaced_a_synced_file_are_not_recorded() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("file-to-folder");
    std::fs::create_dir_all(dir.join("f")).unwrap();
    std::fs::write(dir.join("f/x.txt"), "x").unwrap();
    let fp = dir.join("f").to_str().unwrap().to_string();
    d.cfg.set_root(&me, &fp, false, false);
    d.check_tree(&fp);
    assert!(d.index.is_empty());
    assert!(d.outbox.is_empty());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_file_that_replaced_a_synced_folder_is_not_recorded() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("folder-to-file");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("f"), "x").unwrap();
    let fp = dir.join("f").to_str().unwrap().to_string();
    d.cfg.set_root(&me, &fp, true, false);
    d.check_tree(&fp);
    assert!(d.index.is_empty());
    assert!(d.outbox.is_empty());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_file_that_replaced_a_synced_folder_inside_another_synced_folder_is_not_recorded() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("nested-folder-to-file");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("f"), "x").unwrap();
    std::fs::write(dir.join("g"), "y").unwrap();
    let rp = dir.to_str().unwrap().to_string();
    d.cfg.set_root(&me, &rp, true, false);
    d.cfg.set_root(&me, &format!("{rp}/f"), true, false);
    d.check_tree(&rp);
    assert_eq!(d.index.keys().collect::<Vec<_>>(), [&format!("{rp}/g")]);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_changed_file_below_a_synced_file_path_is_not_recorded() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("changed-below-file-root");
    std::fs::create_dir_all(dir.join("s/f")).unwrap();
    std::fs::write(dir.join("s/f/x"), "new").unwrap();
    let rp = dir.join("s").to_str().unwrap().to_string();
    d.cfg.set_root(&me, &rp, true, false);
    d.cfg.set_root(&me, &format!("{rp}/f"), false, false);
    let path = format!("{rp}/f/x");
    let old = FileMeta { hash: Some([1; 32]), size: 9, ..meta(0, 0) };
    d.index.insert(path.clone(), old.clone());
    d.check_tree(&rp);
    assert_eq!(d.index[&path], old);
    assert!(d.outbox.iter().all(|e| e.path != path));
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn remote_deletion_below_a_synced_file_path_does_not_record_or_delete_an_untracked_file() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("deletion-below-file-root");
    std::fs::create_dir_all(dir.join("s/f")).unwrap();
    std::fs::write(dir.join("s/f/x"), "x").unwrap();
    let rp = dir.join("s").to_str().unwrap().to_string();
    d.cfg.set_root(&me, &rp, true, false);
    d.cfg.set_root(&me, &format!("{rp}/f"), false, false);
    let path = format!("{rp}/f/x");
    let mut gone = meta(0, 2);
    gone.hash = None;
    gone.mode = 0;
    gone.vv = Vv(vec![(2, 1)]);
    d.on_entry("aa", Entry { path: path.clone(), meta: gone });
    assert!(dir.join("s/f/x").exists());
    assert!(d.outbox.iter().all(|e| e.path != path || e.meta.deleted()));
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn files_in_a_folder_that_cannot_be_opened_are_not_recorded_as_deleted() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("folder-no-access");
    let root = dir.join("in");
    std::fs::create_dir_all(root.join("d")).unwrap();
    std::fs::write(root.join("d/f.txt"), "x").unwrap();
    let rp = root.to_str().unwrap().to_string();
    d.cfg.set_root(&me, &rp, true, false);
    d.check_tree(&rp);
    d.outbox.clear();
    std::fs::set_permissions(root.join("d"), std::fs::Permissions::from_mode(0o000)).unwrap();
    d.check_tree(&rp);
    std::fs::set_permissions(root.join("d"), std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(!d.index[&format!("{rp}/d/f.txt")].deleted());
    assert!(d.outbox.is_empty());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn remote_deletion_does_not_remove_a_local_symlink_that_was_never_synced() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("delete-symlink");
    let root = dir.join("s");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(dir.join("target.txt"), "x").unwrap();
    std::os::unix::fs::symlink(dir.join("target.txt"), root.join("f.txt")).unwrap();
    d.cfg.set_root(&me, root.to_str().unwrap(), true, false);
    let mut gone = meta(0, 2);
    gone.hash = None;
    gone.vv = Vv(vec![(2, 1)]);
    d.on_entry("aa", Entry { path: root.join("f.txt").to_str().unwrap().into(), meta: gone });
    assert!(std::fs::symlink_metadata(root.join("f.txt")).unwrap().is_symlink());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn scan_records_regular_files_but_not_symlinks_to_files_or_folders_outside() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("scan-symlinks");
    let root = dir.join("s");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(dir.join("out")).unwrap();
    std::fs::write(dir.join("out/secret.txt"), "secret").unwrap();
    std::fs::write(root.join("f.txt"), "x").unwrap();
    std::os::unix::fs::symlink(dir.join("out/secret.txt"), root.join("file-link")).unwrap();
    std::os::unix::fs::symlink(dir.join("out"), root.join("dir-link")).unwrap();
    let rp = root.to_str().unwrap().to_string();
    d.cfg.set_root(&me, &rp, true, false);
    d.check_tree(&rp);
    let recorded: Vec<_> = d.index.keys().cloned().collect();
    assert_eq!(recorded, [format!("{rp}/f.txt")]);
    let sent: Vec<_> = d.outbox.iter().map(|e| e.path.clone()).collect();
    assert_eq!(sent, [format!("{rp}/f.txt")]);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn remote_entry_for_a_folder_root_itself_is_skipped() {
    let mut d = daemon();
    let me = d.me.clone();
    d.cfg.set_node(&me, "aa", "h:1", false);
    up(&mut d, "aa", 1);
    let dir = test_dir("entry-root");
    std::fs::create_dir_all(&dir).unwrap();
    let root = dir.join("s");
    let p = root.to_str().unwrap().to_string();
    d.cfg.set_root(&me, &p, true, false);
    let mut r = meta(0, 2);
    r.hash = Some(*blake3::hash(b"abc").as_bytes());
    r.size = 3;
    r.vv = Vv(vec![(2, 1)]);
    d.on_entry("aa", Entry { path: p.clone(), meta: r });
    let msg = Msg::Data { path: p.clone(), hash: *blake3::hash(b"abc").as_bytes(), offset: 0, bytes: b"abc".to_vec(), eof: true };
    d.handle(Event::Peer { id: "aa".into(), conn: 1, msg });
    assert!(!root.is_file());
    assert!(!dir.join(".s.rsync-tmp").exists());
    assert!(d.index.is_empty());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn remote_deletion_of_a_synced_file_that_became_a_folder_removes_the_local_file() {
    let mut d = daemon();
    let me = d.me.clone();
    d.cfg.set_node(&me, "aa", "h:1", false);
    up(&mut d, "aa", 1);
    let dir = test_dir("file-became-folder");
    std::fs::create_dir_all(&dir).unwrap();
    let root = dir.join("s");
    std::fs::write(&root, "x").unwrap();
    let p = root.to_str().unwrap().to_string();
    d.cfg.set_root(&me, &p, false, false);
    d.check_tree(&p);
    d.cfg.set_root(&me, &p, true, false);
    let mut r = d.index[&p].clone();
    r.hash = None;
    r.vv = r.vv.bumped(2);
    d.on_entry("aa", Entry { path: p.clone(), meta: r });
    assert!(!root.exists());
    assert!(d.index[&p].deleted());
    let fp = format!("{p}/f");
    let mut f = meta(0, 2);
    f.vv = Vv(vec![(2, 1)]);
    d.on_entry("aa", Entry { path: fp.clone(), meta: f });
    assert!(d.downloads.contains_key(&fp));
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn remote_deletion_below_a_synced_folder_that_became_a_file_removes_the_local_file() {
    let mut d = daemon();
    let me = d.me.clone();
    d.cfg.set_node(&me, "aa", "h:1", false);
    up(&mut d, "aa", 1);
    let dir = test_dir("folder-became-file");
    let root = dir.join("s");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("f"), "x").unwrap();
    let p = root.to_str().unwrap().to_string();
    d.cfg.set_root(&me, &p, true, false);
    d.check_tree(&p);
    d.cfg.set_root(&me, &p, false, false);
    let fp = format!("{p}/f");
    let mut r = d.index[&fp].clone();
    r.hash = None;
    r.vv = r.vv.bumped(2);
    d.on_entry("aa", Entry { path: fp.clone(), meta: r });
    assert!(!root.join("f").exists());
    assert!(d.index[&fp].deleted());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn remote_deletion_of_the_last_file_in_a_folder_removes_the_empty_folders_but_not_the_root() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("deletion-empties-folders");
    let root = dir.join("s");
    std::fs::create_dir_all(root.join("a/b")).unwrap();
    std::fs::create_dir_all(root.join("c")).unwrap();
    std::fs::write(root.join("a/b/f"), "x").unwrap();
    std::fs::write(root.join("c/g"), "y").unwrap();
    let p = root.to_str().unwrap().to_string();
    d.cfg.set_root(&me, &p, true, false);
    d.check_tree(&p);
    for f in ["a/b/f", "c/g"] {
        let fp = format!("{p}/{f}");
        let mut r = d.index[&fp].clone();
        r.hash = None;
        r.vv = r.vv.bumped(2);
        d.on_entry("aa", Entry { path: fp, meta: r });
    }
    assert!(!root.join("a").exists());
    assert!(!root.join("c").exists());
    assert!(root.is_dir());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn download_dropped_after_a_remote_deletion_removes_the_folders_it_created() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("dropped-download-folders");
    let root = dir.join("s");
    std::fs::create_dir_all(&root).unwrap();
    let rp = root.to_str().unwrap().to_string();
    d.cfg.set_root(&me, &rp, true, false);
    d.cfg.set_node(&me, "aa", "h:1", false);
    let mut a = up(&mut d, "aa", 1);
    while a.try_recv().is_ok() {}
    let p = format!("{rp}/a/b/f");
    let hash = *blake3::hash(b"abc").as_bytes();
    let r = FileMeta { hash: Some(hash), size: 3, vv: Vv(vec![(1, 1)]), ..meta(0, 1) };
    d.on_entry("aa", Entry { path: p.clone(), meta: r.clone() });
    assert!(root.join("a/b").is_dir());
    d.on_entry("aa", Entry { path: p.clone(), meta: FileMeta { hash: None, size: 0, vv: Vv(vec![(1, 2)]), ..r } });
    let msg = Msg::Data { path: p, hash, offset: 0, bytes: b"abc".to_vec(), eof: true };
    d.handle(Event::Peer { id: "aa".into(), conn: 1, msg });
    assert!(!root.join("a").exists());
    assert!(root.is_dir());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn download_refused_with_no_other_source_removes_the_folders_it_created() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("refused-download-folders");
    let root = dir.join("s");
    std::fs::create_dir_all(&root).unwrap();
    let rp = root.to_str().unwrap().to_string();
    d.cfg.set_root(&me, &rp, true, false);
    d.cfg.set_node(&me, "aa", "h:1", false);
    let _a = up(&mut d, "aa", 1);
    let p = format!("{rp}/a/b/f");
    let r = FileMeta { vv: Vv(vec![(1, 1)]), ..meta(0, 1) };
    let hash = r.hash.unwrap();
    d.on_entry("aa", Entry { path: p.clone(), meta: r });
    assert!(root.join("a/b").is_dir());
    d.handle(Event::Peer { id: "aa".into(), conn: 1, msg: Msg::Unavailable { path: p, hash } });
    assert!(!root.join("a").exists());
    assert!(root.is_dir());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn remote_deletion_of_the_last_file_in_a_nested_synced_folder_keeps_that_folder() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("deletion-keeps-nested-root");
    let root = dir.join("s");
    std::fs::create_dir_all(root.join("n")).unwrap();
    std::fs::write(root.join("n/f"), "x").unwrap();
    let p = root.to_str().unwrap().to_string();
    let inner = format!("{p}/n");
    d.cfg.set_root(&me, &p, true, false);
    d.cfg.set_root(&me, &inner, true, false);
    d.check_tree(&p);
    let fp = format!("{inner}/f");
    let mut r = d.index[&fp].clone();
    r.hash = None;
    r.vv = r.vv.bumped(2);
    d.on_entry("aa", Entry { path: fp, meta: r });
    assert!(!root.join("n/f").exists());
    assert!(root.join("n").is_dir());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn remote_deletion_keeps_a_folder_that_still_has_files() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("deletion-keeps-folder");
    let root = dir.join("s");
    std::fs::create_dir_all(root.join("a")).unwrap();
    std::fs::write(root.join("a/f"), "x").unwrap();
    std::fs::write(root.join("a/other"), "y").unwrap();
    let p = root.to_str().unwrap().to_string();
    d.cfg.set_root(&me, &p, true, false);
    d.check_tree(&p);
    let fp = format!("{p}/a/f");
    let mut r = d.index[&fp].clone();
    r.hash = None;
    r.vv = r.vv.bumped(2);
    d.on_entry("aa", Entry { path: fp, meta: r });
    assert!(!root.join("a/f").exists());
    assert!(root.join("a/other").exists());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn downloaded_file_replaces_the_empty_folder_left_by_a_synced_folder_that_became_a_file() {
    let mut d = daemon();
    let me = d.me.clone();
    d.cfg.set_node(&me, "aa", "h:1", false);
    up(&mut d, "aa", 1);
    let dir = test_dir("folder-became-file-download");
    let root = dir.join("s");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("f"), "x").unwrap();
    let p = root.to_str().unwrap().to_string();
    d.cfg.set_root(&me, &p, true, false);
    d.check_tree(&p);
    d.cfg.set_root(&me, &p, false, false);
    let mut r = meta(0, 2);
    r.hash = Some(*blake3::hash(b"abc").as_bytes());
    r.size = 3;
    r.vv = Vv(vec![(2, 1)]);
    d.on_entry("aa", Entry { path: p.clone(), meta: r.clone() });
    let fp = format!("{p}/f");
    let mut del = d.index[&fp].clone();
    del.hash = None;
    del.vv = del.vv.bumped(2);
    d.on_entry("aa", Entry { path: fp, meta: del });
    let msg = Msg::Data { path: p.clone(), hash: *blake3::hash(b"abc").as_bytes(), offset: 0, bytes: b"abc".to_vec(), eof: true };
    d.handle(Event::Peer { id: "aa".into(), conn: 1, msg });
    assert_eq!(std::fs::read(&root).unwrap(), b"abc");
    assert_eq!(d.index[&p], r);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn remote_entry_below_a_file_root_is_skipped() {
    let mut d = daemon();
    let me = d.me.clone();
    d.cfg.set_node(&me, "aa", "h:1", false);
    up(&mut d, "aa", 1);
    let dir = test_dir("entry-below-file-root");
    std::fs::create_dir_all(&dir).unwrap();
    let root = dir.join("f");
    d.cfg.set_root(&me, root.to_str().unwrap(), false, false);
    let p = root.join("x").to_str().unwrap().to_string();
    let mut r = meta(0, 2);
    r.hash = Some(*blake3::hash(b"abc").as_bytes());
    r.size = 3;
    r.vv = Vv(vec![(2, 1)]);
    d.on_entry("aa", Entry { path: p.clone(), meta: r });
    let msg = Msg::Data { path: p, hash: *blake3::hash(b"abc").as_bytes(), offset: 0, bytes: b"abc".to_vec(), eof: true };
    d.handle(Event::Peer { id: "aa".into(), conn: 1, msg });
    assert!(!root.exists());
    assert!(d.index.is_empty());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn remote_file_is_written_without_setuid_setgid_or_sticky_bits() {
    let mut d = daemon();
    let me = d.me.clone();
    d.cfg.set_node(&me, "aa", "h:1", false);
    up(&mut d, "aa", 1);
    let dir = test_dir("entry-setuid");
    std::fs::create_dir_all(&dir).unwrap();
    d.cfg.set_root(&me, dir.to_str().unwrap(), true, false);
    let p = dir.join("f.sh").to_str().unwrap().to_string();
    let mut r = meta(0, 2);
    r.hash = Some(*blake3::hash(b"abc").as_bytes());
    r.size = 3;
    r.mode = 0o7755;
    r.vv = Vv(vec![(2, 1)]);
    d.on_entry("aa", Entry { path: p.clone(), meta: r });
    let msg = Msg::Data { path: p.clone(), hash: *blake3::hash(b"abc").as_bytes(), offset: 0, bytes: b"abc".to_vec(), eof: true };
    d.handle(Event::Peer { id: "aa".into(), conn: 1, msg });
    let mode = std::fs::metadata(&p).unwrap().permissions().mode();
    assert_eq!(mode & 0o7777, 0o755);
    d.outbox.clear();
    d.check_tree(&p);
    assert!(d.outbox.is_empty());
    std::fs::remove_dir_all(&dir).unwrap();
}
