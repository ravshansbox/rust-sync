use super::*;

#[test]
fn file_events_are_checked_only_inside_roots_and_skip_ignored_names() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("fs");
    let root = dir.join("in");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(dir.join("out")).unwrap();
    d.cfg.set_root(&me, root.to_str().unwrap(), true, false);
    for lp in [dir.join("out/f.txt"), root.join(".DS_Store"), root.join("f.txt")] {
        std::fs::write(&lp, "x").unwrap();
        d.on_fs(&lp);
    }
    let p = root.join("f.txt").to_str().unwrap().to_string();
    assert_eq!(d.index.keys().collect::<Vec<_>>(), [&p]);
    assert_eq!(d.index[&p].vv, Vv(vec![(d.me_short, 1)]));
    assert_eq!(d.outbox.len(), 1);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn file_events_through_a_symlinked_folder_are_ignored() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("fs-symlink");
    let root = dir.join("in");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(dir.join("out/sub")).unwrap();
    std::fs::write(dir.join("out/f.txt"), "secret").unwrap();
    std::fs::write(dir.join("out/sub/g.txt"), "secret").unwrap();
    std::os::unix::fs::symlink(dir.join("out"), root.join("link")).unwrap();
    d.cfg.set_root(&me, root.to_str().unwrap(), true, false);
    d.on_fs(&root.join("link/f.txt"));
    d.on_fs(&root.join("link/sub"));
    assert!(d.index.is_empty());
    assert!(d.outbox.is_empty());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn watches_follow_active_roots_with_files_watched_through_their_folder() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("watch");
    let (a, b, c) = (dir.join("a"), dir.join("b"), dir.join("c"));
    d.cfg.set_root(&me, a.to_str().unwrap(), true, false);
    d.cfg.set_root(&me, b.join("f.txt").to_str().unwrap(), false, false);
    d.cfg.set_root(&me, c.to_str().unwrap(), true, true);
    d.reconcile_watches();
    assert!(a.is_dir() && b.is_dir() && !c.exists());
    let want = HashMap::from([(a.clone(), RecursiveMode::Recursive), (b.clone(), RecursiveMode::NonRecursive)]);
    assert_eq!(d.watched, want);
    d.cfg.set_root(&me, a.to_str().unwrap(), true, true);
    d.cfg.set_root(&me, b.to_str().unwrap(), true, false);
    d.reconcile_watches();
    assert_eq!(d.watched, HashMap::from([(b, RecursiveMode::Recursive)]));
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn rescan_checks_only_active_roots() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("rescan");
    for (name, removed) in [("a", false), ("b", true)] {
        std::fs::create_dir_all(dir.join(name)).unwrap();
        std::fs::write(dir.join(name).join("f.txt"), "x").unwrap();
        d.cfg.set_root(&me, dir.join(name).to_str().unwrap(), true, removed);
    }
    d.handle(Event::Rescan);
    let keys: Vec<_> = d.index.keys().cloned().collect();
    assert_eq!(keys, [dir.join("a/f.txt").to_str().unwrap()]);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn tree_check_records_nested_files_skips_ignored_names_and_marks_missing_files_deleted() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("tree");
    std::fs::create_dir_all(dir.join("sub")).unwrap();
    let root = dir.to_str().unwrap().to_string();
    d.cfg.set_root(&me, &root, true, false);
    for name in ["sub/a.txt", ".DS_Store", "b.rsync-tmp"] {
        std::fs::write(dir.join(name), "x").unwrap();
    }
    let gone = dir.join("gone.txt").to_str().unwrap().to_string();
    d.index.insert(gone.clone(), meta(0, 2));
    d.check_tree(&root);
    let a = dir.join("sub/a.txt").to_str().unwrap().to_string();
    let mut keys: Vec<_> = d.index.keys().cloned().collect();
    keys.sort();
    assert_eq!(keys, [gone.clone(), a.clone()]);
    assert!(!d.index[&a].deleted());
    assert!(d.index[&gone].deleted());
    assert_eq!(d.outbox.len(), 2);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn tree_checks_and_file_events_skip_the_state_folder() {
    let mut d = daemon();
    let me = d.me.clone();
    let state = paths::state_dir();
    let dir = state.parent().unwrap();
    std::fs::create_dir_all(state).unwrap();
    std::fs::write(state.join("index.bin"), "x").unwrap();
    std::fs::write(dir.join("notes.txt"), "x").unwrap();
    let root = dir.to_str().unwrap().to_string();
    d.cfg.set_root(&me, &root, true, false);
    d.check_tree(&root);
    d.on_fs(&state.join("index.bin"));
    let notes = dir.join("notes.txt").to_str().unwrap().to_string();
    assert_eq!(d.index.keys().collect::<Vec<_>>(), [&notes]);
    std::fs::remove_file(dir.join("notes.txt")).unwrap();
    std::fs::remove_file(state.join("index.bin")).unwrap();
}

#[test]
fn unreadable_file_is_not_recorded() {
    let mut d = daemon();
    let me = d.me.clone();
    let dir = test_dir("unreadable");
    std::fs::create_dir_all(&dir).unwrap();
    let root = dir.to_str().unwrap().to_string();
    d.cfg.set_root(&me, &root, true, false);
    let f = dir.join("secret.txt");
    std::fs::write(&f, "x").unwrap();
    std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o000)).unwrap();
    d.check_tree(&root);
    assert!(d.index.is_empty());
    assert!(d.outbox.is_empty());
    std::fs::remove_dir_all(&dir).unwrap();
}
