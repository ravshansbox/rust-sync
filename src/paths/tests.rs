use super::*;

fn real_and_link(name: &str) -> PathBuf {
    let tmp = test_dir(name);
    std::fs::create_dir_all(tmp.join("real")).unwrap();
    let _ = std::fs::remove_file(tmp.join("link"));
    std::os::unix::fs::symlink(tmp.join("real"), tmp.join("link")).unwrap();
    tmp
}

#[test]
fn state_folder_from_env_is_resolved_through_symlinks() {
    let tmp = real_and_link("state-env");
    let want = tmp.canonicalize().unwrap().join("real/.rust-sync");
    assert_eq!(resolve_parent(tmp.join("link/.rust-sync")), want);
    assert_eq!(resolve_parent(tmp.join("link/../real/.rust-sync")), want);
    std::fs::remove_dir_all(&tmp).unwrap();
}

#[test]
fn state_folder_from_env_that_is_a_symlink_is_resolved_to_its_target() {
    let tmp = real_and_link("state-env-link");
    assert_eq!(resolve_parent(tmp.join("link")), tmp.canonicalize().unwrap().join("real"));
    std::fs::remove_dir_all(&tmp).unwrap();
}

#[test]
fn state_folder_from_env_is_resolved_when_its_parent_folders_do_not_exist_yet() {
    let tmp = real_and_link("state-env-new");
    let want = tmp.canonicalize().unwrap().join("real/new/dir/.rust-sync");
    assert_eq!(resolve_parent(tmp.join("link/new/dir/.rust-sync")), want);
    assert_eq!(resolve_parent(tmp.join("link/new/gone/../dir/.rust-sync")), want);
    assert_eq!(resolve_parent(tmp.join("link/gone/../../real/new/dir/.rust-sync")), want);
    std::fs::remove_dir_all(&tmp).unwrap();
}

#[test]
fn is_under_matches_root_and_children_only() {
    assert!(is_under("/a/b", "/a/b"));
    assert!(is_under("/a/b/c", "/a/b"));
    assert!(is_under("/a/b/c", "/a/b/"));
    assert!(!is_under("/a/bc", "/a/b"));
    assert!(!is_under("/a", "/a/b"));
}

#[test]
fn write_atomic_replaces_contents_and_leaves_no_temp_file() {
    let dir = test_dir("write-atomic");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("state.json");
    std::fs::write(&path, "old").unwrap();
    write_atomic(&path, b"new").unwrap();
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "new");
    assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn ignored_skips_temp_files_and_ds_store() {
    assert!(ignored("x.rsync-tmp"));
    assert!(ignored(".DS_Store"));
    assert!(!ignored("notes.txt"));
    assert!(!ignored("sub/.DS_Store.bak"));
    assert!(!ignored("notes.DS_Store"));
}

#[test]
fn to_local_rejects_unsafe_paths() {
    assert_eq!(to_local("~/a/../b"), None);
    assert_eq!(to_local("~/./a"), None);
    assert_eq!(to_local("/a\0b"), None);
    assert_eq!(to_local("relative/path"), None);
    assert_eq!(to_local("~other/x"), None);
    assert_eq!(to_local("~//etc/x"), None);
    assert_eq!(to_local("~/a/"), None);
    assert_eq!(to_local("~/"), None);
    assert_eq!(to_local("/a/"), None);
}

#[test]
fn to_local_accepts_names_made_of_dots() {
    assert_eq!(to_local("~/a..b/.c/..."), Some(home().join("a..b/.c/...")));
}

#[test]
fn to_local_rejects_state_folder() {
    assert_eq!(to_local(state_dir().to_str().unwrap()), None);
    assert_eq!(to_local(state_dir().join("x").to_str().unwrap()), None);
}

#[test]
fn to_local_rejects_state_folder_written_in_another_case() {
    std::fs::create_dir_all(state_dir()).unwrap();
    let other = state_dir().with_file_name(".RUST-SYNC");
    assert_eq!(to_local(other.to_str().unwrap()), None);
    assert_eq!(to_local(other.join("config.json").to_str().unwrap()), None);
}

#[test]
fn home_paths_round_trip_through_tilde() {
    assert_eq!(to_portable(home()).as_deref(), Some("~"));
    assert_eq!(to_portable(&home().join("a/b")).as_deref(), Some("~/a/b"));
    assert_eq!(to_local("~"), Some(home().to_path_buf()));
    assert_eq!(to_local("~/a/b"), Some(home().join("a/b")));
}

#[test]
fn to_local_rejects_absolute_spelling_of_a_home_path() {
    assert_eq!(to_local(home().join("a/b").to_str().unwrap()), None);
    assert_eq!(to_local(home().to_str().unwrap()), None);
}

#[test]
fn to_local_rejects_absolute_spelling_of_a_home_path_in_another_case() {
    let upper = home().to_str().unwrap().to_uppercase();
    assert_eq!(to_local(&format!("{upper}/a/b")), None);
    assert_eq!(to_local(&upper), None);
}

#[test]
fn to_local_rejects_absolute_path_that_reaches_home_or_state_folder_at_another_depth() {
    let tmp = resolve_parent(test_dir("to-local-link"));
    std::fs::create_dir_all(tmp.join("a/b")).unwrap();
    std::fs::create_dir_all(state_dir()).unwrap();
    let _ = std::fs::remove_file(tmp.join("a/b/home"));
    let _ = std::fs::remove_file(tmp.join("a/b/state"));
    std::os::unix::fs::symlink(home(), tmp.join("a/b/home")).unwrap();
    std::os::unix::fs::symlink(state_dir(), tmp.join("a/b/state")).unwrap();
    assert_eq!(to_local(tmp.join("a/b/home/x").to_str().unwrap()), None);
    assert_eq!(to_local(tmp.join("a/b/state/config.json").to_str().unwrap()), None);
    assert_eq!(to_local(tmp.join("a/x").to_str().unwrap()), Some(tmp.join("a/x")));
    std::fs::remove_dir_all(&tmp).unwrap();
}

#[test]
fn absolute_paths_outside_home_stay_absolute() {
    assert_eq!(to_portable(Path::new("/nonexistent-root/x")).as_deref(), Some("/nonexistent-root/x"));
    assert_eq!(to_local("/nonexistent-root/x"), Some(PathBuf::from("/nonexistent-root/x")));
}

#[test]
fn to_portable_rejects_non_utf8_paths() {
    use std::os::unix::ffi::OsStrExt;
    let name = std::ffi::OsStr::from_bytes(b"bad\xff");
    assert_eq!(to_portable(&home().join(name)), None);
    assert_eq!(to_portable(&Path::new("/nonexistent-root").join(name)), None);
}
