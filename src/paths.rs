//! Converting between local paths and the portable form shared with other nodes.
//!
//! Paths under the home folder are shared as `~/rest`, so they work on machines
//! with different usernames. Other paths are shared as absolute paths.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

pub fn home() -> &'static Path {
    static HOME: OnceLock<PathBuf> = OnceLock::new();
    HOME.get_or_init(|| {
        let h = PathBuf::from(std::env::var_os("HOME").expect("HOME is not set"));
        h.canonicalize().unwrap_or(h)
    })
}

pub fn state_dir() -> &'static Path {
    static DIR: OnceLock<PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        if cfg!(test) {
            return std::env::temp_dir().canonicalize().unwrap().join(format!("rust-sync-state-{}", std::process::id())).join(".rust-sync");
        }
        std::env::var_os("RUST_SYNC_HOME").map_or_else(|| home().join(".rust-sync"), |d| resolve_parent(d.into()))
    })
}

/// Resolve symlinks and `..` in the path and its parents, so `starts_with` checks
/// against canonical paths match. The path itself, and folders above it, may not exist.
pub fn resolve_parent(dir: PathBuf) -> PathBuf {
    let dir = std::path::absolute(&dir).unwrap_or(dir);
    for base in dir.ancestors() {
        let Ok(mut p) = base.canonicalize() else { continue };
        for part in dir.strip_prefix(base).unwrap_or(&dir).components() {
            if part == std::path::Component::ParentDir {
                p.pop();
            } else {
                p.push(part);
            }
        }
        return p;
    }
    dir
}

pub fn config_file() -> PathBuf {
    state_dir().join("config.json")
}

pub fn index_file() -> PathBuf {
    state_dir().join("index.bin")
}

/// Absolute local path -> portable string.
pub fn to_portable(p: &Path) -> Option<String> {
    if let Ok(rel) = p.strip_prefix(home()) {
        let s = rel.to_str()?;
        return Some(if s.is_empty() { "~".into() } else { format!("~/{s}") });
    }
    p.to_str().map(String::from)
}

/// Portable string -> absolute local path. Returns `None` for anything unsafe.
pub fn to_local(portable: &str) -> Option<PathBuf> {
    if portable.contains('\0') || portable.contains("//") || (portable.len() > 1 && portable.ends_with('/')) || portable.split('/').any(|c| c == ".." || c == ".") {
        return None;
    }
    let p = if portable == "~" {
        home().to_path_buf()
    } else if let Some(rest) = portable.strip_prefix("~/") {
        home().join(rest)
    } else if portable.starts_with('/') && !Path::new(portable).starts_with(home()) && !is_under_in_another_spelling(Path::new(portable), home()) {
        PathBuf::from(portable)
    } else {
        return None;
    };
    // Never touch our own state folder.
    if p.starts_with(state_dir()) || is_under_in_another_spelling(&p, state_dir()) {
        return None;
    }
    Some(p)
}

fn is_under_in_another_spelling(p: &Path, dir: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    let Ok(d) = std::fs::metadata(dir) else { return false };
    p.ancestors().any(|a| std::fs::metadata(a).is_ok_and(|a| (a.dev(), a.ino()) == (d.dev(), d.ino())))
}

/// True if `path` is `root` or inside it.
pub fn is_under(path: &str, root: &str) -> bool {
    match path.strip_prefix(root) {
        Some("") => true,
        Some(rest) => rest.starts_with('/') || root.ends_with('/'),
        None => false,
    }
}

/// True if a folder between `root` and `path` is a symbolic link.
pub fn through_symlink(path: &Path, root: &Path) -> bool {
    path.ancestors()
        .skip(1)
        .take_while(|a| *a != root && a.starts_with(root))
        .any(|a| std::fs::symlink_metadata(a).is_ok_and(|m| m.is_symlink()))
}

/// Write `bytes` to a temporary file next to `path`, then rename it into place.
pub fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(tmp, path)
}

/// Files we never sync: our temporary files and macOS clutter.
pub fn ignored(name: &str) -> bool {
    name.ends_with(".rsync-tmp") || name == ".DS_Store"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_folder_from_env_is_resolved_through_symlinks() {
        let tmp = std::env::temp_dir().join(format!("rust-sync-state-env-{}", std::process::id()));
        std::fs::create_dir_all(tmp.join("real")).unwrap();
        let _ = std::fs::remove_file(tmp.join("link"));
        std::os::unix::fs::symlink(tmp.join("real"), tmp.join("link")).unwrap();
        let want = tmp.canonicalize().unwrap().join("real/.rust-sync");
        assert_eq!(resolve_parent(tmp.join("link/.rust-sync")), want);
        assert_eq!(resolve_parent(tmp.join("link/../real/.rust-sync")), want);
        std::fs::remove_dir_all(&tmp).unwrap();
    }

    #[test]
    fn state_folder_from_env_that_is_a_symlink_is_resolved_to_its_target() {
        let tmp = std::env::temp_dir().join(format!("rust-sync-state-env-link-{}", std::process::id()));
        std::fs::create_dir_all(tmp.join("real")).unwrap();
        let _ = std::fs::remove_file(tmp.join("link"));
        std::os::unix::fs::symlink(tmp.join("real"), tmp.join("link")).unwrap();
        assert_eq!(resolve_parent(tmp.join("link")), tmp.canonicalize().unwrap().join("real"));
        std::fs::remove_dir_all(&tmp).unwrap();
    }

    #[test]
    fn state_folder_from_env_is_resolved_when_its_parent_folders_do_not_exist_yet() {
        let tmp = std::env::temp_dir().join(format!("rust-sync-state-env-new-{}", std::process::id()));
        std::fs::create_dir_all(tmp.join("real")).unwrap();
        let _ = std::fs::remove_file(tmp.join("link"));
        std::os::unix::fs::symlink(tmp.join("real"), tmp.join("link")).unwrap();
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
        let dir = std::env::temp_dir().join(format!("rust-sync-write-atomic-{}", std::process::id()));
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
        let tmp = std::env::temp_dir().canonicalize().unwrap().join(format!("rust-sync-to-local-link-{}", std::process::id()));
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
}
