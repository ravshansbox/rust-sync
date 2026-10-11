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
        std::env::var_os("RUST_SYNC_HOME").map_or_else(|| home().join(".rust-sync"), PathBuf::from)
    })
}

pub fn config_file() -> PathBuf {
    state_dir().join("config.json")
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
    if portable.contains('\0') || portable.split('/').any(|c| c == ".." || c == ".") {
        return None;
    }
    let p = if portable == "~" {
        home().to_path_buf()
    } else if let Some(rest) = portable.strip_prefix("~/") {
        home().join(rest)
    } else if portable.starts_with('/') {
        PathBuf::from(portable)
    } else {
        return None;
    };
    // Never touch our own state folder.
    if p.starts_with(state_dir()) {
        return None;
    }
    Some(p)
}

/// True if `path` is `root` or inside it.
pub fn is_under(path: &str, root: &str) -> bool {
    match path.strip_prefix(root) {
        Some("") => true,
        Some(rest) => rest.starts_with('/') || root.ends_with('/'),
        None => false,
    }
}

/// Files we never sync: our temporary files and macOS clutter.
pub fn ignored(name: &str) -> bool {
    name.ends_with(".rsync-tmp") || name == ".DS_Store"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_under_matches_root_and_children_only() {
        assert!(is_under("/a/b", "/a/b"));
        assert!(is_under("/a/b/c", "/a/b"));
        assert!(is_under("/a/b/c", "/a/b/"));
        assert!(!is_under("/a/bc", "/a/b"));
        assert!(!is_under("/a", "/a/b"));
    }

    #[test]
    fn ignored_skips_temp_files_and_ds_store() {
        assert!(ignored("x.rsync-tmp"));
        assert!(ignored(".DS_Store"));
        assert!(!ignored("notes.txt"));
        assert!(!ignored("sub/.DS_Store.bak"));
    }

    #[test]
    fn to_local_rejects_unsafe_paths() {
        assert_eq!(to_local("~/a/../b"), None);
        assert_eq!(to_local("~/./a"), None);
        assert_eq!(to_local("/a\0b"), None);
        assert_eq!(to_local("relative/path"), None);
    }

    #[test]
    fn to_local_accepts_names_made_of_dots() {
        assert_eq!(to_local("~/a..b/.c/..."), Some(home().join("a..b/.c/...")));
    }

    #[test]
    fn to_local_rejects_another_users_home() {
        assert_eq!(to_local("~other/x"), None);
    }

    #[test]
    fn to_local_rejects_state_folder() {
        assert_eq!(to_local(state_dir().to_str().unwrap()), None);
        assert_eq!(to_local(state_dir().join("x").to_str().unwrap()), None);
    }

    #[test]
    fn home_paths_round_trip_through_tilde() {
        assert_eq!(to_portable(home()).as_deref(), Some("~"));
        assert_eq!(to_portable(&home().join("a/b")).as_deref(), Some("~/a/b"));
        assert_eq!(to_local("~"), Some(home().to_path_buf()));
        assert_eq!(to_local("~/a/b"), Some(home().join("a/b")));
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
