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
        std::env::var_os("RUST_SYNC_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| home().join(".rust-sync"))
    })
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
