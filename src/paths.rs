//! Converting between local paths and the portable form shared with other nodes.
//!
//! Paths under the home folder are shared as `~/rest`, so they work on machines
//! with different usernames. Other paths are shared as absolute paths.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// The home folder from `HOME`, with symlinks resolved. Panics if `HOME` is not set.
pub fn home() -> &'static Path {
    static HOME: OnceLock<PathBuf> = OnceLock::new();
    HOME.get_or_init(|| {
        let h = PathBuf::from(std::env::var_os("HOME").expect("HOME is not set"));
        h.canonicalize().unwrap_or(h)
    })
}

/// Our state folder: `RUST_SYNC_HOME` if set, otherwise `~/.rust-sync`. Tests use a temporary folder.
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

/// Path of the config file in the state folder.
pub fn config_file() -> PathBuf {
    state_dir().join("config.json")
}

/// Path of the index file in the state folder.
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

/// True if `p` or a folder above it is the same file as `dir`, such as a path in a different letter case or through a symlink.
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

/// A temporary folder for a test, unique to this process.
#[cfg(test)]
pub fn test_dir(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("rust-sync-{name}-{}", std::process::id()))
}

#[cfg(test)]
mod tests;
