//! Checking paths before adding or removing them.

use super::send;
use crate::ctl::Req;
use crate::paths;
use anyhow::{Context, bail};
use std::path::PathBuf;

pub(super) async fn path_add(path: PathBuf) -> anyhow::Result<()> {
    let abs = std::fs::canonicalize(&path).with_context(|| format!("cannot find {}", path.display()))?;
    let is_dir = abs.is_dir();
    if !is_dir && !abs.is_file() {
        bail!("{} is not a regular file or folder", abs.display());
    }
    let portable = paths::to_portable(&abs)
        .filter(|p| paths::to_local(p).is_some())
        .with_context(|| format!("cannot sync {}", abs.display()))?;
    send(Req::AddPath { path: portable, is_dir }).await
}

pub(super) fn removal_path(path: PathBuf) -> anyhow::Result<String> {
    // The path may be gone already, so do not require it to exist.
    let abs = std::fs::canonicalize(&path).unwrap_or_else(|_| paths::resolve_parent(path));
    paths::to_portable(&abs).context("path is not valid UTF-8")
}
