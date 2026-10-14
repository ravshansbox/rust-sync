//! Applying remote versions of files: installing, deleting, adopting and naming conflicts.

use super::Daemon;
use crate::index::{self, Action, Entry, FileMeta};
use crate::log::log;
use crate::paths;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

impl Daemon {
    pub(super) fn on_entry(&mut self, from: &str, mut e: Entry) {
        e.meta.mode &= 0o777;
        let Some(root) = self.local_root(&e.path) else { return };
        let Some(lp) = paths::to_local(&e.path) else { return };
        if !e.meta.deleted() && self.not_a_synced_file(&e.path) || paths::through_symlink(&lp, &root) {
            return;
        }
        if lp.file_name().and_then(|n| n.to_str()).is_none_or(paths::ignored) {
            return;
        }
        // Catch local changes the watcher has not reported yet, before deciding.
        if self.index.contains_key(&e.path) || !self.not_a_synced_file(&e.path) {
            self.check_file(&e.path, &lp);
        }
        self.apply(from, &e.path, &lp, e.meta, None);
    }

    /// Act on a remote version of `p`. `tmp` holds its content once downloaded.
    pub(super) fn apply(&mut self, from: &str, p: &str, lp: &Path, r: FileMeta, mut tmp: Option<PathBuf>) {
        let local = self.index.get(p).cloned();
        let action = index::decide(local.as_ref(), &r);
        let same_content = local.as_ref().is_some_and(|l| l.hash == r.hash);
        match action {
            // When our version wins a conflict we change nothing: the other side sees
            // the same conflict, picks the same winner, and keeps its copy.
            Action::Nothing | Action::LocalWins => {}
            Action::Same => self.adopt(p, lp, local, r),
            Action::Take | Action::Conflict if same_content => self.adopt(p, lp, local, r),
            Action::Take | Action::Conflict if r.deleted() => self.delete(p, lp, local.as_ref(), r),
            Action::Take | Action::Conflict => match tmp.take() {
                None => self.start_download(from, p, r),
                Some(t) => self.install(p, lp, &t, local.as_ref().filter(|_| action == Action::Conflict), r),
            },
        }
        if let Some(t) = tmp {
            let _ = std::fs::remove_file(t);
            self.remove_empty_parents(p, lp);
        }
    }

    fn delete(&mut self, p: &str, lp: &Path, local: Option<&FileMeta>, r: FileMeta) {
        if local.is_some_and(|l| !l.deleted()) {
            match std::fs::remove_file(lp) {
                Ok(()) => {
                    log!("deleted {p}");
                    self.remove_empty_parents(p, lp);
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return log!("cannot delete {p}: {e}"),
            }
        }
        self.set_entry(p, r);
    }

    fn install(&mut self, p: &str, lp: &Path, t: &Path, conflict: Option<&FileMeta>, mut r: FileMeta) {
        if std::fs::symlink_metadata(lp).is_ok_and(|m| m.is_dir()) {
            let _ = std::fs::remove_dir(lp);
        }
        let mut kept = None;
        if let Some(l) = conflict
            && !l.deleted()
            && lp.exists()
        {
            let copy = conflict_name(lp, l);
            if let Err(e) = std::fs::rename(lp, &copy) {
                let _ = std::fs::remove_file(t);
                return log!("cannot keep conflict copy of {p}: {e}");
            }
            kept = Some(copy);
        }
        if let Err(e) = std::fs::rename(t, lp) {
            let _ = std::fs::remove_file(t);
            if let Some(copy) = kept {
                let _ = std::fs::rename(copy, lp);
            }
            return log!("cannot write {p}: {e}");
        }
        if let Some(copy) = kept {
            log!("conflict: kept local version of {p} as {}", copy.display());
        }
        log!("updated {p}");
        if let Some(l) = conflict {
            r.vv = l.vv.merged(&r.vv);
        }
        self.set_entry(p, r);
    }

    pub(super) fn remove_empty_parents(&self, p: &str, lp: &Path) {
        if let Some(root) = self.local_root(p) {
            let _ = lp
                .ancestors()
                .skip(1)
                .take_while(|a| a.starts_with(&root) && paths::to_portable(a).is_none_or(|ap| self.cfg.active_root(&ap).is_none()))
                .try_for_each(std::fs::remove_dir);
        }
    }

    /// Same content on both sides: keep our file, take the combined version history.
    pub(super) fn adopt(&mut self, p: &str, lp: &Path, local: Option<FileMeta>, r: FileMeta) {
        let mut meta = r;
        if let Some(l) = local {
            meta.vv = l.vv.merged(&meta.vv);
            if !meta.deleted() {
                if l.mode != meta.mode {
                    let _ = std::fs::set_permissions(lp, std::fs::Permissions::from_mode(meta.mode));
                }
                (meta.size, meta.mtime) = (l.size, l.mtime);
            }
        }
        self.set_entry(p, meta);
    }
}

/// `notes.md` -> `notes.sync-conflict-<mtime>-<node>.md`, named after the losing
/// version, so every node that holds that version picks the same name.
pub(super) fn conflict_name(lp: &Path, loser: &FileMeta) -> PathBuf {
    let tag = format!("sync-conflict-{}-{:08x}", loser.mtime / 1_000_000_000, loser.by >> 32);
    let mut stem = lp.file_stem().unwrap_or_default().to_string_lossy().into_owned();
    let mut ext = lp.extension().map(|e| format!(".{}", e.to_string_lossy())).unwrap_or_default();
    if ext.len() + tag.len() + 1 > 255 {
        (stem, ext) = (lp.file_name().unwrap_or_default().to_string_lossy().into_owned(), String::new());
    }
    while stem.len() + tag.len() + 1 + ext.len() > 255 {
        stem.pop();
    }
    lp.with_file_name(format!("{stem}.{tag}{ext}"))
}

#[cfg(test)]
mod tests;
