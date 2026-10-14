use super::Daemon;
use crate::index::{self, FileMeta, Vv};
use crate::log::log;
use crate::paths;
use notify::{RecursiveMode, Watcher};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

impl Daemon {
    pub(super) fn reconcile_watches(&mut self) {
        let mut want: HashMap<PathBuf, RecursiveMode> = HashMap::new();
        for (root, e) in self.cfg.active_roots() {
            let Some(lp) = paths::to_local(root) else { continue };
            if e.is_dir {
                let _ = std::fs::create_dir_all(&lp);
                want.insert(lp, RecursiveMode::Recursive);
            } else if let Some(parent) = lp.parent() {
                // Watch the parent folder, so we still see the file when an editor
                // replaces it with a new one.
                let _ = std::fs::create_dir_all(parent);
                want.entry(parent.to_path_buf()).or_insert(RecursiveMode::NonRecursive);
            }
        }
        for (p, _) in self.watched.extract_if(|p, m| want.get(p) != Some(m)) {
            let _ = self.watcher.unwatch(&p);
        }
        for (p, m) in want {
            if self.watched.contains_key(&p) {
                continue;
            }
            match self.watcher.watch(&p, m) {
                Ok(()) => drop(self.watched.insert(p, m)),
                Err(e) => log!("cannot watch {}: {e}", p.display()),
            }
        }
    }

    pub(super) fn rescan_all(&mut self) {
        for (r, _) in self.active_roots() {
            self.check_tree(&r);
        }
    }

    pub(super) fn on_fs(&mut self, lp: &Path) {
        if lp.file_name().and_then(|n| n.to_str()).is_some_and(paths::ignored) {
            return;
        }
        let Some(p) = paths::to_portable(lp) else { return };
        let Some(root) = self.local_root(&p) else { return };
        if !paths::through_symlink(lp, &root) {
            self.check_tree(&p);
        }
    }

    /// Bring the index up to date for `p` and everything under it.
    pub(super) fn check_tree(&mut self, p: &str) {
        let Some(lp) = paths::to_local(p) else { return };
        let mut seen = HashSet::new();
        match std::fs::symlink_metadata(&lp) {
            Ok(m) if m.is_dir() => self.walk(&lp, &mut seen),
            _ if !self.not_a_synced_file(p) => {
                self.check_file(p, &lp);
                seen.insert(p.to_string());
            }
            _ => {}
        }
        let missing: Vec<String> = index::under(&self.index, p)
            .filter(|(k, _)| !seen.contains(*k))
            .map(|(k, _)| k.clone())
            .collect();
        for k in missing {
            let Some(l) = paths::to_local(&k) else { continue };
            let root = self.local_root(&k);
            let st = index::stat(&l).filter(|_| !root.is_some_and(|r| paths::through_symlink(&l, &r)));
            if st.is_some() && self.not_a_synced_file(&k) {
                continue;
            }
            self.check_stat(&k, &l, st);
        }
    }

    pub(super) fn walk(&mut self, dir: &Path, seen: &mut HashSet<String>) {
        let Ok(rd) = std::fs::read_dir(dir) else { return };
        for e in rd.flatten() {
            let Ok(ft) = e.file_type() else { continue };
            if e.file_name().to_str().is_none_or(paths::ignored) {
                continue;
            }
            let path = e.path();
            if path.starts_with(paths::state_dir()) {
                continue;
            }
            if ft.is_dir() {
                self.walk(&path, seen);
            } else if ft.is_file()
                && let Some(p) = paths::to_portable(&path)
                && !self.not_a_synced_file(&p)
            {
                self.check_file(&p, &path);
                seen.insert(p);
            }
        }
    }

    pub(super) fn is_synced(&self, p: &str) -> bool {
        self.cfg.root_of(p).is_some() && !self.not_a_synced_file(p)
    }

    pub(super) fn not_a_synced_file(&self, p: &str) -> bool {
        self.cfg.active_root(p).is_some_and(|r| r.is_dir)
            || self.cfg.active_roots().any(|(r, re)| !re.is_dir && r != p && paths::is_under(p, r))
    }

    pub(super) fn local_root(&self, p: &str) -> Option<PathBuf> {
        self.cfg.root_of(p).and_then(paths::to_local)
    }

    /// Compare one file on disk with the index. Records and announces local changes.
    pub(super) fn check_file(&mut self, p: &str, lp: &Path) {
        self.check_stat(p, lp, index::stat(lp));
    }

    pub(super) fn check_stat(&mut self, p: &str, lp: &Path, st: Option<index::Stat>) {
        let old = self.index.get(p).cloned();
        let me = self.me_short;
        let bump = |old: &Option<FileMeta>| old.as_ref().map_or_else(Vv::default, |m| m.vv.clone()).bumped(me);
        let new = match (st, &old) {
            (None, None) => return,
            (None, Some(m)) if m.deleted() => return,
            (None, Some(_))
                if std::fs::symlink_metadata(lp)
                    .is_err_and(|e| !matches!(e.kind(), std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory)) =>
            {
                return;
            }
            (None, Some(_)) => FileMeta { hash: None, size: 0, mtime: index::now(), mode: 0, vv: bump(&old), by: me },
            (Some(s), Some(m)) if !m.deleted() && (m.size, m.mtime, m.mode) == (s.size, s.mtime, s.mode) => return,
            (Some(s), _) => {
                let Ok((hash, size)) = index::hash_file(lp) else { return };
                if let Some(m) = self.index.get_mut(p)
                    && m.hash == Some(hash)
                    && m.mode == s.mode
                {
                    // Only the timestamp changed. Remember it, but it is not a new version.
                    (m.size, m.mtime) = (size, s.mtime);
                    self.index_dirty = true;
                    return;
                }
                FileMeta { hash: Some(hash), size, mtime: s.mtime, mode: s.mode, vv: bump(&old), by: me }
            }
        };
        self.set_entry(p, new);
    }
}
