use super::{Daemon, Download, MAX_DOWNLOADS};
use crate::index::{self, Action, Entry, FileMeta};
use crate::log::log;
use crate::net::Msg;
use crate::paths;
use std::ffi::OsStr;
use std::fs::File;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::{Duration, UNIX_EPOCH};

impl Daemon {
    pub(super) fn start_download(&mut self, from: &str, p: &str, r: FileMeta) {
        if let Some(d) = self.downloads.get_mut(p) {
            if !matches!(index::decide(Some(&d.meta), &r), Action::Take | Action::Conflict) {
                add_alt(&mut d.alts, &d.from, &d.meta, from, &r);
                return;
            }
            self.cancel_download(p);
        }
        if self.downloads.len() >= MAX_DOWNLOADS {
            if let Some((qfrom, q, alts)) = self.queued.get_mut(p) {
                match index::decide(Some(q), &r) {
                    Action::Nothing | Action::Same => {
                        add_alt(alts, qfrom, q, from, &r);
                        return;
                    }
                    Action::LocalWins => return,
                    _ => {}
                }
            }
            self.queued.insert(p.to_string(), (from.to_string(), r, Vec::new()));
            return;
        }
        let Some(peer) = self.peers.get(from) else { return };
        let Some(lp) = paths::to_local(p) else { return };
        let (Some(parent), Some(name)) = (lp.parent(), lp.file_name()) else { return };
        let tmp = parent.join(tmp_name(name));
        let file = match std::fs::create_dir_all(parent).and_then(|()| File::create(&tmp)) {
            Ok(f) => f,
            Err(e) => return log!("cannot create {}: {e}", tmp.display()),
        };
        let _ = peer.ctl.send(Msg::Request { path: p.to_string(), hash: r.hash.unwrap() });
        self.downloads.insert(p.to_string(), Download::new(from, r, tmp, file));
    }

    pub(super) fn pump_queue(&mut self) {
        while self.downloads.len() < MAX_DOWNLOADS {
            let Some((p, (from, r, alts))) = self.queued.pop_first() else { break };
            if !self.peers.contains_key(&from) {
                continue;
            }
            let hash = r.hash;
            // Decide again: things may have changed while it was queued.
            self.on_entry(&from, Entry { path: p.clone(), meta: r });
            if let Some(d) = self.downloads.get_mut(&p)
                && d.from == from
                && d.meta.hash == hash
            {
                d.alts = alts;
            }
        }
    }

    pub(super) fn on_data(&mut self, from: &str, path: &str, hash: [u8; 32], offset: u64, bytes: &[u8], eof: bool) {
        let Some(d) = self.downloads.get_mut(path) else { return };
        if d.from != from || d.meta.hash != Some(hash) {
            return;
        }
        if offset != d.written || d.written + bytes.len() as u64 > d.meta.size {
            let d = self.downloads.remove(path).unwrap();
            return self.retry_elsewhere(path.to_string(), d, from);
        }
        if d.file.write_all(bytes).is_err() {
            return self.cancel_download(path);
        }
        d.hasher.update(bytes);
        d.written += bytes.len() as u64;
        if !eof {
            return;
        }
        let d = self.downloads.remove(path).unwrap();
        self.finish_download(from, path, d);
    }

    fn finish_download(&mut self, from: &str, path: &str, d: Download) {
        if Some(*d.hasher.finalize().as_bytes()) != d.meta.hash {
            log!("{path} changed while being sent; will retry");
            return self.retry_elsewhere(path.to_string(), d, from);
        }
        let when = UNIX_EPOCH + Duration::from_nanos(d.meta.mtime.max(0).cast_unsigned());
        let _ = d.file.set_modified(when);
        let _ = d.file.set_permissions(std::fs::Permissions::from_mode(d.meta.mode));
        drop(d.file);
        let Some(lp) = paths::to_local(path) else { return };
        if self.local_root(path).is_some_and(|r| paths::through_symlink(&lp, &r)) {
            let _ = std::fs::remove_file(&d.tmp);
            return;
        }
        self.check_file(path, &lp);
        self.apply(from, path, &lp, d.meta, Some(d.tmp));
    }

    pub(super) fn on_unavailable(&mut self, from: &str, path: String, hash: [u8; 32]) {
        if self.downloads.get(&path).is_some_and(|d| d.from == from && d.meta.hash == Some(hash)) {
            let d = self.downloads.remove(&path).unwrap();
            self.retry_elsewhere(path, d, from);
        }
    }

    pub(super) fn cancel_download(&mut self, p: &str) {
        if let Some(d) = self.downloads.remove(p) {
            self.discard_tmp(p, &d.tmp);
        }
    }

    pub(super) fn cancel_all_downloads(&mut self) {
        let paths: Vec<String> = self.downloads.keys().cloned().collect();
        paths.iter().for_each(|p| self.cancel_download(p));
    }

    pub(super) fn drop_downloads_from(&mut self, id: &str) {
        let peers = &self.peers;
        let fallback = |alts: &mut Vec<String>| {
            alts.retain(|a| a != id && peers.contains_key(a));
            (!alts.is_empty()).then(|| alts.remove(0))
        };
        self.queued.retain(|_, (from, _, alts)| {
            if from != id {
                return true;
            }
            fallback(alts).map(|alt| *from = alt).is_some()
        });
        let gone: Vec<_> = self.downloads.extract_if(|_, d| d.from == id).collect();
        for (p, d) in gone {
            self.retry_elsewhere(p, d, id);
        }
    }

    fn retry_elsewhere(&mut self, p: String, mut d: Download, gone: &str) {
        self.discard_tmp(&p, &d.tmp);
        d.alts.retain(|a| a != gone && self.peers.contains_key(a));
        if !d.alts.is_empty() {
            let alt = d.alts.remove(0);
            self.queued.entry(p).or_insert((alt, d.meta, d.alts));
        }
    }

    fn discard_tmp(&self, p: &str, tmp: &Path) {
        let _ = std::fs::remove_file(tmp);
        self.remove_empty_parents(p, tmp);
    }
}

fn add_alt(alts: &mut Vec<String>, holder: &str, held: &FileMeta, from: &str, r: &FileMeta) {
    if holder != from && held.hash == r.hash && !alts.iter().any(|a| a == from) {
        alts.push(from.to_string());
    }
}

fn tmp_name(name: &OsStr) -> String {
    let mut tmp = format!(".{}.rsync-tmp", name.to_string_lossy());
    if tmp.len() > 255 {
        tmp = format!(".{}.rsync-tmp", blake3::hash(name.as_encoded_bytes()).to_hex());
    }
    tmp
}
