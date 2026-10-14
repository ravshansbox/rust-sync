use super::batch::config_batches;
use super::text::{nodes_text, status_text};
use super::Daemon;
use crate::config::Shared;
use crate::ctl::{self, Req, Resp};
use crate::index::{self, Entry};
use crate::log::log;
use crate::net::Msg;
use std::collections::HashSet;

impl Daemon {
    pub(super) fn active_roots(&self) -> HashSet<(String, bool)> {
        self.cfg.active_roots().map(|(r, e)| (r.clone(), e.is_dir)).collect()
    }

    pub(super) fn on_config(&mut self, shared: &Shared) {
        let before = self.active_roots();
        if self.cfg.merge(shared, &self.me) {
            self.config_changed(&before);
        }
    }

    pub(super) fn config_changed(&mut self, before: &HashSet<(String, bool)>) {
        if let Err(e) = self.cfg.save(&self.config_file) {
            log!("cannot save config: {e}");
        }
        let gone: Vec<String> = self.peers.extract_if(|id, _| self.cfg.trusted(id).is_none()).map(|(id, _)| id).collect();
        for id in gone {
            self.drop_downloads_from(&id);
        }
        let unwanted: Vec<String> = self
            .downloads
            .keys()
            .chain(self.queued.keys())
            .filter(|p| !self.is_synced(p))
            .cloned()
            .collect();
        for p in unwanted {
            self.cancel_download(&p);
            self.queued.remove(&p);
        }
        self.reconcile_watches();
        let after = self.active_roots();
        for (root, _) in after.difference(before) {
            log!("now syncing {root}");
            self.check_tree(root);
            // Peers may not have seen our entries for this root yet.
            self.outbox.extend(index::under(&self.index, root).map(Entry::from));
        }
        for (root, _) in before.difference(&after) {
            if self.cfg.active_root(root).is_none() && self.cfg.root_of(root).is_some() {
                self.check_tree(root);
                self.outbox.extend(index::under(&self.index, root).map(Entry::from));
            }
        }
        for shared in config_batches(&self.cfg.shared) {
            self.broadcast(&Msg::Config(shared));
        }
        self.dial_all();
    }

    pub(super) fn on_ctl(&mut self, req: &Req) -> Resp {
        let before = self.active_roots();
        match req {
            Req::Status => return Resp { ok: true, text: self.status() },
            Req::ListNodes => {
                let text = nodes_text(&self.cfg, |id| Some(self.peers.contains_key(id)), &self.pending);
                return Resp { ok: true, text };
            }
            Req::AddNode { id, .. } => {
                self.remove_pending(id);
                self.backoff.remove(id);
            }
            _ => {}
        }
        let text = match ctl::apply(&mut self.cfg, &self.me, req) {
            Ok(text) => text,
            Err(text) => return Resp { ok: false, text },
        };
        self.config_changed(&before);
        Resp { ok: true, text }
    }

    pub(super) fn status(&self) -> String {
        let s = status_text(&self.cfg, &self.me, |id| Some(self.peers.contains_key(id)), &self.pending);
        let files = self.index.iter().filter(|(p, m)| !m.deleted() && self.is_synced(p)).count();
        format!(
            "{s}\nFiles tracked: {files}\nTransfers: {} active, {} queued\n",
            self.downloads.len(),
            self.queued.len()
        )
    }
}
