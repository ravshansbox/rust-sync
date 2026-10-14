use super::batch::{batches, config_batches};
use super::tasks::dial;
use super::{Daemon, Event, MAX_PENDING, Peer};
use crate::index::Entry;
use crate::log::log;
use crate::net::{self, Msg};
use std::time::Duration;

impl Daemon {
    pub(super) fn peer_up(&mut self, id: String, peer: Peer) {
        if self.cfg.trusted(&id).is_none() {
            return;
        }
        if self.peers.contains_key(&id) {
            // Both sides dialled at once. Keep the connection dialled by the lower ID,
            // so both sides keep the same one.
            if peer.outbound != (self.me < id) {
                return;
            }
            self.drop_downloads_from(&id);
        }
        log!("connected to {}", net::pretty_id(&id));
        self.backoff.remove(&id);
        self.remove_pending(&id);
        for shared in config_batches(&self.cfg.shared) {
            let _ = peer.ctl.send(Msg::Config(shared));
        }
        let entries: Vec<Entry> = self
            .index
            .iter()
            .filter(|(p, _)| self.cfg.root_of(p).is_some())
            .map(Entry::from)
            .collect();
        for batch in batches(entries) {
            let _ = peer.ctl.send(Msg::Index(batch));
        }
        self.peers.insert(id, peer);
    }

    pub(super) fn add_pending(&mut self, id: String, addr: String) {
        self.pending_order.retain(|x| *x != id);
        self.pending_order.push_back(id.clone());
        self.pending.insert(id, addr);
        while self.pending_order.len() > MAX_PENDING
            && let Some(oldest) = self.pending_order.pop_front()
        {
            self.pending.remove(&oldest);
        }
    }

    pub(super) fn remove_pending(&mut self, id: &str) {
        self.pending.remove(id);
        self.pending_order.retain(|x| x != id);
    }

    /// Dial every trusted node we are not connected to, except those waiting to retry.
    pub(super) fn dial_all(&mut self) {
        let ids: Vec<String> =
            self.cfg.trusted_nodes().map(|(id, _)| id).filter(|id| !self.backoff.contains_key(*id)).cloned().collect();
        for id in ids {
            self.dial(&id);
        }
    }

    pub(super) fn dial(&mut self, id: &str) {
        let Some(n) = self.cfg.trusted(id) else {
            self.backoff.remove(id);
            return;
        };
        if self.peers.contains_key(id) || self.dialling.contains(id) {
            return;
        }
        // Only report the first failure in a row, so an offline node doesn't flood the log.
        let quiet = self.backoff.contains_key(id);
        self.dialling.insert(id.to_string());
        tokio::spawn(dial(n.addr.clone(), id.to_string(), quiet, self.keys.clone(), self.tx.clone(), self.cfg.port));
    }

    pub(super) fn redial_after(&mut self, id: String, wait: Duration) {
        self.backoff.insert(id.clone(), wait);
        let tx = self.tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(wait).await;
            let _ = tx.send(Event::Redial { id });
        });
    }
}
