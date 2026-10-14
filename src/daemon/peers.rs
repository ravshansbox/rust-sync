use super::batch::{batches, config_batches};
use super::conn::dial;
use super::{Daemon, Event, MAX_PENDING, Peer};
use crate::index::Entry;
use crate::log::log;
use crate::net::{self, Msg};
use std::time::Duration;

/// After a failed dial, retry after 1 s, then 2 s, 4 s... up to this.
pub(super) const MAX_BACKOFF: Duration = super::TICK;

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

    /// Whether a node that finished the handshake is trusted. Untrusted ones are remembered.
    pub(super) fn on_auth(&mut self, id: String, addr: Option<String>) -> bool {
        let ok = self.cfg.trusted(&id).is_some();
        if let (false, Some(addr)) = (ok, addr) {
            // Log each node once, so repeated attempts don't flood the log.
            if !self.pending.contains_key(&id) {
                log!("untrusted node {} at {addr} tried to connect", net::pretty_id(&id));
            }
            self.add_pending(id, addr);
        }
        ok
    }

    pub(super) fn is_current(&self, id: &str, conn: u64) -> bool {
        self.peers.get(id).is_some_and(|p| p.conn == conn)
    }

    pub(super) fn peer_down(&mut self, id: String, conn: u64) {
        if self.is_current(&id, conn) {
            self.peers.remove(&id);
            self.drop_downloads_from(&id);
            log!("disconnected from {}", net::pretty_id(&id));
            if !self.dialling.contains(&id) {
                self.redial_after(id, Duration::from_secs(1));
            }
        }
    }

    pub(super) fn dial_done(&mut self, id: String) {
        self.dialling.remove(&id);
        if !self.peers.contains_key(&id) {
            let wait = self.backoff.get(&id).map_or(Duration::from_secs(1), |d| (*d * 2).min(MAX_BACKOFF));
            self.redial_after(id, wait);
        }
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
