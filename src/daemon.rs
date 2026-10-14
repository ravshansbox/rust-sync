//! The background process. All state lives in one `Daemon` value, driven by one
//! event loop on a single thread. Other tasks (network, watcher, control socket)
//! only pass events to it.

use crate::config::{self, Config, Shared};
use crate::ctl::{self, Req, Resp};
use crate::index::{self, Action, Entry, FileMeta, Index, Vv};
use crate::net::{self, Keys, Msg};
use crate::log::{self, log};
use crate::paths;
use anyhow::{Context, bail};
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs::File;
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream, UnixListener};
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::time::timeout;

mod batch;
mod text;

use batch::{batches, config_batches};
pub use text::{nodes_text, paths_text, status_text};

/// Wait this long after the last change before reading a file.
const SETTLE: Duration = Duration::from_millis(500);
/// ...but never longer than this, for files that change all the time.
const SETTLE_MAX: Duration = Duration::from_secs(3);
const TICK: Duration = Duration::from_secs(30);
/// Full rescan every this many ticks (15 minutes), in case the watcher missed something.
const RESCAN_TICKS: u64 = 30;
/// Drop a connection that has sent nothing for this long. Peers send a ping every tick.
const IDLE_LIMIT: Duration = Duration::from_secs(95);
const MAX_DOWNLOADS: usize = 8;
const MAX_UPLOADS: usize = 8;
/// Untrusted nodes remembered for `node list`. Older ones are forgotten, so repeated
/// connection attempts can't grow the list without end.
const MAX_PENDING: usize = 20;
/// Happy Eyeballs (RFC 8305): start the next address after this long, without
/// waiting for the earlier attempts to fail.
const ATTEMPT_DELAY: Duration = Duration::from_millis(250);
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);
/// After a failed dial, retry after 1 s, then 2 s, 4 s... up to this.
const MAX_BACKOFF: Duration = TICK;

static NEXT_CONN: AtomicU64 = AtomicU64::new(1);

enum Event {
    Fs(Vec<PathBuf>),
    Rescan,
    Tick,
    /// A peer finished the handshake. Reply with whether we trust it.
    Auth { id: String, addr: Option<String>, reply: oneshot::Sender<bool> },
    Up { id: String, conn: u64, outbound: bool, ctl: mpsc::UnboundedSender<Msg>, data: mpsc::Sender<Msg> },
    Down { id: String, conn: u64 },
    DialDone { id: String },
    Redial { id: String },
    Peer { id: String, conn: u64, msg: Msg },
    Ctl(Req, oneshot::Sender<Resp>),
    Shutdown,
}

struct Peer {
    conn: u64,
    outbound: bool,
    /// Small messages: config, index, requests.
    ctl: mpsc::UnboundedSender<Msg>,
    /// File data. Bounded, so a slow peer slows the upload down instead of filling RAM.
    data: mpsc::Sender<Msg>,
}

struct Download {
    from: String,
    meta: FileMeta,
    tmp: PathBuf,
    file: File,
    hasher: blake3::Hasher,
    written: u64,
    /// Other nodes that offered the same content, to fall back on if `from` goes down.
    alts: Vec<String>,
}

struct Daemon {
    me: String,
    me_short: u64,
    keys: Arc<Keys>,
    cfg: Config,
    config_file: PathBuf,
    index: Index,
    index_file: PathBuf,
    peers: HashMap<String, Peer>,
    dialling: HashSet<String>,
    /// Nodes we failed to reach, and how long we wait before the next try.
    backoff: HashMap<String, Duration>,
    /// Untrusted nodes that tried to connect: ID -> address.
    pending: BTreeMap<String, String>,
    /// IDs in `pending`, oldest first.
    pending_order: VecDeque<String>,
    downloads: HashMap<String, Download>,
    queued: BTreeMap<String, (String, FileMeta, Vec<String>)>,
    uploads: Arc<Semaphore>,
    watcher: RecommendedWatcher,
    watched: HashMap<PathBuf, RecursiveMode>,
    tx: mpsc::UnboundedSender<Event>,
    outbox: Vec<Entry>,
    index_dirty: bool,
    ticks: u64,
}

/// Run the daemon. With `log_file`, log lines go there (rotated) instead of stderr.
pub async fn run(port: Option<u16>, log_file: Option<PathBuf>) -> anyhow::Result<()> {
    if let Some(path) = log_file {
        log::to_file(path.clone()).with_context(|| format!("cannot open log file {}", path.display()))?;
    }
    // Under launchd nobody sees stderr, so panics go to the log too.
    std::panic::set_hook(Box::new(|info| log!("panic: {info}")));
    let res = run_inner(port).await;
    if let Err(e) = &res {
        log!("error: {e:#}");
    }
    res
}

async fn run_inner(port: Option<u16>) -> anyhow::Result<()> {
    let dir = paths::state_dir();
    std::fs::create_dir_all(dir)?;
    if ctl::call(&Req::Status).await?.is_some() {
        bail!("a daemon is already running for {}", dir.display());
    }
    let keys = Arc::new(net::load_or_create_keys(dir)?);
    let config_file = paths::config_file();
    let index_file = paths::index_file();
    let mut cfg = Config::load(&config_file)?;
    if let Some(p) = port {
        cfg.port = p;
    }
    cfg.save(&config_file)?;
    let (tx, mut rx) = mpsc::unbounded_channel();

    // File watcher. It runs on its own thread and hands raw paths to `debounce`.
    let (fs_tx, fs_rx) = mpsc::unbounded_channel::<Option<PathBuf>>();
    let watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| match res {
        Ok(ev) if !ev.need_rescan() => ev.paths.into_iter().for_each(|p| drop(fs_tx.send(Some(p)))),
        _ => drop(fs_tx.send(None)),
    })?;
    tokio::spawn(debounce(fs_rx, tx.clone()));

    // "::" also accepts IPv4 on macOS (net.inet6.ip6.v6only is 0 by default).
    let listener = match TcpListener::bind(("::", cfg.port)).await {
        Ok(l) => l,
        Err(_) => TcpListener::bind(("0.0.0.0", cfg.port))
            .await
            .with_context(|| format!("cannot listen on port {}", cfg.port))?,
    };
    tokio::spawn(accept_loop(listener, keys.clone(), tx.clone(), cfg.port));

    let sock = ctl::socket_path()?;
    let _ = std::fs::remove_file(&sock);
    let ctl_listener = UnixListener::bind(&sock)?;
    std::fs::set_permissions(&sock, std::fs::Permissions::from_mode(0o600))?;
    tokio::spawn(ctl_loop(ctl_listener, tx.clone()));

    let t = tx.clone();
    tokio::spawn(async move {
        let mut iv = tokio::time::interval(TICK);
        iv.tick().await;
        while t.send(Event::Tick).is_ok() {
            iv.tick().await;
        }
    });
    let t = tx.clone();
    tokio::spawn(async move {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).expect("signal handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
        let _ = t.send(Event::Shutdown);
    });

    let me = keys.id();
    let mut d = Daemon {
        me_short: net::short_id(&me),
        me,
        keys,
        cfg,
        config_file,
        index: index::load(&index_file),
        index_file,
        peers: HashMap::new(),
        dialling: HashSet::new(),
        backoff: HashMap::new(),
        pending: BTreeMap::new(),
        pending_order: VecDeque::new(),
        downloads: HashMap::new(),
        queued: BTreeMap::new(),
        uploads: Arc::new(Semaphore::new(MAX_UPLOADS)),
        watcher,
        watched: HashMap::new(),
        tx,
        outbox: Vec::new(),
        index_dirty: false,
        ticks: 0,
    };
    log!("rust-sync node {}", net::pretty_id(&d.me));
    log!("listening on port {}", d.cfg.port);
    d.reconcile_watches();
    d.rescan_all();
    d.outbox.clear();
    d.dial_all();

    while let Some(ev) = rx.recv().await {
        let stop = matches!(ev, Event::Shutdown);
        d.handle(ev);
        if stop {
            break;
        }
        d.pump_queue();
        d.flush();
    }

    log!("stopping");
    d.save_index();
    let _ = std::fs::remove_file(sock);
    Ok(())
}

impl Daemon {
    fn handle(&mut self, ev: Event) {
        match ev {
            Event::Fs(paths) => paths.iter().for_each(|p| self.on_fs(p)),
            Event::Rescan => self.rescan_all(),
            Event::Tick => {
                self.ticks += 1;
                self.broadcast(&Msg::Ping);
                self.dial_all();
                if self.ticks.is_multiple_of(RESCAN_TICKS) {
                    self.rescan_all();
                }
                self.save_index();
            }
            Event::Auth { id, addr, reply } => {
                let ok = self.cfg.trusted(&id).is_some();
                if let (false, Some(addr)) = (ok, addr) {
                    // Log each node once, so repeated attempts don't flood the log.
                    if !self.pending.contains_key(&id) {
                        log!("untrusted node {} at {addr} tried to connect", net::pretty_id(&id));
                    }
                    self.add_pending(id, addr);
                }
                let _ = reply.send(ok);
            }
            Event::Up { id, conn, outbound, ctl, data } => self.peer_up(id, Peer { conn, outbound, ctl, data }),
            Event::Down { id, conn } => {
                if self.peers.get(&id).is_some_and(|p| p.conn == conn) {
                    self.peers.remove(&id);
                    self.drop_downloads_from(&id);
                    log!("disconnected from {}", net::pretty_id(&id));
                    if !self.dialling.contains(&id) {
                        self.redial_after(id, Duration::from_secs(1));
                    }
                }
            }
            Event::DialDone { id } => {
                self.dialling.remove(&id);
                if !self.peers.contains_key(&id) {
                    let wait = self.backoff.get(&id).map_or(Duration::from_secs(1), |d| (*d * 2).min(MAX_BACKOFF));
                    self.redial_after(id, wait);
                }
            }
            Event::Redial { id } => self.dial(&id),
            Event::Peer { id, conn, msg } => {
                if self.peers.get(&id).is_some_and(|p| p.conn == conn) {
                    self.on_msg(&id, msg);
                }
            }
            Event::Ctl(req, reply) => {
                let resp = self.on_ctl(&req);
                let _ = reply.send(resp);
            }
            Event::Shutdown => {
                let paths: Vec<String> = self.downloads.keys().cloned().collect();
                paths.iter().for_each(|p| self.cancel_download(p));
            }
        }
    }

    // ---- Peers ----

    fn peer_up(&mut self, id: String, peer: Peer) {
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

    fn add_pending(&mut self, id: String, addr: String) {
        self.pending_order.retain(|x| *x != id);
        self.pending_order.push_back(id.clone());
        self.pending.insert(id, addr);
        while self.pending_order.len() > MAX_PENDING
            && let Some(oldest) = self.pending_order.pop_front()
        {
            self.pending.remove(&oldest);
        }
    }

    fn remove_pending(&mut self, id: &str) {
        self.pending.remove(id);
        self.pending_order.retain(|x| x != id);
    }

    /// Dial every trusted node we are not connected to, except those waiting to retry.
    fn dial_all(&mut self) {
        let ids: Vec<String> =
            self.cfg.trusted_nodes().map(|(id, _)| id).filter(|id| !self.backoff.contains_key(*id)).cloned().collect();
        for id in ids {
            self.dial(&id);
        }
    }

    fn dial(&mut self, id: &str) {
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

    fn redial_after(&mut self, id: String, wait: Duration) {
        self.backoff.insert(id.clone(), wait);
        let tx = self.tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(wait).await;
            let _ = tx.send(Event::Redial { id });
        });
    }

    fn broadcast(&self, msg: &Msg) {
        for p in self.peers.values() {
            let _ = p.ctl.send(msg.clone());
        }
    }

    /// Send the index entries that changed while handling the last event.
    fn flush(&mut self) {
        let mut entries = std::mem::take(&mut self.outbox);
        entries.sort_by(|a, b| a.path.cmp(&b.path));
        for batch in batches(entries) {
            self.broadcast(&Msg::Index(batch));
        }
    }

    fn on_msg(&mut self, from: &str, msg: Msg) {
        match msg {
            Msg::Hello { .. } | Msg::Ping => {}
            Msg::Config(shared) => self.on_config(&shared),
            Msg::Index(entries) => entries.into_iter().for_each(|e| self.on_entry(from, e)),
            Msg::Request { path, hash } => self.on_request(from, path, hash),
            Msg::Data { path, hash, offset, bytes, eof } => self.on_data(from, &path, hash, offset, &bytes, eof),
            Msg::Unavailable { path, hash } => {
                if self.downloads.get(&path).is_some_and(|d| d.from == from && d.meta.hash == Some(hash)) {
                    let d = self.downloads.remove(&path).unwrap();
                    self.retry_elsewhere(path, d, from);
                }
            }
        }
    }

    // ---- Shared config ----

    fn active_roots(&self) -> HashSet<(String, bool)> {
        self.cfg.active_roots().map(|(r, e)| (r.clone(), e.is_dir)).collect()
    }

    fn on_config(&mut self, shared: &Shared) {
        let before = self.active_roots();
        if self.cfg.merge(shared, &self.me) {
            self.config_changed(&before);
        }
    }

    fn config_changed(&mut self, before: &HashSet<(String, bool)>) {
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

    fn on_ctl(&mut self, req: &Req) -> Resp {
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

    fn status(&self) -> String {
        let s = status_text(&self.cfg, &self.me, |id| Some(self.peers.contains_key(id)), &self.pending);
        let files = self.index.iter().filter(|(p, m)| !m.deleted() && self.is_synced(p)).count();
        format!(
            "{s}\nFiles tracked: {files}\nTransfers: {} active, {} queued\n",
            self.downloads.len(),
            self.queued.len()
        )
    }

    // ---- Watching and scanning ----

    fn reconcile_watches(&mut self) {
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

    fn rescan_all(&mut self) {
        for (r, _) in self.active_roots() {
            self.check_tree(&r);
        }
    }

    fn on_fs(&mut self, lp: &Path) {
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
    fn check_tree(&mut self, p: &str) {
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

    fn walk(&mut self, dir: &Path, seen: &mut HashSet<String>) {
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

    fn is_synced(&self, p: &str) -> bool {
        self.cfg.root_of(p).is_some() && !self.not_a_synced_file(p)
    }

    fn not_a_synced_file(&self, p: &str) -> bool {
        self.cfg.active_root(p).is_some_and(|r| r.is_dir)
            || self.cfg.active_roots().any(|(r, re)| !re.is_dir && r != p && paths::is_under(p, r))
    }

    fn local_root(&self, p: &str) -> Option<PathBuf> {
        self.cfg.root_of(p).and_then(paths::to_local)
    }

    /// Compare one file on disk with the index. Records and announces local changes.
    fn check_file(&mut self, p: &str, lp: &Path) {
        self.check_stat(p, lp, index::stat(lp));
    }

    fn check_stat(&mut self, p: &str, lp: &Path, st: Option<index::Stat>) {
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

    fn set_entry(&mut self, p: &str, meta: FileMeta) {
        self.index.insert(p.to_string(), meta.clone());
        self.index_dirty = true;
        self.outbox.push(Entry { path: p.to_string(), meta });
    }

    fn save_index(&mut self) {
        if !self.index_dirty {
            return;
        }
        match index::save(&self.index, &self.index_file) {
            Ok(()) => self.index_dirty = false,
            Err(e) => log!("cannot save index: {e}"),
        }
    }

    // ---- Remote changes ----

    fn on_entry(&mut self, from: &str, mut e: Entry) {
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
    fn apply(&mut self, from: &str, p: &str, lp: &Path, mut r: FileMeta, mut tmp: Option<PathBuf>) {
        let local = self.index.get(p).cloned();
        let action = index::decide(local.as_ref(), &r);
        let same_content = local.as_ref().is_some_and(|l| l.hash == r.hash);
        match action {
            // When our version wins a conflict we change nothing: the other side sees
            // the same conflict, picks the same winner, and keeps its copy.
            Action::Nothing | Action::LocalWins => {}
            Action::Same => self.adopt(p, lp, local, r),
            Action::Take | Action::Conflict if same_content => self.adopt(p, lp, local, r),
            Action::Take | Action::Conflict if r.deleted() => {
                if local.as_ref().is_some_and(|l| !l.deleted()) {
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
            Action::Take | Action::Conflict => match tmp.take() {
                None => self.start_download(from, p, r),
                Some(t) => {
                    if std::fs::symlink_metadata(lp).is_ok_and(|m| m.is_dir()) {
                        let _ = std::fs::remove_dir(lp);
                    }
                    let mut kept = None;
                    if let (Action::Conflict, Some(l)) = (&action, &local)
                        && !l.deleted()
                        && lp.exists()
                    {
                        let copy = conflict_name(lp, l);
                        if let Err(e) = std::fs::rename(lp, &copy) {
                            let _ = std::fs::remove_file(&t);
                            return log!("cannot keep conflict copy of {p}: {e}");
                        }
                        kept = Some(copy);
                    }
                    if let Err(e) = std::fs::rename(&t, lp) {
                        let _ = std::fs::remove_file(&t);
                        if let Some(copy) = kept {
                            let _ = std::fs::rename(copy, lp);
                        }
                        return log!("cannot write {p}: {e}");
                    }
                    if let Some(copy) = kept {
                        log!("conflict: kept local version of {p} as {}", copy.display());
                    }
                    log!("updated {p}");
                    if let (Action::Conflict, Some(l)) = (&action, &local) {
                        r.vv = l.vv.merged(&r.vv);
                    }
                    self.set_entry(p, r);
                }
            },
        }
        if let Some(t) = tmp {
            let _ = std::fs::remove_file(t);
            self.remove_empty_parents(p, lp);
        }
    }

    fn remove_empty_parents(&self, p: &str, lp: &Path) {
        if let Some(root) = self.local_root(p) {
            let _ = lp
                .ancestors()
                .skip(1)
                .take_while(|a| a.starts_with(&root) && paths::to_portable(a).is_none_or(|ap| self.cfg.active_root(&ap).is_none()))
                .try_for_each(std::fs::remove_dir);
        }
    }

    /// Same content on both sides: keep our file, take the combined version history.
    fn adopt(&mut self, p: &str, lp: &Path, local: Option<FileMeta>, r: FileMeta) {
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

    fn start_download(&mut self, from: &str, p: &str, r: FileMeta) {
        if let Some(d) = self.downloads.get_mut(p) {
            if !matches!(index::decide(Some(&d.meta), &r), Action::Take | Action::Conflict) {
                if d.from != from && d.meta.hash == r.hash && !d.alts.iter().any(|a| a == from) {
                    d.alts.push(from.to_string());
                }
                return;
            }
            self.cancel_download(p);
        }
        if self.downloads.len() >= MAX_DOWNLOADS {
            if let Some((qfrom, q, alts)) = self.queued.get_mut(p) {
                match index::decide(Some(q), &r) {
                    Action::Nothing | Action::Same => {
                        if qfrom != from && q.hash == r.hash && !alts.iter().any(|a| a == from) {
                            alts.push(from.to_string());
                        }
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
        let mut tmp_name = format!(".{}.rsync-tmp", name.to_string_lossy());
        if tmp_name.len() > 255 {
            tmp_name = format!(".{}.rsync-tmp", blake3::hash(name.as_encoded_bytes()).to_hex());
        }
        let tmp = parent.join(tmp_name);
        let file = match std::fs::create_dir_all(parent).and_then(|()| File::create(&tmp)) {
            Ok(f) => f,
            Err(e) => return log!("cannot create {}: {e}", tmp.display()),
        };
        let _ = peer.ctl.send(Msg::Request { path: p.to_string(), hash: r.hash.unwrap() });
        let d = Download { from: from.to_string(), meta: r, tmp, file, hasher: blake3::Hasher::new(), written: 0, alts: Vec::new() };
        self.downloads.insert(p.to_string(), d);
    }

    fn pump_queue(&mut self) {
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

    fn on_data(&mut self, from: &str, path: &str, hash: [u8; 32], offset: u64, bytes: &[u8], eof: bool) {
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

    fn cancel_download(&mut self, p: &str) {
        if let Some(d) = self.downloads.remove(p) {
            let _ = std::fs::remove_file(&d.tmp);
            self.remove_empty_parents(p, &d.tmp);
        }
    }

    fn drop_downloads_from(&mut self, id: &str) {
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
        let _ = std::fs::remove_file(&d.tmp);
        self.remove_empty_parents(&p, &d.tmp);
        d.alts.retain(|a| a != gone && self.peers.contains_key(a));
        if !d.alts.is_empty() {
            let alt = d.alts.remove(0);
            self.queued.entry(p).or_insert((alt, d.meta, d.alts));
        }
    }

    fn on_request(&self, from: &str, path: String, hash: [u8; 32]) {
        let Some(peer) = self.peers.get(from) else { return };
        let root = self.local_root(&path);
        let ok = root.is_some() && !self.not_a_synced_file(&path) && self.index.get(&path).is_some_and(|m| m.hash == Some(hash));
        let safe = |lp: &Path| index::stat(lp).is_some() && !root.as_ref().is_some_and(|r| paths::through_symlink(lp, r));
        match paths::to_local(&path) {
            Some(lp) if ok && safe(&lp) => drop(tokio::spawn(upload(lp, root, path, hash, peer.data.clone(), self.uploads.clone()))),
            _ => drop(peer.ctl.send(Msg::Unavailable { path, hash })),
        }
    }
}

/// `notes.md` -> `notes.sync-conflict-<mtime>-<node>.md`, named after the losing
/// version, so every node that holds that version picks the same name.
fn conflict_name(lp: &Path, loser: &FileMeta) -> PathBuf {
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

// ---- Tasks ----

/// Collect watcher events until things go quiet, then pass them on as one batch.
async fn debounce(mut rx: mpsc::UnboundedReceiver<Option<PathBuf>>, tx: mpsc::UnboundedSender<Event>) {
    while let Some(first) = rx.recv().await {
        let (mut paths, mut rescan) = (HashSet::new(), false);
        let mut add = |p: Option<PathBuf>| match p {
            Some(p) => drop(paths.insert(p)),
            None => rescan = true,
        };
        add(first);
        let deadline = Instant::now() + SETTLE_MAX;
        while Instant::now() < deadline {
            match timeout(SETTLE, rx.recv()).await {
                Ok(Some(p)) => add(p),
                Ok(None) => return,
                Err(_) => break,
            }
        }
        let ev = if rescan { Event::Rescan } else { Event::Fs(paths.into_iter().collect()) };
        if tx.send(ev).is_err() {
            return;
        }
    }
}

async fn accept_loop(l: TcpListener, keys: Arc<Keys>, tx: mpsc::UnboundedSender<Event>, port: u16) {
    loop {
        match l.accept().await {
            Ok((s, addr)) => {
                drop(tokio::spawn(connection(s, addr.ip().to_canonical(), None, keys.clone(), tx.clone(), port)));
            }
            Err(_) => tokio::time::sleep(Duration::from_secs(1)).await,
        }
    }
}

/// Answer CLI requests, one JSON line per connection.
async fn ctl_loop(l: UnixListener, tx: mpsc::UnboundedSender<Event>) {
    loop {
        let Ok((s, _)) = l.accept().await else {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        };
        let tx = tx.clone();
        tokio::spawn(async move {
            let (r, mut w) = s.into_split();
            let mut line = String::new();
            BufReader::new(r).read_line(&mut line).await?;
            let resp = match serde_json::from_str(&line) {
                Ok(req) => {
                    let (reply, rx) = oneshot::channel();
                    tx.send(Event::Ctl(req, reply))?;
                    rx.await?
                }
                Err(e) => Resp {
                    ok: false,
                    text: format!(
                        "the daemon cannot read this request ({e}). Is it running a different version? Try `rust-sync service restart`."
                    ),
                },
            };
            let mut out = serde_json::to_string(&resp)?;
            out.push('\n');
            w.write_all(out.as_bytes()).await?;
            anyhow::Ok(())
        });
    }
}

async fn dial(addr: String, id: String, quiet: bool, keys: Arc<Keys>, tx: mpsc::UnboundedSender<Event>, port: u16) {
    let addr = config::with_port(&addr);
    match timeout(DIAL_TIMEOUT, connect_any(&addr)).await {
        Ok(Ok(s)) => {
            let ip = s.peer_addr().map_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED), |a| a.ip().to_canonical());
            connection(s, ip, Some(id.clone()), keys, tx.clone(), port).await;
        }
        Ok(Err(e)) if !quiet => {
            let hint = if e.kind() == std::io::ErrorKind::HostUnreachable {
                " (on macOS this can mean Local Network access is blocked for this process; see README)"
            } else {
                ""
            };
            log!("cannot reach {} at {addr}: {e}{hint}", net::pretty_id(&id));
        }
        Err(_) if !quiet => log!("cannot reach {} at {addr}: timed out", net::pretty_id(&id)),
        _ => {}
    }
    let _ = tx.send(Event::DialDone { id });
}

/// Connect to any address `addr` resolves to. Tries them in parallel, a new one every
/// `ATTEMPT_DELAY`, alternating IPv6 and IPv4, and keeps the first that connects
/// (Happy Eyeballs, RFC 8305). A failed attempt starts the next one at once.
async fn connect_any(addr: &str) -> std::io::Result<TcpStream> {
    let (v6, v4): (Vec<SocketAddr>, Vec<SocketAddr>) =
        tokio::net::lookup_host(addr).await?.partition(SocketAddr::is_ipv6);
    let mut order = Vec::with_capacity(v6.len() + v4.len());
    let (mut v6, mut v4) = (v6.into_iter(), v4.into_iter());
    loop {
        match (v6.next(), v4.next()) {
            (None, None) => break,
            (a, b) => order.extend(a.into_iter().chain(b)),
        }
    }
    let mut next = order.into_iter();
    let mut attempts = tokio::task::JoinSet::new();
    let mut last_err = std::io::Error::new(std::io::ErrorKind::NotFound, "no addresses found");
    loop {
        if let Some(a) = next.next() {
            attempts.spawn(TcpStream::connect(a));
        } else if attempts.is_empty() {
            return Err(last_err);
        }
        tokio::select! {
            Some(res) = attempts.join_next() => match res {
                Ok(Ok(s)) => return Ok(s),
                Ok(Err(e)) => last_err = e,
                Err(e) => last_err = std::io::Error::other(e),
            },
            () = tokio::time::sleep(ATTEMPT_DELAY), if next.len() > 0 => {}
        }
    }
}

/// Handle one connection from start to end. `expect` is set when we dialled out.
async fn connection(
    s: TcpStream,
    ip: IpAddr,
    expect: Option<String>,
    keys: Arc<Keys>,
    tx: mpsc::UnboundedSender<Event>,
    port: u16,
) {
    if let Err(e) = connection_inner(s, ip, expect, keys, tx, port).await {
        log!("connection with {ip}: {e:#}");
    }
}

async fn connection_inner(
    s: TcpStream,
    ip: IpAddr,
    expect: Option<String>,
    keys: Arc<Keys>,
    tx: mpsc::UnboundedSender<Event>,
    port: u16,
) -> anyhow::Result<()> {
    let outbound = expect.is_some();
    let (mut r, mut w, id) = net::handshake(s, &keys, outbound).await?;
    if let Some(e) = &expect
        && *e != id
    {
        bail!("expected node {} but found {}", net::pretty_id(e), net::pretty_id(&id));
    }
    let auth = async |addr: Option<String>| {
        let (reply, rx) = oneshot::channel();
        tx.send(Event::Auth { id: id.clone(), addr, reply }).ok()?;
        rx.await.ok()
    };
    let hello = |trusts_you| Msg::Hello { port, probe: false, trusts_you };
    let recv_hello = async |r: &mut net::Reader| match timeout(Duration::from_secs(10), r.recv()).await?? {
        Msg::Hello { port, probe, trusts_you } => anyhow::Ok((port, probe, trusts_you)),
        _ => bail!("expected hello"),
    };
    let (trusted, probe, trusts_us) = if outbound {
        let trusted = auth(None).await.unwrap_or(false);
        w.send(&hello(trusted)).await?;
        let (_, probe, trusts_us) = recv_hello(&mut r).await?;
        (trusted, probe, trusts_us)
    } else {
        let (their_port, probe, trusts_us) = recv_hello(&mut r).await?;
        let trusted = auth(Some(SocketAddr::new(ip, their_port).to_string())).await.unwrap_or(false);
        w.send(&hello(trusted)).await?;
        (trusted, probe, trusts_us)
    };
    if probe || !trusted {
        return Ok(());
    }
    if !trusts_us {
        log!("node {} does not trust this node yet", net::pretty_id(&id));
        return Ok(());
    }

    let conn = NEXT_CONN.fetch_add(1, Ordering::Relaxed);
    let (ctl_tx, mut ctl_rx) = mpsc::unbounded_channel();
    let (data_tx, mut data_rx) = mpsc::channel(8);
    tx.send(Event::Up { id: id.clone(), conn, outbound, ctl: ctl_tx, data: data_tx })?;
    let writer = async move {
        loop {
            tokio::select! {
                biased;
                m = ctl_rx.recv() => match m {
                    Some(m) => w.send(&m).await?,
                    None => return anyhow::Ok(()),
                },
                Some(m) = data_rx.recv() => w.send(&m).await?,
            }
        }
    };
    let reader = async {
        loop {
            let msg = timeout(IDLE_LIMIT, r.recv()).await.context("peer went quiet")??;
            if tx.send(Event::Peer { id: id.clone(), conn, msg }).is_err() {
                return anyhow::Ok(());
            }
        }
    };
    let res = tokio::select! { r = writer => r, r = reader => r };
    let _ = tx.send(Event::Down { id, conn });
    res
}

/// Send one file in chunks. Waits when the peer's send queue is full.
async fn upload(lp: PathBuf, root: Option<PathBuf>, path: String, hash: [u8; 32], tx: mpsc::Sender<Msg>, slots: Arc<Semaphore>) {
    let Ok(_slot) = slots.acquire().await else { return };
    let opened = File::open(&lp).ok().filter(|f| {
        let (Ok(a), Ok(b)) = (f.metadata(), std::fs::symlink_metadata(&lp)) else { return false };
        b.is_file() && (a.dev(), a.ino()) == (b.dev(), b.ino()) && !root.as_ref().is_some_and(|r| paths::through_symlink(&lp, r))
    });
    let Some(mut f) = opened else {
        let _ = tx.send(Msg::Unavailable { path, hash }).await;
        return;
    };
    let mut buf = vec![0u8; net::CHUNK];
    let mut offset = 0u64;
    loop {
        let msg = match f.read(&mut buf) {
            Ok(0) => Msg::Data { path: path.clone(), hash, offset, bytes: Vec::new(), eof: true },
            Ok(n) => Msg::Data { path: path.clone(), hash, offset, bytes: buf[..n].to_vec(), eof: false },
            Err(_) => Msg::Unavailable { path: path.clone(), hash },
        };
        let done = !matches!(msg, Msg::Data { eof: false, .. });
        if let Msg::Data { bytes, .. } = &msg {
            offset += bytes.len() as u64;
        }
        if tx.send(msg).await.is_err() || done {
            return;
        }
    }
}

#[cfg(test)]
mod tests;
