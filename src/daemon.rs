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
        if matches!(ev, Event::Shutdown) {
            break;
        }
        d.handle(ev);
        d.pump_queue();
        d.flush();
    }

    log!("stopping");
    for (_, dl) in d.downloads.drain() {
        let _ = std::fs::remove_file(dl.tmp);
    }
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
                    self.redial_after(id, Duration::from_secs(1));
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
            Event::Shutdown => {}
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
        let Some(n) = self.cfg.trusted(id) else { return };
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
            .filter(|p| self.cfg.root_of(p).is_none() || self.cfg.active_root(p).is_some_and(|r| r.is_dir) || self.below_file_root(p))
            .cloned()
            .collect();
        for p in unwanted {
            self.cancel_download(&p);
        }
        self.reconcile_watches();
        for (root, _) in self.active_roots().difference(before) {
            log!("now syncing {root}");
            self.check_tree(root);
            // Peers may not have seen our entries for this root yet.
            self.outbox.extend(index::under(&self.index, root).map(Entry::from));
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
        let files = self.index.iter().filter(|(p, m)| !m.deleted() && self.cfg.root_of(p).is_some()).count();
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
            _ if !self.below_file_root(p) && !self.cfg.active_root(p).is_some_and(|r| r.is_dir) => {
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
                && !self.below_file_root(&p)
            {
                self.check_file(&p, &path);
                seen.insert(p);
            }
        }
    }

    fn below_file_root(&self, p: &str) -> bool {
        self.cfg.active_roots().any(|(r, re)| !re.is_dir && r != p && paths::is_under(p, r))
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
                let Ok(hash) = index::hash_file(lp) else { return };
                if let Some(m) = self.index.get_mut(p)
                    && m.hash == Some(hash)
                    && m.mode == s.mode
                {
                    // Only the timestamp changed. Remember it, but it is not a new version.
                    (m.size, m.mtime) = (s.size, s.mtime);
                    self.index_dirty = true;
                    return;
                }
                FileMeta { hash: Some(hash), size: s.size, mtime: s.mtime, mode: s.mode, vv: bump(&old), by: me }
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
        if !e.meta.deleted() && (self.cfg.active_root(&e.path).is_some_and(|r| r.is_dir) || self.below_file_root(&e.path)) || paths::through_symlink(&lp, &root) {
            return;
        }
        if lp.file_name().and_then(|n| n.to_str()).is_none_or(paths::ignored) {
            return;
        }
        // Catch local changes the watcher has not reported yet, before deciding.
        self.check_file(&e.path, &lp);
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
            let _ = std::fs::remove_file(d.tmp);
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
        let ok = root.is_some() && self.index.get(&path).is_some_and(|m| m.hash == Some(hash));
        let safe = |lp: &Path| index::stat(lp).is_some() && !root.as_ref().is_some_and(|r| paths::through_symlink(lp, r));
        match paths::to_local(&path) {
            Some(lp) if ok && safe(&lp) => drop(tokio::spawn(upload(lp, root, path, hash, peer.data.clone(), self.uploads.clone()))),
            _ => drop(peer.ctl.send(Msg::Unavailable { path, hash })),
        }
    }
}

/// Status text. `connected` returns `None` when the daemon is not running.
/// `pending` holds untrusted nodes that tried to connect (ID -> address).
pub fn status_text(
    cfg: &Config,
    me: &str,
    connected: impl Fn(&str) -> Option<bool>,
    pending: &BTreeMap<String, String>,
) -> String {
    let daemon = if connected(me).is_none() { "Daemon: not running (start it with `rust-sync daemon`)\n" } else { "" };
    format!(
        "This node: {}\nPort: {}\n{daemon}\nNodes:\n{}\nPaths:\n{}",
        net::pretty_id(me),
        cfg.port,
        nodes_text(cfg, connected, pending),
        paths_text(cfg)
    )
}

/// Synced files and folders. Folders end with `/`.
pub fn paths_text(cfg: &Config) -> String {
    let roots: Vec<_> = cfg.active_roots().collect();
    if roots.is_empty() {
        return "  (none) add one with `rust-sync path add <path>`\n".into();
    }
    roots.into_iter().map(|(p, r)| format!("  {p}{}\n", if r.is_dir { "/" } else { "" })).collect()
}

/// Trusted nodes with their address and connection state, then untrusted nodes
/// waiting to be accepted. `connected` returns `None` when the daemon is not running.
pub fn nodes_text(
    cfg: &Config,
    connected: impl Fn(&str) -> Option<bool>,
    pending: &BTreeMap<String, String>,
) -> String {
    let mut s = String::new();
    let nodes: Vec<_> = cfg.trusted_nodes().collect();
    if nodes.is_empty() {
        s.push_str("  (none) add one with `rust-sync node add <host>`\n");
    }
    s.extend(nodes.into_iter().map(|(id, n)| {
        let state = match connected(id) {
            Some(true) => "connected",
            Some(false) => "offline",
            None => "",
        };
        format!("  {}  {}  {state}\n", net::pretty_id(id), n.addr)
    }));
    if !pending.is_empty() {
        s.push_str("\nWaiting to be trusted (run `rust-sync node add <address>` to accept):\n");
        s.extend(pending.iter().map(|(id, addr)| format!("  {}  {addr}\n", net::pretty_id(id))));
    }
    s
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

/// Split entries into messages that fit in one frame.
fn batches(entries: Vec<Entry>) -> Vec<Vec<Entry>> {
    const LIMIT: usize = 40_000;
    let (mut out, mut cur, mut size) = (Vec::new(), Vec::new(), 0);
    for e in entries {
        let s = e.path.len() + 100 + e.meta.vv.0.len() * 20;
        if size + s > LIMIT && !cur.is_empty() {
            out.push(std::mem::take(&mut cur));
            size = 0;
        }
        size += s;
        cur.push(e);
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// Split the shared config into parts that each fit in one message.
fn config_batches(shared: &Shared) -> Vec<Shared> {
    const LIMIT: usize = 40_000;
    let mut out = vec![Shared::default()];
    let mut size = 0;
    let mut add = |s: usize, put: &mut dyn FnMut(&mut Shared)| {
        if size + s > LIMIT && out.last().is_some_and(|c| !c.nodes.is_empty() || !c.roots.is_empty()) {
            out.push(Shared::default());
            size = 0;
        }
        size += s;
        put(out.last_mut().unwrap());
    };
    for (id, n) in &shared.nodes {
        let s = postcard::to_stdvec(&(id, n)).map_or(0, |b| b.len());
        add(s, &mut |c| drop(c.nodes.insert(id.clone(), n.clone())));
    }
    for (path, r) in &shared.roots {
        let s = postcard::to_stdvec(&(path, r)).map_or(0, |b| b.len());
        add(s, &mut |c| drop(c.roots.insert(path.clone(), r.clone())));
    }
    out
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
mod tests {
    use super::*;

    fn meta(mtime: i64, by: u64) -> FileMeta {
        FileMeta { hash: Some([1; 32]), size: 1, mtime, mode: 0o644, vv: Vv::default(), by }
    }

    fn entry(path: String, nodes: u64) -> Entry {
        let mut m = meta(0, 0);
        m.vv = Vv((0..nodes).map(|n| (u64::MAX - n, u64::MAX)).collect());
        Entry { path, meta: m }
    }

    fn download(from: &str, meta: FileMeta, tmp: &Path) -> Download {
        let file = File::create(tmp).unwrap();
        Download { from: from.into(), meta, tmp: tmp.to_path_buf(), file, hasher: blake3::Hasher::new(), written: 0, alts: Vec::new() }
    }

    fn test_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("rust-sync-{name}-{}", std::process::id()))
    }

    fn up(d: &mut Daemon, id: &str, conn: u64) -> mpsc::UnboundedReceiver<Msg> {
        let (ctl, rx) = mpsc::unbounded_channel();
        d.handle(Event::Up { id: id.into(), conn, outbound: true, ctl, data: mpsc::channel(1).0 });
        rx
    }

    fn daemon() -> Daemon {
        let keys = Arc::new(Keys { private: vec![0; 32], public: vec![1; 32] });
        let me = keys.id();
        Daemon {
            me_short: net::short_id(&me),
            me,
            keys,
            cfg: Config::default(),
            config_file: test_dir("no-state").join("config.json"),
            index: Index::new(),
            index_file: test_dir("no-state").join("index.bin"),
            peers: HashMap::new(),
            dialling: HashSet::new(),
            backoff: HashMap::new(),
            pending: BTreeMap::new(),
            pending_order: VecDeque::new(),
            downloads: HashMap::new(),
            queued: BTreeMap::new(),
            uploads: Arc::new(Semaphore::new(MAX_UPLOADS)),
            watcher: notify::recommended_watcher(|_: notify::Result<notify::Event>| {}).unwrap(),
            watched: HashMap::new(),
            tx: mpsc::unbounded_channel().0,
            outbox: Vec::new(),
            index_dirty: false,
            ticks: 0,
        }
    }

    #[test]
    fn untrusted_nodes_are_listed_until_newer_ones_push_the_oldest_out() {
        let mut d = daemon();
        for i in 0..=MAX_PENDING {
            let (reply, mut rx) = oneshot::channel();
            d.handle(Event::Auth { id: format!("{i:02}"), addr: Some(format!("h:{i}")), reply });
            assert!(!rx.try_recv().unwrap());
        }
        let text = d.on_ctl(&Req::ListNodes).text;
        assert!(!text.contains("  00  h:0\n"), "{text}");
        assert!(text.contains("  01  h:1\n"), "{text}");
        assert!(text.contains(&format!("  {MAX_PENDING}  h:{MAX_PENDING}\n")), "{text}");
        assert_eq!(text.matches("  h:").count(), MAX_PENDING);
    }

    #[test]
    fn untrusted_node_trying_again_is_listed_with_its_new_address_and_kept_as_newest() {
        let mut d = daemon();
        let mut auth = |id: String, addr: String| {
            d.handle(Event::Auth { id, addr: Some(addr), reply: oneshot::channel().0 });
        };
        for i in 0..MAX_PENDING {
            auth(format!("{i:02}"), format!("h:{i}"));
        }
        auth("00".into(), "h:new".into());
        auth("99".into(), "h:99".into());
        let text = d.on_ctl(&Req::ListNodes).text;
        assert!(text.contains("  00  h:new\n"), "{text}");
        assert!(!text.contains("  01  h:1\n"), "{text}");
        assert!(text.contains("  99  h:99\n"), "{text}");
        assert_eq!(text.matches("  h:").count(), MAX_PENDING);
    }

    #[test]
    fn trusted_node_is_accepted_and_not_listed_as_waiting() {
        let mut d = daemon();
        let me = d.me.clone();
        d.cfg.set_node(&me, "aa", "h:1", false);
        let (reply, mut rx) = oneshot::channel();
        d.handle(Event::Auth { id: "aa".into(), addr: Some("h:2".into()), reply });
        assert!(rx.try_recv().unwrap());
        let text = d.on_ctl(&Req::ListNodes).text;
        assert!(!text.contains("Waiting to be trusted"), "{text}");
    }

    #[tokio::test]
    async fn trusting_a_waiting_node_stops_listing_it_and_dials_it_without_waiting_to_retry() {
        let mut d = daemon();
        d.handle(Event::Auth { id: "aa".into(), addr: Some("h:1".into()), reply: oneshot::channel().0 });
        d.backoff.insert("aa".into(), Duration::from_secs(8));
        let resp = d.on_ctl(&Req::AddNode { id: "aa".into(), addr: "127.0.0.1:1".into() });
        assert!(resp.ok, "{}", resp.text);
        let text = d.on_ctl(&Req::ListNodes).text;
        assert!(!text.contains("Waiting to be trusted"), "{text}");
        assert!(d.backoff.is_empty());
        assert_eq!(d.dialling, HashSet::from(["aa".to_string()]));
    }

    #[test]
    fn waiting_node_trusted_by_a_peer_is_no_longer_listed_once_connected() {
        let mut d = daemon();
        d.handle(Event::Auth { id: "aa".into(), addr: Some("h:1".into()), reply: oneshot::channel().0 });
        d.cfg.set_node("peer", "aa", "h:1", false);
        up(&mut d, "aa", 1);
        let text = d.on_ctl(&Req::ListNodes).text;
        assert!(!text.contains("Waiting to be trusted"), "{text}");
    }

    #[test]
    fn connection_from_a_node_removed_after_auth_is_not_used() {
        let mut d = daemon();
        let me = d.me.clone();
        d.cfg.set_node(&me, "aa", "h:1", false);
        d.cfg.set_node(&me, "aa", "h:1", true);
        let mut rx = up(&mut d, "aa", 1);
        assert!(rx.try_recv().is_err());
        assert!(d.peers.is_empty());
    }

    #[test]
    fn duplicate_connection_is_kept_only_if_dialled_by_the_lower_id() {
        let mut d = daemon();
        let me = d.me.clone();
        for (id, keep) in [("00", false), ("ff", true)] {
            d.cfg.set_node(&me, id, "h:1", false);
            let mut up = |conn, outbound| {
                let (ctl, mut rx) = mpsc::unbounded_channel();
                let data = mpsc::channel(1).0;
                d.handle(Event::Up { id: id.into(), conn, outbound, ctl, data });
                matches!(rx.try_recv(), Ok(Msg::Config(_)))
            };
            assert!(up(1, !keep));
            assert!(!up(2, !keep));
            assert!(up(3, keep));
        }
    }

    #[test]
    fn duplicate_connection_that_is_kept_cancels_downloads_over_the_old_one() {
        let mut d = daemon();
        let me = d.me.clone();
        d.cfg.set_node(&me, "ff", "h:1", false);
        let _rx = up(&mut d, "ff", 1);
        let tmp = test_dir("dup-down");
        d.downloads.insert("~/s/f".into(), download("ff", meta(0, 0), &tmp));
        let (ctl, _rx2) = mpsc::unbounded_channel();
        d.handle(Event::Up { id: "ff".into(), conn: 2, outbound: false, ctl, data: mpsc::channel(1).0 });
        assert!(tmp.exists());
        let _rx3 = up(&mut d, "ff", 3);
        assert!(!tmp.exists());
        assert!(d.downloads.is_empty());
    }

    #[tokio::test]
    async fn tick_pings_peers_and_dials_offline_nodes_not_waiting_to_retry() {
        let mut d = daemon();
        let me = d.me.clone();
        for id in ["aa", "bb", "cc"] {
            d.cfg.set_node(&me, id, "127.0.0.1:1", false);
        }
        let mut rx = up(&mut d, "aa", 1);
        while rx.try_recv().is_ok() {}
        d.backoff.insert("bb".into(), Duration::from_secs(1));
        d.handle(Event::Tick);
        assert!(matches!(rx.try_recv(), Ok(Msg::Ping)));
        assert_eq!(d.dialling, HashSet::from(["cc".to_string()]));
        d.handle(Event::Redial { id: "bb".into() });
        assert_eq!(d.dialling, HashSet::from(["bb".to_string(), "cc".to_string()]));
    }

    #[tokio::test]
    async fn node_being_dialled_is_not_dialled_again_until_the_dial_is_done() {
        let mut d = daemon();
        let me = d.me.clone();
        d.cfg.set_node(&me, "aa", "127.0.0.1:1", false);
        d.backoff.insert("aa".into(), Duration::from_secs(1));
        let (tx, mut rx) = mpsc::unbounded_channel();
        d.tx = tx;
        d.handle(Event::Redial { id: "aa".into() });
        d.handle(Event::Redial { id: "aa".into() });
        let done = rx.recv().await.unwrap();
        assert!(matches!(done, Event::DialDone { .. }));
        assert!(timeout(Duration::from_millis(200), rx.recv()).await.is_err());
        d.handle(done);
        d.handle(Event::Redial { id: "aa".into() });
        assert!(matches!(rx.recv().await, Some(Event::DialDone { .. })));
    }

    #[test]
    fn tick_saves_the_index_only_after_it_changed() {
        let mut d = daemon();
        let dir = test_dir("tick-save");
        std::fs::create_dir_all(&dir).unwrap();
        d.index_file = dir.join("index.bin");
        d.handle(Event::Tick);
        assert!(!d.index_file.exists());
        d.set_entry("~/s/a", meta(0, 0));
        d.handle(Event::Tick);
        assert_eq!(index::load(&d.index_file).get("~/s/a"), Some(&meta(0, 0)));
        std::fs::remove_file(&d.index_file).unwrap();
        d.handle(Event::Tick);
        assert!(!d.index_file.exists());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn tick_rescans_active_roots_only_every_rescan_ticks() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("tick-rescan");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("f.txt"), "x").unwrap();
        d.index_file = dir.join("index.bin");
        d.cfg.set_root(&me, dir.to_str().unwrap(), true, false);
        for _ in 1..RESCAN_TICKS {
            d.handle(Event::Tick);
        }
        assert!(d.index.is_empty());
        d.handle(Event::Tick);
        assert!(d.index.contains_key(dir.join("f.txt").to_str().unwrap()));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn only_the_current_connection_going_down_marks_the_node_offline() {
        let mut d = daemon();
        let me = d.me.clone();
        d.cfg.set_node(&me, "aa", "h:1", false);
        let _rx = up(&mut d, "aa", 1);
        d.handle(Event::Down { id: "aa".into(), conn: 2 });
        assert!(d.on_ctl(&Req::ListNodes).text.contains("  connected"));
        d.handle(Event::Down { id: "aa".into(), conn: 1 });
        assert!(d.on_ctl(&Req::ListNodes).text.contains("  offline"));
    }

    #[tokio::test]
    async fn node_that_goes_down_is_redialled_after_a_second() {
        let mut d = daemon();
        let me = d.me.clone();
        d.cfg.set_node(&me, "aa", "h:1", false);
        let _rx = up(&mut d, "aa", 1);
        let (tx, mut rx) = mpsc::unbounded_channel();
        d.tx = tx;
        d.handle(Event::Down { id: "aa".into(), conn: 1 });
        assert!(timeout(Duration::from_millis(900), rx.recv()).await.is_err());
        let ev = timeout(Duration::from_millis(500), rx.recv()).await.unwrap();
        assert!(matches!(ev, Some(Event::Redial { id }) if id == "aa"));
    }

    #[tokio::test]
    async fn failed_dials_wait_twice_as_long_each_time_up_to_a_limit_until_connected() {
        let mut d = daemon();
        let me = d.me.clone();
        d.cfg.set_node(&me, "aa", "h:1", false);
        let mut waits = Vec::new();
        for _ in 0..7 {
            d.handle(Event::DialDone { id: "aa".into() });
            waits.push(d.backoff["aa"].as_secs());
        }
        assert_eq!(waits, [1, 2, 4, 8, 16, MAX_BACKOFF.as_secs(), MAX_BACKOFF.as_secs()]);
        up(&mut d, "aa", 1);
        d.handle(Event::DialDone { id: "aa".into() });
        assert!(d.backoff.is_empty());
    }

    #[tokio::test]
    async fn going_down_cancels_only_the_downloads_from_that_node() {
        let mut d = daemon();
        let me = d.me.clone();
        let mut tmps = Vec::new();
        for (id, conn) in [("aa", 1), ("bb", 2)] {
            d.cfg.set_node(&me, id, "h:1", false);
            up(&mut d, id, conn);
            let tmp = test_dir(&format!("down-{id}"));
            d.downloads.insert(format!("~/s/{id}"), download(id, meta(0, 0), &tmp));
            tmps.push(tmp);
        }
        d.handle(Event::Down { id: "aa".into(), conn: 1 });
        assert!(!tmps[0].exists());
        assert!(tmps[1].exists());
        d.cancel_download("~/s/bb");
    }

    #[tokio::test]
    async fn download_from_a_node_that_goes_down_is_requested_from_another_node_that_offered_it() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("download-other-source");
        let root = dir.join("s");
        std::fs::create_dir_all(&root).unwrap();
        let rp = root.to_str().unwrap().to_string();
        d.cfg.set_root(&me, &rp, true, false);
        for id in ["aa", "bb"] {
            d.cfg.set_node(&me, id, "h:1", false);
        }
        let (_a, mut b) = (up(&mut d, "aa", 1), up(&mut d, "bb", 2));
        while b.try_recv().is_ok() {}
        let p = format!("{rp}/f");
        let r = FileMeta { vv: Vv(vec![(1, 1)]), ..meta(0, 1) };
        d.on_entry("aa", Entry { path: p.clone(), meta: r.clone() });
        d.on_entry("bb", Entry { path: p.clone(), meta: r });
        assert!(b.try_recv().is_err());
        d.handle(Event::Down { id: "aa".into(), conn: 1 });
        d.pump_queue();
        assert!(matches!(b.try_recv(), Ok(Msg::Request { path, .. }) if path == p));
        assert_eq!(d.downloads[&p].from, "bb");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn queued_download_from_a_node_that_goes_down_is_requested_from_another_node_that_offered_it() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("queued-other-source");
        let root = dir.join("s");
        std::fs::create_dir_all(&root).unwrap();
        let rp = root.to_str().unwrap().to_string();
        d.cfg.set_root(&me, &rp, true, false);
        for id in ["aa", "bb"] {
            d.cfg.set_node(&me, id, "h:1", false);
        }
        let (mut a, mut b) = (up(&mut d, "aa", 1), up(&mut d, "bb", 2));
        while a.try_recv().is_ok() {}
        while b.try_recv().is_ok() {}
        for n in 0..MAX_DOWNLOADS {
            d.downloads.insert(format!("{rp}/{n}"), download("aa", meta(0, 0), &dir.join(format!("t{n}"))));
        }
        let p = format!("{rp}/f");
        let r = FileMeta { vv: Vv(vec![(1, 1)]), ..meta(0, 1) };
        for from in ["aa", "bb"] {
            d.on_entry(from, Entry { path: p.clone(), meta: r.clone() });
        }
        d.downloads.clear();
        d.handle(Event::Down { id: "bb".into(), conn: 2 });
        d.pump_queue();
        assert!(matches!(a.try_recv(), Ok(Msg::Request { path, .. }) if path == p));
        assert_eq!(d.downloads[&p].from, "aa");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn queued_download_is_kept_when_another_node_offers_a_concurrent_version_with_the_same_content_and_goes_down() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("queued-same-content");
        let root = dir.join("s");
        std::fs::create_dir_all(&root).unwrap();
        let rp = root.to_str().unwrap().to_string();
        d.cfg.set_root(&me, &rp, true, false);
        for id in ["aa", "bb"] {
            d.cfg.set_node(&me, id, "h:1", false);
        }
        let (mut a, mut b) = (up(&mut d, "aa", 1), up(&mut d, "bb", 2));
        while a.try_recv().is_ok() {}
        while b.try_recv().is_ok() {}
        for n in 0..MAX_DOWNLOADS {
            d.downloads.insert(format!("{rp}/{n}"), download("aa", meta(0, 0), &dir.join(format!("t{n}"))));
        }
        let p = format!("{rp}/f");
        d.on_entry("aa", Entry { path: p.clone(), meta: FileMeta { vv: Vv(vec![(1, 1)]), ..meta(0, 1) } });
        d.on_entry("bb", Entry { path: p.clone(), meta: FileMeta { vv: Vv(vec![(2, 1)]), ..meta(5, 2) } });
        d.downloads.clear();
        d.handle(Event::Down { id: "bb".into(), conn: 2 });
        d.pump_queue();
        assert!(matches!(a.try_recv(), Ok(Msg::Request { path, .. }) if path == p));
        assert_eq!(d.downloads[&p].from, "aa");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn queued_download_that_starts_and_loses_its_node_is_requested_from_another_node_that_offered_it() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("started-other-source");
        let root = dir.join("s");
        std::fs::create_dir_all(&root).unwrap();
        let rp = root.to_str().unwrap().to_string();
        d.cfg.set_root(&me, &rp, true, false);
        for id in ["aa", "bb"] {
            d.cfg.set_node(&me, id, "h:1", false);
        }
        let (mut a, mut b) = (up(&mut d, "aa", 1), up(&mut d, "bb", 2));
        while a.try_recv().is_ok() {}
        while b.try_recv().is_ok() {}
        for n in 0..MAX_DOWNLOADS {
            d.downloads.insert(format!("{rp}/{n}"), download("aa", meta(0, 0), &dir.join(format!("t{n}"))));
        }
        let p = format!("{rp}/f");
        let r = FileMeta { vv: Vv(vec![(1, 1)]), ..meta(0, 1) };
        for from in ["aa", "bb"] {
            d.on_entry(from, Entry { path: p.clone(), meta: r.clone() });
        }
        d.downloads.clear();
        d.pump_queue();
        assert!(matches!(a.try_recv(), Ok(Msg::Request { path, .. }) if path == p));
        d.handle(Event::Down { id: "aa".into(), conn: 1 });
        d.pump_queue();
        assert!(matches!(b.try_recv(), Ok(Msg::Request { path, .. }) if path == p));
        assert_eq!(d.downloads[&p].from, "bb");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn file_with_a_name_of_the_longest_allowed_length_is_downloaded() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("download-long-name");
        let root = dir.join("s");
        std::fs::create_dir_all(&root).unwrap();
        let rp = root.to_str().unwrap().to_string();
        d.cfg.set_root(&me, &rp, true, false);
        d.cfg.set_node(&me, "aa", "h:1", false);
        let mut a = up(&mut d, "aa", 1);
        while a.try_recv().is_ok() {}
        let p = format!("{rp}/{}", "n".repeat(255));
        let hash = *blake3::hash(b"abc").as_bytes();
        let r = FileMeta { hash: Some(hash), size: 3, vv: Vv(vec![(1, 1)]), ..meta(0, 1) };
        d.on_entry("aa", Entry { path: p.clone(), meta: r });
        assert!(matches!(a.try_recv(), Ok(Msg::Request { path, .. }) if path == p));
        let msg = Msg::Data { path: p.clone(), hash, offset: 0, bytes: b"abc".to_vec(), eof: true };
        d.handle(Event::Peer { id: "aa".into(), conn: 1, msg });
        assert_eq!(std::fs::read(&p).unwrap(), b"abc");
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn request_is_refused_outside_shared_folders_or_for_another_version() {
        let mut d = daemon();
        let me = d.me.clone();
        d.cfg.set_node(&me, "aa", "h:1", false);
        d.cfg.set_root(&me, "~/s", true, false);
        d.index.insert("~/s/f".into(), meta(0, 0));
        d.index.insert("~/x".into(), meta(0, 0));
        let mut rx = up(&mut d, "aa", 1);
        while rx.try_recv().is_ok() {}
        for (path, hash) in [("~/s/f", [2; 32]), ("~/x", [1; 32])] {
            d.handle(Event::Peer { id: "aa".into(), conn: 1, msg: Msg::Request { path: path.into(), hash } });
            assert!(matches!(rx.try_recv(), Ok(Msg::Unavailable { path: p, .. }) if p == path));
        }
    }

    #[tokio::test]
    async fn request_is_refused_for_a_file_reached_through_a_symlink() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("request-symlink");
        let root = dir.join("s");
        std::fs::create_dir_all(root.join("d")).unwrap();
        std::fs::write(dir.join("secret"), "abc").unwrap();
        let rp = root.to_str().unwrap().to_string();
        d.cfg.set_root(&me, &rp, true, false);
        d.cfg.set_node(&me, "aa", "h:1", false);
        let hash = *blake3::hash(b"abc").as_bytes();
        let synced = FileMeta { hash: Some(hash), size: 3, ..meta(0, 0) };
        d.index.insert(format!("{rp}/d/f"), synced.clone());
        d.index.insert(format!("{rp}/g"), synced);
        std::fs::rename(root.join("d"), dir.join("out")).unwrap();
        std::fs::copy(dir.join("secret"), dir.join("out/f")).unwrap();
        std::os::unix::fs::symlink(dir.join("out"), root.join("d")).unwrap();
        std::os::unix::fs::symlink(dir.join("secret"), root.join("g")).unwrap();
        let mut rx = up(&mut d, "aa", 1);
        while rx.try_recv().is_ok() {}
        for path in [format!("{rp}/d/f"), format!("{rp}/g")] {
            d.handle(Event::Peer { id: "aa".into(), conn: 1, msg: Msg::Request { path: path.clone(), hash } });
            assert!(matches!(rx.try_recv(), Ok(Msg::Unavailable { path: p, .. }) if p == path), "{path}");
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn messages_from_a_stale_connection_are_ignored() {
        let mut d = daemon();
        let me = d.me.clone();
        d.cfg.set_node(&me, "aa", "h:1", false);
        let mut rx = up(&mut d, "aa", 1);
        while rx.try_recv().is_ok() {}
        let request = || Msg::Request { path: "~/x".into(), hash: [1; 32] };
        d.handle(Event::Peer { id: "aa".into(), conn: 2, msg: request() });
        assert!(rx.try_recv().is_err());
        d.handle(Event::Peer { id: "aa".into(), conn: 1, msg: request() });
        assert!(matches!(rx.try_recv(), Ok(Msg::Unavailable { .. })));
    }

    #[test]
    fn connected_node_gets_config_then_only_index_entries_in_shared_folders() {
        let mut d = daemon();
        let me = d.me.clone();
        d.cfg.set_node(&me, "aa", "h:1", false);
        d.cfg.set_root(&me, "~/s", true, false);
        d.index.insert("~/s/f".into(), meta(0, 0));
        d.index.insert("~/x".into(), meta(0, 0));
        let mut rx = up(&mut d, "aa", 1);
        assert!(matches!(rx.try_recv(), Ok(Msg::Config(_))));
        let Ok(Msg::Index(entries)) = rx.try_recv() else { panic!("no index sent") };
        assert_eq!(entries.iter().map(|e| e.path.as_str()).collect::<Vec<_>>(), ["~/s/f"]);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn a_config_too_large_for_one_message_is_sent_in_pieces_that_fit() {
        let mut d = daemon();
        let me = d.me.clone();
        d.cfg.set_node(&me, "aa", "h:1", false);
        for i in 0..2000 {
            d.cfg.set_root(&me, &format!("~/{}/{i}", "x".repeat(40)), true, true);
        }
        let received = |rx: &mut mpsc::UnboundedReceiver<Msg>| {
            let mut got = Config::default();
            while let Ok(m) = rx.try_recv() {
                if let Msg::Config(s) = m {
                    let len = postcard::to_stdvec(&Msg::Config(s.clone())).unwrap().len();
                    assert!(len <= net::MAX_MSG, "{len}");
                    got.merge(&s, "zz");
                }
            }
            got.shared
        };
        let mut rx = up(&mut d, "aa", 1);
        let got = received(&mut rx);
        assert_eq!((got.nodes.len(), got.roots.len()), (1, 2000));
        d.on_ctl(&Req::AddPath { path: "~/new".into(), is_dir: true });
        let got = received(&mut rx);
        assert_eq!((got.nodes.len(), got.roots.len()), (1, 2001));
    }

    #[test]
    fn status_counts_only_live_files_in_shared_folders_and_queued_transfers() {
        let mut d = daemon();
        let me = d.me.clone();
        d.cfg.set_root(&me, "~/s", true, false);
        d.index.insert("~/s/a".into(), meta(0, 0));
        d.index.insert("~/s/b".into(), meta(0, 0));
        d.index.insert("~/s/gone".into(), FileMeta { hash: None, ..meta(0, 0) });
        d.index.insert("~/x".into(), meta(0, 0));
        d.queued.insert("~/s/c".into(), ("aa".into(), meta(0, 0), Vec::new()));
        let text = d.on_ctl(&Req::Status).text;
        assert!(text.contains("\nFiles tracked: 2\nTransfers: 0 active, 1 queued\n"), "{text}");
    }

    #[test]
    fn rejected_request_replies_with_the_error_and_changes_nothing() {
        let mut d = daemon();
        let me = d.me.clone();
        d.cfg.set_node(&me, "aa", "h:1", false);
        let mut rx = up(&mut d, "aa", 1);
        while rx.try_recv().is_ok() {}
        let clock = d.cfg.clock;
        let resp = d.on_ctl(&Req::RemovePath { path: "~/x".into() });
        assert_eq!((resp.ok, resp.text.as_str()), (false, "~/x is not synced"));
        let resp = d.on_ctl(&Req::RemoveNode { id: "bb".into() });
        assert_eq!((resp.ok, resp.text.as_str()), (false, "No such node"));
        assert_eq!(d.cfg.clock, clock);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn added_path_is_saved_shared_with_peers_and_its_known_entries_are_sent() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("add-path");
        std::fs::create_dir_all(dir.join("s")).unwrap();
        d.config_file = dir.join("config.json");
        let root = dir.join("s").to_str().unwrap().to_string();
        let known = dir.join("s/gone.txt").to_str().unwrap().to_string();
        d.cfg.set_node(&me, "aa", "h:1", false);
        d.index.insert(known.clone(), FileMeta { hash: None, ..meta(0, 2) });
        d.index.insert(dir.join("x").to_str().unwrap().into(), meta(0, 2));
        let mut rx = up(&mut d, "aa", 1);
        while rx.try_recv().is_ok() {}
        let resp = d.on_ctl(&Req::AddPath { path: root.clone(), is_dir: true });
        assert!(resp.ok, "{}", resp.text);
        assert!(Config::load(&d.config_file).unwrap().active_root(&root).is_some());
        assert!(matches!(rx.try_recv(), Ok(Msg::Config(c)) if c.roots.contains_key(&root)));
        assert_eq!(d.outbox.iter().map(|e| e.path.as_str()).collect::<Vec<_>>(), [known.as_str()]);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn files_in_a_folder_are_recorded_when_a_synced_file_path_is_added_again_as_a_folder() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("file-root-becomes-folder");
        std::fs::create_dir_all(&dir).unwrap();
        d.config_file = dir.join("config.json");
        let root = dir.join("x").to_str().unwrap().to_string();
        std::fs::write(&root, "a").unwrap();
        d.cfg.set_root(&me, &root, false, false);
        d.check_tree(&root);
        std::fs::remove_file(&root).unwrap();
        std::fs::create_dir(&root).unwrap();
        std::fs::write(dir.join("x/f"), "b").unwrap();
        d.check_tree(&root);
        let resp = d.on_ctl(&Req::AddPath { path: root.clone(), is_dir: true });
        assert!(resp.ok, "{}", resp.text);
        let f = dir.join("x/f").to_str().unwrap().to_string();
        assert!(d.index.get(&f).is_some_and(|m| !m.deleted()));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn deletion_of_a_synced_file_is_sent_before_the_files_of_the_folder_that_replaced_it() {
        let mut d = daemon();
        let me = d.me.clone();
        d.cfg.set_node(&me, "aa", "h:1", false);
        let mut rx = up(&mut d, "aa", 1);
        let dir = test_dir("file-replaced-by-folder-order");
        std::fs::create_dir_all(&dir).unwrap();
        let root = dir.join("x").to_str().unwrap().to_string();
        std::fs::write(&root, "a").unwrap();
        d.cfg.set_root(&me, &root, false, false);
        d.check_tree(&root);
        d.outbox.clear();
        std::fs::remove_file(&root).unwrap();
        std::fs::create_dir(&root).unwrap();
        std::fs::write(dir.join("x/f"), "b").unwrap();
        d.cfg.set_root(&me, &root, true, false);
        d.check_tree(&root);
        while rx.try_recv().is_ok() {}
        d.flush();
        let mut sent = Vec::new();
        while let Ok(Msg::Index(b)) = rx.try_recv() {
            sent.extend(b.into_iter().map(|e| e.path));
        }
        assert_eq!(sent, [root.clone(), format!("{root}/f")]);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn peer_config_removing_a_node_drops_its_connection_and_downloads_and_an_old_config_changes_nothing() {
        let mut d = daemon();
        let me = d.me.clone();
        let mut rxs = Vec::new();
        for (id, conn) in [("aa", 1), ("bb", 2)] {
            d.cfg.set_node(&me, id, "h:1", false);
            let rx = up(&mut d, id, conn);
            rxs.push(rx);
        }
        d.queued.insert("~/s/f".into(), ("bb".into(), meta(0, 0), Vec::new()));
        let old = d.cfg.shared.clone();
        let mut theirs = Config { clock: d.cfg.clock, ..Config::default() };
        theirs.set_node("aa", "bb", "h:1", true);
        for rx in &mut rxs {
            while rx.try_recv().is_ok() {}
        }
        d.handle(Event::Peer { id: "aa".into(), conn: 1, msg: Msg::Config(theirs.shared) });
        assert_eq!(d.peers.keys().collect::<Vec<_>>(), ["aa"]);
        assert!(d.queued.is_empty());
        assert!(matches!(rxs[0].try_recv(), Ok(Msg::Config(c)) if c.nodes["bb"].removed));
        d.handle(Event::Peer { id: "aa".into(), conn: 1, msg: Msg::Config(old) });
        assert!(d.cfg.trusted("bb").is_none());
        assert!(rxs[0].try_recv().is_err());
    }

    #[tokio::test]
    async fn node_list_shows_a_peer_shared_node_id_that_is_not_ascii() {
        let mut d = daemon();
        let me = d.me.clone();
        d.cfg.set_node(&me, "aa", "h:1", false);
        let _rx = up(&mut d, "aa", 1);
        let mut theirs = Config { clock: d.cfg.clock, ..Config::default() };
        theirs.set_node("aa", "aéééé", "h:2", false);
        d.handle(Event::Peer { id: "aa".into(), conn: 1, msg: Msg::Config(theirs.shared) });
        let resp = d.on_ctl(&Req::ListNodes);
        assert!(resp.text.contains("AÉÉÉÉ  h:2"), "{}", resp.text);
    }

    #[tokio::test]
    async fn download_refused_by_its_node_is_requested_from_another_node_that_offered_it() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("refused-other-source");
        let root = dir.join("s");
        std::fs::create_dir_all(&root).unwrap();
        let rp = root.to_str().unwrap().to_string();
        d.cfg.set_root(&me, &rp, true, false);
        for id in ["aa", "bb"] {
            d.cfg.set_node(&me, id, "h:1", false);
        }
        let (_a, mut b) = (up(&mut d, "aa", 1), up(&mut d, "bb", 2));
        while b.try_recv().is_ok() {}
        let p = format!("{rp}/f");
        let r = FileMeta { vv: Vv(vec![(1, 1)]), ..meta(0, 1) };
        let hash = r.hash.unwrap();
        for from in ["aa", "bb"] {
            d.on_entry(from, Entry { path: p.clone(), meta: r.clone() });
        }
        d.handle(Event::Peer { id: "aa".into(), conn: 1, msg: Msg::Unavailable { path: p.clone(), hash } });
        d.pump_queue();
        assert!(matches!(b.try_recv(), Ok(Msg::Request { path, .. }) if path == p));
        assert_eq!(d.downloads[&p].from, "bb");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn download_with_wrong_content_is_requested_from_another_node_that_offered_it() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("mismatch-other-source");
        let root = dir.join("s");
        std::fs::create_dir_all(&root).unwrap();
        let rp = root.to_str().unwrap().to_string();
        d.cfg.set_root(&me, &rp, true, false);
        for id in ["aa", "bb"] {
            d.cfg.set_node(&me, id, "h:1", false);
        }
        let (_a, mut b) = (up(&mut d, "aa", 1), up(&mut d, "bb", 2));
        while b.try_recv().is_ok() {}
        let p = format!("{rp}/f");
        let r = FileMeta { vv: Vv(vec![(1, 1)]), ..meta(0, 1) };
        let hash = r.hash.unwrap();
        for from in ["aa", "bb"] {
            d.on_entry(from, Entry { path: p.clone(), meta: r.clone() });
        }
        let data = Msg::Data { path: p.clone(), hash, offset: 0, bytes: b"x".to_vec(), eof: true };
        d.handle(Event::Peer { id: "aa".into(), conn: 1, msg: data });
        d.pump_queue();
        assert!(matches!(b.try_recv(), Ok(Msg::Request { path, .. }) if path == p));
        assert_eq!(d.downloads[&p].from, "bb");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn download_that_grows_past_its_size_is_requested_from_another_node_that_offered_it() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("oversize-other-source");
        let root = dir.join("s");
        std::fs::create_dir_all(&root).unwrap();
        let rp = root.to_str().unwrap().to_string();
        d.cfg.set_root(&me, &rp, true, false);
        for id in ["aa", "bb"] {
            d.cfg.set_node(&me, id, "h:1", false);
        }
        let (_a, mut b) = (up(&mut d, "aa", 1), up(&mut d, "bb", 2));
        while b.try_recv().is_ok() {}
        let p = format!("{rp}/f");
        let r = FileMeta { vv: Vv(vec![(1, 1)]), ..meta(0, 1) };
        let hash = r.hash.unwrap();
        for from in ["aa", "bb"] {
            d.on_entry(from, Entry { path: p.clone(), meta: r.clone() });
        }
        let data = Msg::Data { path: p.clone(), hash, offset: 0, bytes: b"xy".to_vec(), eof: false };
        d.handle(Event::Peer { id: "aa".into(), conn: 1, msg: data });
        d.pump_queue();
        assert!(matches!(b.try_recv(), Ok(Msg::Request { path, .. }) if path == p));
        assert_eq!(d.downloads[&p].from, "bb");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn unavailable_cancels_a_download_only_when_sent_by_its_source_for_the_same_version() {
        let mut d = daemon();
        let me = d.me.clone();
        for (id, conn) in [("aa", 1), ("bb", 2)] {
            d.cfg.set_node(&me, id, "h:1", false);
            up(&mut d, id, conn);
        }
        let tmp = test_dir("unavailable");
        d.downloads.insert("~/s/f".into(), download("aa", meta(0, 0), &tmp));
        let unavailable = |hash| Msg::Unavailable { path: "~/s/f".into(), hash };
        d.handle(Event::Peer { id: "bb".into(), conn: 2, msg: unavailable([1; 32]) });
        assert!(tmp.exists());
        d.handle(Event::Peer { id: "aa".into(), conn: 1, msg: unavailable([2; 32]) });
        assert!(tmp.exists());
        d.handle(Event::Peer { id: "aa".into(), conn: 1, msg: unavailable([1; 32]) });
        assert!(!tmp.exists());
    }

    #[test]
    fn data_is_written_only_from_the_source_and_a_gap_cancels_the_download() {
        let mut d = daemon();
        let me = d.me.clone();
        for (id, conn) in [("aa", 1), ("bb", 2)] {
            d.cfg.set_node(&me, id, "h:1", false);
            up(&mut d, id, conn);
        }
        let tmp = test_dir("data");
        d.downloads.insert("~/s/f".into(), download("aa", FileMeta { size: 5, ..meta(0, 0) }, &tmp));
        let data = |offset, bytes: &[u8]| Msg::Data { path: "~/s/f".into(), hash: [1; 32], offset, bytes: bytes.to_vec(), eof: false };
        d.handle(Event::Peer { id: "bb".into(), conn: 2, msg: data(0, b"xx") });
        d.handle(Event::Peer { id: "aa".into(), conn: 1, msg: data(0, b"abc") });
        assert_eq!(std::fs::read(&tmp).unwrap(), b"abc");
        d.handle(Event::Peer { id: "aa".into(), conn: 1, msg: data(4, b"d") });
        assert!(!tmp.exists());
    }

    #[test]
    fn download_whose_content_does_not_match_its_hash_is_dropped() {
        let mut d = daemon();
        let me = d.me.clone();
        d.cfg.set_node(&me, "aa", "h:1", false);
        up(&mut d, "aa", 1);
        let tmp = test_dir("mismatch");
        d.downloads.insert("~/s/f".into(), download("aa", meta(0, 0), &tmp));
        let msg = Msg::Data { path: "~/s/f".into(), hash: [1; 32], offset: 0, bytes: b"abc".to_vec(), eof: true };
        d.handle(Event::Peer { id: "aa".into(), conn: 1, msg });
        assert!(d.downloads.is_empty());
        assert!(!tmp.exists());
    }

    #[test]
    fn completed_download_is_written_with_the_senders_mtime_and_mode() {
        let mut d = daemon();
        let me = d.me.clone();
        d.cfg.set_node(&me, "aa", "h:1", false);
        up(&mut d, "aa", 1);
        let dir = test_dir("done");
        std::fs::create_dir_all(&dir).unwrap();
        let lp = dir.join("f.sh");
        let p = lp.to_str().unwrap().to_string();
        let mut r = meta(1_000_000_000_000_000_000, 2);
        r.hash = Some(*blake3::hash(b"abc").as_bytes());
        r.size = 3;
        r.mode = 0o755;
        r.vv = Vv(vec![(2, 1)]);
        let tmp = dir.join(".f.sh.rsync-tmp");
        d.downloads.insert(p.clone(), download("aa", r.clone(), &tmp));
        let msg = Msg::Data { path: p.clone(), hash: *blake3::hash(b"abc").as_bytes(), offset: 0, bytes: b"abc".to_vec(), eof: true };
        d.handle(Event::Peer { id: "aa".into(), conn: 1, msg });
        assert_eq!(std::fs::read(&lp).unwrap(), b"abc");
        let s = index::stat(&lp).unwrap();
        assert_eq!((s.mtime, s.mode), (r.mtime, 0o755));
        assert!(!tmp.exists());
        assert_eq!(d.index[&p], r);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn completed_download_with_mtime_before_1970_is_written_with_the_epoch() {
        let mut d = daemon();
        let me = d.me.clone();
        d.cfg.set_node(&me, "aa", "h:1", false);
        up(&mut d, "aa", 1);
        let dir = test_dir("before-epoch");
        std::fs::create_dir_all(&dir).unwrap();
        let lp = dir.join("f");
        let p = lp.to_str().unwrap().to_string();
        let mut r = meta(-5_000_000_000, 2);
        r.hash = Some(*blake3::hash(b"abc").as_bytes());
        r.size = 3;
        r.vv = Vv(vec![(2, 1)]);
        let tmp = dir.join(".f.rsync-tmp");
        d.downloads.insert(p.clone(), download("aa", r, &tmp));
        let msg = Msg::Data { path: p, hash: *blake3::hash(b"abc").as_bytes(), offset: 0, bytes: b"abc".to_vec(), eof: true };
        d.handle(Event::Peer { id: "aa".into(), conn: 1, msg });
        assert_eq!(std::fs::read(&lp).unwrap(), b"abc");
        assert_eq!(index::stat(&lp).unwrap().mtime, 0);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_running_download_is_replaced_only_by_a_newer_version() {
        let mut d = daemon();
        let tmp = test_dir("replace");
        let mut running = meta(0, 0);
        running.vv = Vv(vec![(1, 2)]);
        d.downloads.insert("~/s/f".into(), download("aa", running, &tmp));
        let mut r = meta(0, 0);
        for n in [1, 2] {
            r.vv = Vv(vec![(1, n)]);
            d.start_download("aa", "~/s/f", r.clone());
            assert!(tmp.exists());
            assert!(d.downloads.contains_key("~/s/f"));
        }
        r.vv = Vv(vec![(1, 3)]);
        d.start_download("aa", "~/s/f", r);
        assert!(!tmp.exists());
        assert!(d.downloads.is_empty());
    }

    #[test]
    fn running_download_is_replaced_by_a_concurrent_version_that_wins_the_conflict() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("download-concurrent");
        let root = dir.join("s");
        std::fs::create_dir_all(&root).unwrap();
        let rp = root.to_str().unwrap().to_string();
        d.cfg.set_root(&me, &rp, true, false);
        for id in ["aa", "bb"] {
            d.cfg.set_node(&me, id, "h:1", false);
        }
        let (_a, _b) = (up(&mut d, "aa", 1), up(&mut d, "bb", 2));
        let p = format!("{rp}/f");
        let version = |data: &[u8], mtime, node| FileMeta { hash: Some(*blake3::hash(data).as_bytes()), size: data.len() as u64, vv: Vv(vec![(node, 1)]), ..meta(mtime, node) };
        d.on_entry("aa", Entry { path: p.clone(), meta: version(b"loser", 1_000_000_000, 1) });
        d.on_entry("bb", Entry { path: p.clone(), meta: version(b"winner", 2_000_000_000, 2) });
        d.on_data("aa", &p, *blake3::hash(b"loser").as_bytes(), 0, b"loser", true);
        d.on_data("bb", &p, *blake3::hash(b"winner").as_bytes(), 0, b"winner", true);
        assert_eq!(std::fs::read(root.join("f")).unwrap(), b"winner");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn running_download_replaced_by_a_newer_version_ignores_data_from_the_old_upload() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("download-stale-upload");
        let root = dir.join("s");
        std::fs::create_dir_all(&root).unwrap();
        let rp = root.to_str().unwrap().to_string();
        d.cfg.set_root(&me, &rp, true, false);
        d.cfg.set_node(&me, "aa", "h:1", false);
        let _rx = up(&mut d, "aa", 1);
        let p = format!("{rp}/f");
        let version = |data: &[u8], n| FileMeta { hash: Some(*blake3::hash(data).as_bytes()), size: data.len() as u64, vv: Vv(vec![(2, n)]), ..meta(0, 2) };
        d.on_entry("aa", Entry { path: p.clone(), meta: version(b"old!", 1) });
        d.on_data("aa", &p, *blake3::hash(b"old!").as_bytes(), 0, b"ol", false);
        d.on_entry("aa", Entry { path: p.clone(), meta: version(b"new", 2) });
        d.on_data("aa", &p, *blake3::hash(b"old!").as_bytes(), 2, b"d!", false);
        d.on_data("aa", &p, *blake3::hash(b"new").as_bytes(), 0, b"new", true);
        assert_eq!(std::fs::read(root.join("f")).unwrap(), b"new");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn when_downloads_are_full_the_queue_keeps_the_newest_version() {
        let mut d = daemon();
        let tmp = test_dir("queue");
        for n in 0..MAX_DOWNLOADS {
            d.downloads.insert(format!("~/s/{n}"), download("aa", meta(0, 0), &tmp));
        }
        let queued = |d: &Daemon| d.queued.get("~/s/f").map(|(from, m, _)| (from.clone(), m.vv.clone()));
        let mut r = meta(0, 0);
        for (from, n, want) in [("aa", 2, ("aa", 2)), ("bb", 1, ("aa", 2)), ("bb", 2, ("aa", 2)), ("cc", 3, ("cc", 3))] {
            r.vv = Vv(vec![(1, n)]);
            d.start_download(from, "~/s/f", r.clone());
            assert_eq!(queued(&d), Some((want.0.to_string(), Vv(vec![(1, want.1)]))));
        }
        assert_eq!(d.downloads.len(), MAX_DOWNLOADS);
        std::fs::remove_file(tmp).unwrap();
    }

    #[test]
    fn when_downloads_are_full_a_queued_version_is_not_replaced_by_a_concurrent_loser() {
        let mut d = daemon();
        let tmp = test_dir("queue-concurrent");
        for n in 0..MAX_DOWNLOADS {
            d.downloads.insert(format!("~/s/{n}"), download("aa", meta(0, 0), &tmp));
        }
        let winner = FileMeta { vv: Vv(vec![(1, 1)]), ..meta(2, 1) };
        let loser = FileMeta { hash: Some([2; 32]), vv: Vv(vec![(2, 1)]), ..meta(1, 2) };
        d.start_download("aa", "~/s/f", winner.clone());
        d.start_download("bb", "~/s/f", loser);
        assert_eq!(d.queued.get("~/s/f"), Some(&("aa".to_string(), winner, Vec::new())));
        std::fs::remove_file(tmp).unwrap();
    }

    #[test]
    fn queue_waits_for_a_free_slot_and_drops_files_from_offline_nodes() {
        let mut d = daemon();
        let tmp = test_dir("pump");
        for n in 0..MAX_DOWNLOADS {
            d.downloads.insert(format!("~/s/{n}"), download("aa", meta(0, 0), &tmp));
        }
        d.queued.insert("~/s/f".into(), ("aa".into(), meta(0, 0), Vec::new()));
        d.queued.insert("~/s/g".into(), ("bb".into(), meta(0, 0), Vec::new()));
        d.pump_queue();
        assert_eq!(d.queued.len(), 2);
        d.cancel_download("~/s/0");
        d.pump_queue();
        assert!(d.queued.is_empty());
        assert_eq!(d.downloads.len(), MAX_DOWNLOADS - 1);
        let _ = std::fs::remove_file(tmp);
    }

    #[test]
    fn queued_entry_is_not_downloaded_after_its_folder_stops_being_synced() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("queue-unshared");
        let root = dir.join("s");
        std::fs::create_dir_all(&root).unwrap();
        let rp = root.to_str().unwrap().to_string();
        d.cfg.set_root(&me, &rp, true, false);
        d.cfg.set_node(&me, "aa", "h:1", false);
        let _rx = up(&mut d, "aa", 1);
        for n in 0..MAX_DOWNLOADS {
            d.downloads.insert(format!("~/s/{n}"), download("aa", meta(0, 0), &dir.join(format!("busy{n}"))));
        }
        d.on_entry("aa", Entry { path: format!("{rp}/f"), meta: meta(0, 0) });
        assert_eq!(d.queued.len(), 1);
        d.cfg.set_root(&me, &rp, true, true);
        d.cancel_download("~/s/0");
        d.pump_queue();
        assert!(!root.join(".f.rsync-tmp").exists());
        assert_eq!(d.downloads.len(), MAX_DOWNLOADS - 1);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn running_download_is_dropped_when_a_peer_stops_syncing_its_folder() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("download-unshared");
        let root = dir.join("s");
        std::fs::create_dir_all(&root).unwrap();
        let rp = root.to_str().unwrap().to_string();
        d.cfg.set_root(&me, &rp, true, false);
        d.cfg.set_node(&me, "aa", "h:1", false);
        let _rx = up(&mut d, "aa", 1);
        let p = format!("{rp}/f");
        let data = b"abc";
        let r = FileMeta { hash: Some(*blake3::hash(data).as_bytes()), size: 3, ..meta(0, 0) };
        d.on_entry("aa", Entry { path: p.clone(), meta: r });
        assert!(root.join(".f.rsync-tmp").exists());
        let mut theirs = d.cfg.clone();
        theirs.set_root("aa", &rp, true, true);
        d.on_config(&theirs.shared);
        d.on_data("aa", &p, *blake3::hash(data).as_bytes(), 0, data, true);
        assert!(!root.join("f").exists());
        assert!(!root.join(".f.rsync-tmp").exists());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn running_download_is_dropped_when_a_peer_starts_syncing_its_path_as_a_folder() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("download-becomes-root");
        let root = dir.join("s");
        std::fs::create_dir_all(&root).unwrap();
        let rp = root.to_str().unwrap().to_string();
        d.cfg.set_root(&me, &rp, true, false);
        d.cfg.set_node(&me, "aa", "h:1", false);
        let _rx = up(&mut d, "aa", 1);
        let p = format!("{rp}/f");
        let data = b"abc";
        let r = FileMeta { hash: Some(*blake3::hash(data).as_bytes()), size: 3, ..meta(0, 0) };
        d.on_entry("aa", Entry { path: p.clone(), meta: r });
        assert!(root.join(".f.rsync-tmp").exists());
        let mut theirs = d.cfg.clone();
        theirs.set_root("aa", &p, true, false);
        d.on_config(&theirs.shared);
        d.on_data("aa", &p, *blake3::hash(data).as_bytes(), 0, data, true);
        assert!(root.join("f").is_dir());
        assert!(!root.join(".f.rsync-tmp").exists());
        assert!(!d.index.contains_key(&p));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn running_download_is_not_written_through_a_folder_replaced_by_a_symlink() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("download-symlink");
        let root = dir.join("s");
        std::fs::create_dir_all(root.join("d")).unwrap();
        let rp = root.to_str().unwrap().to_string();
        d.cfg.set_root(&me, &rp, true, false);
        d.cfg.set_node(&me, "aa", "h:1", false);
        let _rx = up(&mut d, "aa", 1);
        let p = format!("{rp}/d/f");
        let data = b"abc";
        let r = FileMeta { hash: Some(*blake3::hash(data).as_bytes()), size: 3, ..meta(0, 0) };
        d.on_entry("aa", Entry { path: p.clone(), meta: r });
        std::fs::rename(root.join("d"), dir.join("out")).unwrap();
        std::os::unix::fs::symlink(dir.join("out"), root.join("d")).unwrap();
        d.on_data("aa", &p, *blake3::hash(data).as_bytes(), 0, data, true);
        assert!(!dir.join("out/f").exists());
        assert!(!dir.join("out/.f.rsync-tmp").exists());
        assert!(d.index.is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn downloaded_conflict_winner_replaces_the_file_and_keeps_the_local_copy() {
        let mut d = daemon();
        let dir = test_dir("conflict");
        std::fs::create_dir_all(&dir).unwrap();
        let lp = dir.join("f.txt");
        let tmp = dir.join("tmp");
        std::fs::write(&lp, "mine").unwrap();
        std::fs::write(&tmp, "theirs").unwrap();
        let mut local = meta(0, 1);
        local.vv = Vv(vec![(1, 1)]);
        d.index.insert("~/s/f.txt".into(), local.clone());
        let mut r = meta(1, 2);
        r.hash = Some([2; 32]);
        r.vv = Vv(vec![(2, 1)]);
        d.apply("aa", "~/s/f.txt", &lp, r.clone(), Some(tmp.clone()));
        assert_eq!(std::fs::read_to_string(&lp).unwrap(), "theirs");
        assert_eq!(std::fs::read_to_string(conflict_name(&lp, &local)).unwrap(), "mine");
        assert!(!tmp.exists());
        assert_eq!(d.index["~/s/f.txt"].hash, r.hash);
        assert_eq!(d.outbox.len(), 1);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn downloaded_conflict_winner_that_cannot_be_moved_into_place_leaves_the_local_file_where_it_was() {
        let mut d = daemon();
        let dir = test_dir("conflict-unwritable");
        std::fs::create_dir_all(&dir).unwrap();
        let lp = dir.join("f.txt");
        std::fs::write(&lp, "mine").unwrap();
        let mut local = meta(0, 1);
        local.vv = Vv(vec![(1, 1)]);
        d.index.insert("~/s/f.txt".into(), local.clone());
        let mut r = meta(1, 2);
        r.hash = Some([2; 32]);
        r.vv = Vv(vec![(2, 1)]);
        d.apply("aa", "~/s/f.txt", &lp, r, Some(dir.join("gone")));
        assert_eq!(std::fs::read_to_string(&lp).unwrap(), "mine");
        assert!(!conflict_name(&lp, &local).exists());
        assert_eq!(d.index["~/s/f.txt"], local);
        assert!(d.outbox.is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn downloaded_conflict_winner_is_dropped_when_the_local_version_cannot_be_kept_as_a_copy() {
        let mut d = daemon();
        let dir = test_dir("conflict-copy-blocked");
        std::fs::create_dir_all(&dir).unwrap();
        let (lp, tmp) = (dir.join("f.txt"), dir.join(".f.txt.rsync-tmp"));
        std::fs::write(&lp, "mine").unwrap();
        std::fs::write(&tmp, "theirs").unwrap();
        let mut local = meta(0, 1);
        local.vv = Vv(vec![(1, 1)]);
        d.index.insert("~/s/f.txt".into(), local.clone());
        let copy = conflict_name(&lp, &local);
        std::fs::create_dir_all(copy.join("sub")).unwrap();
        let mut r = meta(1, 2);
        r.hash = Some([2; 32]);
        r.vv = Vv(vec![(2, 1)]);
        d.apply("aa", "~/s/f.txt", &lp, r, Some(tmp.clone()));
        assert_eq!(std::fs::read_to_string(&lp).unwrap(), "mine");
        assert!(copy.join("sub").is_dir());
        assert!(!tmp.exists());
        assert_eq!(d.index["~/s/f.txt"], local);
        assert!(d.outbox.is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn downloaded_conflict_winner_with_the_longest_allowed_name_keeps_the_local_copy() {
        let mut d = daemon();
        let dir = test_dir("conflict-long-name");
        std::fs::create_dir_all(&dir).unwrap();
        let lp = dir.join(format!("{}.txt", "n".repeat(251)));
        let tmp = dir.join("tmp");
        std::fs::write(&lp, "mine").unwrap();
        std::fs::write(&tmp, "theirs").unwrap();
        let mut local = meta(0, 1);
        local.vv = Vv(vec![(1, 1)]);
        d.index.insert("~/s/f.txt".into(), local);
        let mut r = meta(1, 2);
        r.hash = Some([2; 32]);
        r.vv = Vv(vec![(2, 1)]);
        d.apply("aa", "~/s/f.txt", &lp, r.clone(), Some(tmp.clone()));
        assert_eq!(std::fs::read_to_string(&lp).unwrap(), "theirs");
        let copies: Vec<_> = std::fs::read_dir(&dir).unwrap().map(|e| e.unwrap().path()).filter(|p| *p != lp).collect();
        assert_eq!(copies.len(), 1);
        assert_eq!(std::fs::read_to_string(&copies[0]).unwrap(), "mine");
        assert_eq!(d.index["~/s/f.txt"].hash, r.hash);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn downloaded_conflict_winner_is_sent_on_as_newer_than_both_versions() {
        let mut d = daemon();
        let dir = test_dir("conflict-history");
        std::fs::create_dir_all(&dir).unwrap();
        let (lp, tmp) = (dir.join("f.txt"), dir.join(".f.txt.rsync-tmp"));
        std::fs::write(&lp, "mine").unwrap();
        std::fs::write(&tmp, "theirs").unwrap();
        let mut local = meta(0, 1);
        local.vv = Vv(vec![(1, 3)]);
        d.index.insert("~/s/f.txt".into(), local.clone());
        let mut r = meta(1, 2);
        r.hash = Some([2; 32]);
        r.vv = Vv(vec![(2, 1)]);
        d.apply("aa", "~/s/f.txt", &lp, r.clone(), Some(tmp));
        let sent = &d.outbox[0].meta;
        assert_eq!(index::decide(Some(&local), sent), Action::Take);
        assert_eq!(index::decide(Some(&r), sent), Action::Take);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn downloaded_change_that_beats_a_local_deletion_restores_the_file() {
        let mut d = daemon();
        let dir = test_dir("restore");
        std::fs::create_dir_all(&dir).unwrap();
        let (lp, tmp) = (dir.join("f.txt"), dir.join(".f.txt.rsync-tmp"));
        std::fs::write(&tmp, "theirs").unwrap();
        let mut local = meta(9, 1);
        (local.hash, local.vv) = (None, Vv(vec![(1, 1)]));
        d.index.insert("~/s/f.txt".into(), local);
        let mut r = meta(5, 2);
        r.vv = Vv(vec![(2, 1)]);
        d.apply("aa", "~/s/f.txt", &lp, r.clone(), Some(tmp));
        assert_eq!(std::fs::read_to_string(&lp).unwrap(), "theirs");
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1);
        r.vv = Vv(vec![(1, 1), (2, 1)]);
        assert_eq!(d.index["~/s/f.txt"], r);
        assert_eq!(d.outbox.len(), 1);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn newer_remote_deletion_removes_the_local_file() {
        let mut d = daemon();
        let dir = test_dir("delete");
        std::fs::create_dir_all(&dir).unwrap();
        let lp = dir.join("f.txt");
        std::fs::write(&lp, "mine").unwrap();
        let mut local = meta(0, 1);
        local.vv = Vv(vec![(1, 1)]);
        d.index.insert("~/s/f.txt".into(), local);
        let mut r = meta(1, 1);
        r.hash = None;
        r.vv = Vv(vec![(1, 2)]);
        d.apply("aa", "~/s/f.txt", &lp, r, None);
        assert!(!lp.exists());
        assert!(d.index["~/s/f.txt"].deleted());
        assert_eq!(d.outbox.len(), 1);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn newer_remote_deletion_of_a_file_already_gone_is_still_recorded() {
        let mut d = daemon();
        let lp = Path::new("/nonexistent/f.txt");
        let mut local = meta(0, 1);
        local.vv = Vv(vec![(1, 1)]);
        d.index.insert("~/s/f.txt".into(), local);
        let mut r = meta(1, 1);
        r.hash = None;
        r.vv = Vv(vec![(1, 2)]);
        d.apply("aa", "~/s/f.txt", lp, r, None);
        assert!(d.index["~/s/f.txt"].deleted());
        assert_eq!(d.outbox.len(), 1);
    }

    #[test]
    fn newer_remote_deletion_that_cannot_remove_the_local_file_is_not_recorded() {
        let mut d = daemon();
        let dir = test_dir("delete-locked");
        std::fs::create_dir_all(&dir).unwrap();
        let lp = dir.join("f.txt");
        std::fs::write(&lp, "mine").unwrap();
        let mut local = meta(0, 1);
        local.vv = Vv(vec![(1, 1)]);
        d.index.insert("~/s/f.txt".into(), local.clone());
        let mut r = meta(1, 1);
        r.hash = None;
        r.vv = Vv(vec![(1, 2)]);
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555)).unwrap();
        d.apply("aa", "~/s/f.txt", &lp, r, None);
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert_eq!(std::fs::read_to_string(&lp).unwrap(), "mine");
        assert_eq!(d.index["~/s/f.txt"], local);
        assert!(d.outbox.is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn download_that_cannot_be_moved_into_place_is_dropped_and_not_recorded() {
        let mut d = daemon();
        let dir = test_dir("unwritable");
        let (lp, tmp) = (dir.join("f.txt"), dir.join(".f.txt.rsync-tmp"));
        std::fs::create_dir_all(lp.join("sub")).unwrap();
        std::fs::write(&tmp, "theirs").unwrap();
        d.apply("aa", "~/s/f.txt", &lp, meta(1, 2), Some(tmp.clone()));
        assert!(!tmp.exists());
        assert!(lp.is_dir());
        assert!(d.index.is_empty());
        assert!(d.outbox.is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn downloaded_change_that_beats_a_local_deletion_does_not_move_a_folder_now_at_its_path() {
        let mut d = daemon();
        let dir = test_dir("deleted-became-folder");
        let (lp, tmp) = (dir.join("f.txt"), dir.join(".f.txt.rsync-tmp"));
        std::fs::create_dir_all(&lp).unwrap();
        std::fs::write(lp.join("a"), "mine").unwrap();
        std::fs::write(&tmp, "theirs").unwrap();
        let mut local = meta(5, 1);
        (local.hash, local.vv) = (None, Vv(vec![(1, 1)]));
        d.index.insert("~/s/f.txt".into(), local.clone());
        let mut r = meta(1, 2);
        r.vv = Vv(vec![(2, 1)]);
        d.apply("aa", "~/s/f.txt", &lp, r, Some(tmp.clone()));
        assert_eq!(std::fs::read_to_string(lp.join("a")).unwrap(), "mine");
        assert!(!conflict_name(&lp, &local).exists());
        assert!(!tmp.exists());
        assert_eq!(d.index["~/s/f.txt"], local);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn same_content_from_remote_merges_history_and_keeps_local_size_and_mtime() {
        let mut d = daemon();
        let lp = Path::new("/nonexistent/f.txt");
        for (remote_vv, want) in [(vec![(2, 1)], vec![(1, 1), (2, 1)]), (vec![(1, 2)], vec![(1, 2)])] {
            let mut local = meta(5, 1);
            local.vv = Vv(vec![(1, 1)]);
            d.index.insert("~/s/f.txt".into(), local);
            d.outbox.clear();
            let mut r = meta(9, 2);
            r.size = 7;
            r.vv = Vv(remote_vv);
            d.apply("aa", "~/s/f.txt", lp, r, None);
            let got = &d.index["~/s/f.txt"];
            assert_eq!(got.vv, Vv(want));
            assert_eq!((got.size, got.mtime), (1, 5));
            assert_eq!(d.outbox.len(), 1);
            assert!(d.downloads.is_empty());
        }
    }

    #[test]
    fn newer_remote_deletion_of_a_file_already_deleted_is_taken_as_is() {
        let mut d = daemon();
        let mut local = meta(5, 1);
        (local.hash, local.size, local.vv) = (None, 0, Vv(vec![(1, 1)]));
        d.index.insert("~/s/f.txt".into(), local);
        let mut r = meta(9, 2);
        (r.hash, r.size, r.vv) = (None, 3, Vv(vec![(1, 2)]));
        d.apply("aa", "~/s/f.txt", Path::new("/nonexistent/f.txt"), r.clone(), None);
        assert_eq!(d.index["~/s/f.txt"], r);
        assert_eq!(d.outbox.len(), 1);
    }

    #[test]
    fn same_content_with_a_new_mode_from_remote_changes_only_the_permissions() {
        let mut d = daemon();
        let dir = test_dir("mode");
        std::fs::create_dir_all(&dir).unwrap();
        let lp = dir.join("f.sh");
        std::fs::write(&lp, "mine").unwrap();
        std::fs::set_permissions(&lp, std::fs::Permissions::from_mode(0o644)).unwrap();
        let mut local = meta(0, 1);
        local.vv = Vv(vec![(1, 1)]);
        d.index.insert("~/s/f.sh".into(), local);
        let mut r = meta(0, 1);
        r.mode = 0o755;
        r.vv = Vv(vec![(1, 2)]);
        d.apply("aa", "~/s/f.sh", &lp, r, None);
        assert_eq!(std::fs::metadata(&lp).unwrap().permissions().mode() & 0o777, 0o755);
        assert_eq!(std::fs::read_to_string(&lp).unwrap(), "mine");
        assert_eq!(d.index["~/s/f.sh"].mode, 0o755);
        assert!(d.downloads.is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn concurrent_same_content_with_another_mode_keeps_the_mode_of_the_conflict_winner() {
        let dir = test_dir("concurrent-mode");
        std::fs::create_dir_all(&dir).unwrap();
        let lp = dir.join("f.sh");
        std::fs::write(&lp, "same").unwrap();
        for (local_mtime, remote_mtime, want) in [(9, 5, 0o755), (5, 9, 0o644)] {
            let mut d = daemon();
            std::fs::set_permissions(&lp, std::fs::Permissions::from_mode(0o755)).unwrap();
            let mut local = meta(local_mtime, 1);
            (local.mode, local.vv) = (0o755, Vv(vec![(1, 1)]));
            d.index.insert("~/s/f.sh".into(), local);
            let mut r = meta(remote_mtime, 2);
            r.vv = Vv(vec![(2, 1)]);
            d.apply("aa", "~/s/f.sh", &lp, r, None);
            assert_eq!(std::fs::metadata(&lp).unwrap().permissions().mode() & 0o777, want);
            assert_eq!(d.index["~/s/f.sh"].mode, want);
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn downloaded_conflict_loser_changes_nothing_and_removes_the_temp_file() {
        let mut d = daemon();
        let dir = test_dir("local-wins");
        std::fs::create_dir_all(&dir).unwrap();
        let (lp, tmp) = (dir.join("f.txt"), dir.join(".f.txt.rsync-tmp"));
        std::fs::write(&lp, "mine").unwrap();
        std::fs::write(&tmp, "theirs").unwrap();
        let mut local = meta(9, 1);
        local.vv = Vv(vec![(1, 1)]);
        d.index.insert("~/s/f.txt".into(), local.clone());
        let mut r = meta(5, 2);
        r.hash = Some([2; 32]);
        r.vv = Vv(vec![(2, 1)]);
        d.apply("aa", "~/s/f.txt", &lp, r, Some(tmp.clone()));
        assert_eq!(std::fs::read_to_string(&lp).unwrap(), "mine");
        assert!(!tmp.exists());
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1);
        assert_eq!(d.index["~/s/f.txt"], local);
        assert!(d.outbox.is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn local_file_changes_are_recorded_but_a_touch_is_not_a_new_version() {
        let mut d = daemon();
        let me = d.me_short;
        let dir = test_dir("check");
        std::fs::create_dir_all(&dir).unwrap();
        let lp = dir.join("f.txt");
        let p = "~/s/f.txt";
        d.check_file(p, &lp);
        assert!(d.index.is_empty());
        std::fs::write(&lp, "one").unwrap();
        d.check_file(p, &lp);
        assert_eq!(d.index[p].vv, Vv(vec![(me, 1)]));
        assert_eq!(d.index[p].hash, Some(index::hash_file(&lp).unwrap()));
        assert_eq!(d.outbox.len(), 1);
        d.check_file(p, &lp);
        assert_eq!(d.outbox.len(), 1);
        let f = std::fs::File::options().write(true).open(&lp).unwrap();
        f.set_modified(std::time::SystemTime::UNIX_EPOCH).unwrap();
        d.check_file(p, &lp);
        assert_eq!((d.index[p].mtime, d.index[p].vv.clone()), (0, Vv(vec![(me, 1)])));
        assert_eq!(d.outbox.len(), 1);
        std::fs::write(&lp, "two").unwrap();
        d.check_file(p, &lp);
        assert_eq!(d.index[p].vv, Vv(vec![(me, 2)]));
        assert_eq!(d.index[p].size, 3);
        assert_eq!(d.outbox.len(), 2);
        std::fs::remove_file(&lp).unwrap();
        d.check_file(p, &lp);
        assert!(d.index[p].deleted());
        assert_eq!(d.index[p].vv, Vv(vec![(me, 3)]));
        assert_eq!(d.outbox.len(), 3);
        d.check_file(p, &lp);
        assert_eq!(d.outbox.len(), 3);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn file_recreated_with_the_same_stat_as_its_deletion_is_a_new_version() {
        let mut d = daemon();
        let me = d.me_short;
        let dir = test_dir("recreate");
        std::fs::create_dir_all(&dir).unwrap();
        let lp = dir.join("f.txt");
        let p = "~/s/f.txt";
        let f = std::fs::File::create(&lp).unwrap();
        f.set_modified(std::time::SystemTime::UNIX_EPOCH).unwrap();
        std::fs::set_permissions(&lp, std::fs::Permissions::from_mode(0o644)).unwrap();
        let gone = FileMeta { hash: None, size: 0, mtime: 0, mode: 0o644, vv: Vv(vec![(me, 1)]), by: me };
        d.index.insert(p.into(), gone);
        d.check_file(p, &lp);
        assert_eq!(d.index[p].hash, Some(*blake3::hash(b"").as_bytes()));
        assert_eq!(d.index[p].vv, Vv(vec![(me, 2)]));
        assert_eq!(d.outbox.len(), 1);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn local_permission_change_is_a_new_version() {
        let mut d = daemon();
        let me = d.me_short;
        let dir = test_dir("chmod");
        std::fs::create_dir_all(&dir).unwrap();
        let lp = dir.join("f.sh");
        let p = "~/s/f.sh";
        std::fs::write(&lp, "x").unwrap();
        std::fs::set_permissions(&lp, std::fs::Permissions::from_mode(0o644)).unwrap();
        d.check_file(p, &lp);
        std::fs::set_permissions(&lp, std::fs::Permissions::from_mode(0o755)).unwrap();
        d.check_file(p, &lp);
        assert_eq!(d.index[p].vv, Vv(vec![(me, 2)]));
        assert_eq!(d.index[p].mode, 0o755);
        assert_eq!(d.outbox.len(), 2);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn remote_entries_outside_roots_or_for_ignored_names_are_skipped_and_unseen_local_changes_count() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("entry");
        let root = dir.join("in");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(dir.join("out")).unwrap();
        d.cfg.set_root(&me, root.to_str().unwrap(), true, false);
        let mut gone = meta(0, 2);
        gone.hash = None;
        gone.vv = Vv(vec![(2, 1)]);
        for lp in [dir.join("out/f.txt"), root.join(".DS_Store"), root.join("f.txt")] {
            std::fs::write(&lp, "x").unwrap();
            d.on_entry("aa", Entry { path: lp.to_str().unwrap().into(), meta: gone.clone() });
            assert!(lp.exists(), "{}", lp.display());
        }
        let p = root.join("f.txt").to_str().unwrap().to_string();
        assert_eq!(d.index.keys().collect::<Vec<_>>(), [&p]);
        assert_eq!(d.index[&p].vv, Vv(vec![(d.me_short, 1)]));
        assert_eq!(d.outbox.len(), 1);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn remote_entries_through_a_symlinked_folder_are_skipped() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("entry-symlink");
        let root = dir.join("in");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(dir.join("out")).unwrap();
        std::os::unix::fs::symlink(dir.join("out"), root.join("link")).unwrap();
        d.cfg.set_root(&me, root.to_str().unwrap(), true, false);
        std::fs::write(dir.join("out/f.txt"), "x").unwrap();
        let mut gone = meta(0, 2);
        gone.hash = None;
        gone.vv = Vv(vec![(2, 1), (d.me_short, 1)]);
        d.on_entry("aa", Entry { path: root.join("link/f.txt").to_str().unwrap().into(), meta: gone });
        assert!(dir.join("out/f.txt").exists());
        assert!(d.index.is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn synced_folder_replaced_by_a_symlink_is_recorded_as_deleted() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("folder-to-symlink");
        let root = dir.join("in");
        std::fs::create_dir_all(root.join("d")).unwrap();
        std::fs::write(root.join("d/f.txt"), "x").unwrap();
        let rp = root.to_str().unwrap().to_string();
        d.cfg.set_root(&me, &rp, true, false);
        d.check_tree(&rp);
        std::fs::remove_dir_all(root.join("d")).unwrap();
        std::fs::create_dir_all(dir.join("out")).unwrap();
        std::fs::write(dir.join("out/f.txt"), "secret").unwrap();
        std::os::unix::fs::symlink(dir.join("out"), root.join("d")).unwrap();
        d.check_tree(&rp);
        assert!(d.index[&format!("{rp}/d/f.txt")].deleted());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn files_in_a_folder_that_replaced_a_synced_file_are_not_recorded() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("file-to-folder");
        std::fs::create_dir_all(dir.join("f")).unwrap();
        std::fs::write(dir.join("f/x.txt"), "x").unwrap();
        let fp = dir.join("f").to_str().unwrap().to_string();
        d.cfg.set_root(&me, &fp, false, false);
        d.check_tree(&fp);
        assert!(d.index.is_empty());
        assert!(d.outbox.is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_file_that_replaced_a_synced_folder_is_not_recorded() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("folder-to-file");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("f"), "x").unwrap();
        let fp = dir.join("f").to_str().unwrap().to_string();
        d.cfg.set_root(&me, &fp, true, false);
        d.check_tree(&fp);
        assert!(d.index.is_empty());
        assert!(d.outbox.is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn files_in_a_folder_that_cannot_be_opened_are_not_recorded_as_deleted() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("folder-no-access");
        let root = dir.join("in");
        std::fs::create_dir_all(root.join("d")).unwrap();
        std::fs::write(root.join("d/f.txt"), "x").unwrap();
        let rp = root.to_str().unwrap().to_string();
        d.cfg.set_root(&me, &rp, true, false);
        d.check_tree(&rp);
        d.outbox.clear();
        std::fs::set_permissions(root.join("d"), std::fs::Permissions::from_mode(0o000)).unwrap();
        d.check_tree(&rp);
        std::fs::set_permissions(root.join("d"), std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(!d.index[&format!("{rp}/d/f.txt")].deleted());
        assert!(d.outbox.is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn remote_deletion_does_not_remove_a_local_symlink_that_was_never_synced() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("delete-symlink");
        let root = dir.join("s");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(dir.join("target.txt"), "x").unwrap();
        std::os::unix::fs::symlink(dir.join("target.txt"), root.join("f.txt")).unwrap();
        d.cfg.set_root(&me, root.to_str().unwrap(), true, false);
        let mut gone = meta(0, 2);
        gone.hash = None;
        gone.vv = Vv(vec![(2, 1)]);
        d.on_entry("aa", Entry { path: root.join("f.txt").to_str().unwrap().into(), meta: gone });
        assert!(std::fs::symlink_metadata(root.join("f.txt")).unwrap().is_symlink());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn remote_entry_for_a_folder_root_itself_is_skipped() {
        let mut d = daemon();
        let me = d.me.clone();
        d.cfg.set_node(&me, "aa", "h:1", false);
        up(&mut d, "aa", 1);
        let dir = test_dir("entry-root");
        std::fs::create_dir_all(&dir).unwrap();
        let root = dir.join("s");
        let p = root.to_str().unwrap().to_string();
        d.cfg.set_root(&me, &p, true, false);
        let mut r = meta(0, 2);
        r.hash = Some(*blake3::hash(b"abc").as_bytes());
        r.size = 3;
        r.vv = Vv(vec![(2, 1)]);
        d.on_entry("aa", Entry { path: p.clone(), meta: r });
        let msg = Msg::Data { path: p.clone(), hash: *blake3::hash(b"abc").as_bytes(), offset: 0, bytes: b"abc".to_vec(), eof: true };
        d.handle(Event::Peer { id: "aa".into(), conn: 1, msg });
        assert!(!root.is_file());
        assert!(!dir.join(".s.rsync-tmp").exists());
        assert!(d.index.is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn remote_deletion_of_a_synced_file_that_became_a_folder_removes_the_local_file() {
        let mut d = daemon();
        let me = d.me.clone();
        d.cfg.set_node(&me, "aa", "h:1", false);
        up(&mut d, "aa", 1);
        let dir = test_dir("file-became-folder");
        std::fs::create_dir_all(&dir).unwrap();
        let root = dir.join("s");
        std::fs::write(&root, "x").unwrap();
        let p = root.to_str().unwrap().to_string();
        d.cfg.set_root(&me, &p, false, false);
        d.check_tree(&p);
        d.cfg.set_root(&me, &p, true, false);
        let mut r = d.index[&p].clone();
        r.hash = None;
        r.vv = r.vv.bumped(2);
        d.on_entry("aa", Entry { path: p.clone(), meta: r });
        assert!(!root.exists());
        assert!(d.index[&p].deleted());
        let fp = format!("{p}/f");
        let mut f = meta(0, 2);
        f.vv = Vv(vec![(2, 1)]);
        d.on_entry("aa", Entry { path: fp.clone(), meta: f });
        assert!(d.downloads.contains_key(&fp));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn remote_deletion_below_a_synced_folder_that_became_a_file_removes_the_local_file() {
        let mut d = daemon();
        let me = d.me.clone();
        d.cfg.set_node(&me, "aa", "h:1", false);
        up(&mut d, "aa", 1);
        let dir = test_dir("folder-became-file");
        let root = dir.join("s");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("f"), "x").unwrap();
        let p = root.to_str().unwrap().to_string();
        d.cfg.set_root(&me, &p, true, false);
        d.check_tree(&p);
        d.cfg.set_root(&me, &p, false, false);
        let fp = format!("{p}/f");
        let mut r = d.index[&fp].clone();
        r.hash = None;
        r.vv = r.vv.bumped(2);
        d.on_entry("aa", Entry { path: fp.clone(), meta: r });
        assert!(!root.join("f").exists());
        assert!(d.index[&fp].deleted());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn remote_deletion_of_the_last_file_in_a_folder_removes_the_empty_folders_but_not_the_root() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("deletion-empties-folders");
        let root = dir.join("s");
        std::fs::create_dir_all(root.join("a/b")).unwrap();
        std::fs::create_dir_all(root.join("c")).unwrap();
        std::fs::write(root.join("a/b/f"), "x").unwrap();
        std::fs::write(root.join("c/g"), "y").unwrap();
        let p = root.to_str().unwrap().to_string();
        d.cfg.set_root(&me, &p, true, false);
        d.check_tree(&p);
        for f in ["a/b/f", "c/g"] {
            let fp = format!("{p}/{f}");
            let mut r = d.index[&fp].clone();
            r.hash = None;
            r.vv = r.vv.bumped(2);
            d.on_entry("aa", Entry { path: fp, meta: r });
        }
        assert!(!root.join("a").exists());
        assert!(!root.join("c").exists());
        assert!(root.is_dir());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn download_dropped_after_a_remote_deletion_removes_the_folders_it_created() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("dropped-download-folders");
        let root = dir.join("s");
        std::fs::create_dir_all(&root).unwrap();
        let rp = root.to_str().unwrap().to_string();
        d.cfg.set_root(&me, &rp, true, false);
        d.cfg.set_node(&me, "aa", "h:1", false);
        let mut a = up(&mut d, "aa", 1);
        while a.try_recv().is_ok() {}
        let p = format!("{rp}/a/b/f");
        let hash = *blake3::hash(b"abc").as_bytes();
        let r = FileMeta { hash: Some(hash), size: 3, vv: Vv(vec![(1, 1)]), ..meta(0, 1) };
        d.on_entry("aa", Entry { path: p.clone(), meta: r.clone() });
        assert!(root.join("a/b").is_dir());
        d.on_entry("aa", Entry { path: p.clone(), meta: FileMeta { hash: None, size: 0, vv: Vv(vec![(1, 2)]), ..r } });
        let msg = Msg::Data { path: p, hash, offset: 0, bytes: b"abc".to_vec(), eof: true };
        d.handle(Event::Peer { id: "aa".into(), conn: 1, msg });
        assert!(!root.join("a").exists());
        assert!(root.is_dir());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn download_refused_with_no_other_source_removes_the_folders_it_created() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("refused-download-folders");
        let root = dir.join("s");
        std::fs::create_dir_all(&root).unwrap();
        let rp = root.to_str().unwrap().to_string();
        d.cfg.set_root(&me, &rp, true, false);
        d.cfg.set_node(&me, "aa", "h:1", false);
        let _a = up(&mut d, "aa", 1);
        let p = format!("{rp}/a/b/f");
        let r = FileMeta { vv: Vv(vec![(1, 1)]), ..meta(0, 1) };
        let hash = r.hash.unwrap();
        d.on_entry("aa", Entry { path: p.clone(), meta: r });
        assert!(root.join("a/b").is_dir());
        d.handle(Event::Peer { id: "aa".into(), conn: 1, msg: Msg::Unavailable { path: p, hash } });
        assert!(!root.join("a").exists());
        assert!(root.is_dir());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn remote_deletion_of_the_last_file_in_a_nested_synced_folder_keeps_that_folder() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("deletion-keeps-nested-root");
        let root = dir.join("s");
        std::fs::create_dir_all(root.join("n")).unwrap();
        std::fs::write(root.join("n/f"), "x").unwrap();
        let p = root.to_str().unwrap().to_string();
        let inner = format!("{p}/n");
        d.cfg.set_root(&me, &p, true, false);
        d.cfg.set_root(&me, &inner, true, false);
        d.check_tree(&p);
        let fp = format!("{inner}/f");
        let mut r = d.index[&fp].clone();
        r.hash = None;
        r.vv = r.vv.bumped(2);
        d.on_entry("aa", Entry { path: fp, meta: r });
        assert!(!root.join("n/f").exists());
        assert!(root.join("n").is_dir());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn remote_deletion_keeps_a_folder_that_still_has_files() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("deletion-keeps-folder");
        let root = dir.join("s");
        std::fs::create_dir_all(root.join("a")).unwrap();
        std::fs::write(root.join("a/f"), "x").unwrap();
        std::fs::write(root.join("a/other"), "y").unwrap();
        let p = root.to_str().unwrap().to_string();
        d.cfg.set_root(&me, &p, true, false);
        d.check_tree(&p);
        let fp = format!("{p}/a/f");
        let mut r = d.index[&fp].clone();
        r.hash = None;
        r.vv = r.vv.bumped(2);
        d.on_entry("aa", Entry { path: fp, meta: r });
        assert!(!root.join("a/f").exists());
        assert!(root.join("a/other").exists());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn downloaded_file_replaces_the_empty_folder_left_by_a_synced_folder_that_became_a_file() {
        let mut d = daemon();
        let me = d.me.clone();
        d.cfg.set_node(&me, "aa", "h:1", false);
        up(&mut d, "aa", 1);
        let dir = test_dir("folder-became-file-download");
        let root = dir.join("s");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("f"), "x").unwrap();
        let p = root.to_str().unwrap().to_string();
        d.cfg.set_root(&me, &p, true, false);
        d.check_tree(&p);
        d.cfg.set_root(&me, &p, false, false);
        let mut r = meta(0, 2);
        r.hash = Some(*blake3::hash(b"abc").as_bytes());
        r.size = 3;
        r.vv = Vv(vec![(2, 1)]);
        d.on_entry("aa", Entry { path: p.clone(), meta: r.clone() });
        let fp = format!("{p}/f");
        let mut del = d.index[&fp].clone();
        del.hash = None;
        del.vv = del.vv.bumped(2);
        d.on_entry("aa", Entry { path: fp, meta: del });
        let msg = Msg::Data { path: p.clone(), hash: *blake3::hash(b"abc").as_bytes(), offset: 0, bytes: b"abc".to_vec(), eof: true };
        d.handle(Event::Peer { id: "aa".into(), conn: 1, msg });
        assert_eq!(std::fs::read(&root).unwrap(), b"abc");
        assert_eq!(d.index[&p], r);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn remote_entry_below_a_file_root_is_skipped() {
        let mut d = daemon();
        let me = d.me.clone();
        d.cfg.set_node(&me, "aa", "h:1", false);
        up(&mut d, "aa", 1);
        let dir = test_dir("entry-below-file-root");
        std::fs::create_dir_all(&dir).unwrap();
        let root = dir.join("f");
        d.cfg.set_root(&me, root.to_str().unwrap(), false, false);
        let p = root.join("x").to_str().unwrap().to_string();
        let mut r = meta(0, 2);
        r.hash = Some(*blake3::hash(b"abc").as_bytes());
        r.size = 3;
        r.vv = Vv(vec![(2, 1)]);
        d.on_entry("aa", Entry { path: p.clone(), meta: r });
        let msg = Msg::Data { path: p.clone(), hash: *blake3::hash(b"abc").as_bytes(), offset: 0, bytes: b"abc".to_vec(), eof: true };
        d.handle(Event::Peer { id: "aa".into(), conn: 1, msg });
        assert!(!root.exists());
        assert!(d.index.is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn remote_file_is_written_without_setuid_setgid_or_sticky_bits() {
        let mut d = daemon();
        let me = d.me.clone();
        d.cfg.set_node(&me, "aa", "h:1", false);
        up(&mut d, "aa", 1);
        let dir = test_dir("entry-setuid");
        std::fs::create_dir_all(&dir).unwrap();
        d.cfg.set_root(&me, dir.to_str().unwrap(), true, false);
        let p = dir.join("f.sh").to_str().unwrap().to_string();
        let mut r = meta(0, 2);
        r.hash = Some(*blake3::hash(b"abc").as_bytes());
        r.size = 3;
        r.mode = 0o7755;
        r.vv = Vv(vec![(2, 1)]);
        d.on_entry("aa", Entry { path: p.clone(), meta: r });
        let msg = Msg::Data { path: p.clone(), hash: *blake3::hash(b"abc").as_bytes(), offset: 0, bytes: b"abc".to_vec(), eof: true };
        d.handle(Event::Peer { id: "aa".into(), conn: 1, msg });
        let mode = std::fs::metadata(&p).unwrap().permissions().mode();
        assert_eq!(mode & 0o7777, 0o755);
        d.outbox.clear();
        d.check_tree(&p);
        assert!(d.outbox.is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn download_is_cancelled_when_a_peer_sends_more_bytes_than_the_file_size() {
        let mut d = daemon();
        let me = d.me.clone();
        d.cfg.set_node(&me, "aa", "h:1", false);
        up(&mut d, "aa", 1);
        let dir = test_dir("entry-oversize");
        std::fs::create_dir_all(&dir).unwrap();
        d.cfg.set_root(&me, dir.to_str().unwrap(), true, false);
        let p = dir.join("f").to_str().unwrap().to_string();
        let mut r = meta(0, 2);
        r.hash = Some(*blake3::hash(b"abc").as_bytes());
        r.size = 3;
        r.vv = Vv(vec![(2, 1)]);
        d.on_entry("aa", Entry { path: p.clone(), meta: r });
        assert!(dir.join(".f.rsync-tmp").exists());
        let msg = Msg::Data { path: p.clone(), hash: *blake3::hash(b"abc").as_bytes(), offset: 0, bytes: b"abcd".to_vec(), eof: false };
        d.handle(Event::Peer { id: "aa".into(), conn: 1, msg });
        assert!(!dir.join(".f.rsync-tmp").exists());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn file_events_are_checked_only_inside_roots_and_skip_ignored_names() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("fs");
        let root = dir.join("in");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(dir.join("out")).unwrap();
        d.cfg.set_root(&me, root.to_str().unwrap(), true, false);
        for lp in [dir.join("out/f.txt"), root.join(".DS_Store"), root.join("f.txt")] {
            std::fs::write(&lp, "x").unwrap();
            d.on_fs(&lp);
        }
        let p = root.join("f.txt").to_str().unwrap().to_string();
        assert_eq!(d.index.keys().collect::<Vec<_>>(), [&p]);
        assert_eq!(d.index[&p].vv, Vv(vec![(d.me_short, 1)]));
        assert_eq!(d.outbox.len(), 1);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn file_events_through_a_symlinked_folder_are_ignored() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("fs-symlink");
        let root = dir.join("in");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(dir.join("out/sub")).unwrap();
        std::fs::write(dir.join("out/f.txt"), "secret").unwrap();
        std::fs::write(dir.join("out/sub/g.txt"), "secret").unwrap();
        std::os::unix::fs::symlink(dir.join("out"), root.join("link")).unwrap();
        d.cfg.set_root(&me, root.to_str().unwrap(), true, false);
        d.on_fs(&root.join("link/f.txt"));
        d.on_fs(&root.join("link/sub"));
        assert!(d.index.is_empty());
        assert!(d.outbox.is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn watches_follow_active_roots_with_files_watched_through_their_folder() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("watch");
        let (a, b, c) = (dir.join("a"), dir.join("b"), dir.join("c"));
        d.cfg.set_root(&me, a.to_str().unwrap(), true, false);
        d.cfg.set_root(&me, b.join("f.txt").to_str().unwrap(), false, false);
        d.cfg.set_root(&me, c.to_str().unwrap(), true, true);
        d.reconcile_watches();
        assert!(a.is_dir() && b.is_dir() && !c.exists());
        let want = HashMap::from([(a.clone(), RecursiveMode::Recursive), (b.clone(), RecursiveMode::NonRecursive)]);
        assert_eq!(d.watched, want);
        d.cfg.set_root(&me, a.to_str().unwrap(), true, true);
        d.cfg.set_root(&me, b.to_str().unwrap(), true, false);
        d.reconcile_watches();
        assert_eq!(d.watched, HashMap::from([(b, RecursiveMode::Recursive)]));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn rescan_checks_only_active_roots() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("rescan");
        for (name, removed) in [("a", false), ("b", true)] {
            std::fs::create_dir_all(dir.join(name)).unwrap();
            std::fs::write(dir.join(name).join("f.txt"), "x").unwrap();
            d.cfg.set_root(&me, dir.join(name).to_str().unwrap(), true, removed);
        }
        d.handle(Event::Rescan);
        let keys: Vec<_> = d.index.keys().cloned().collect();
        assert_eq!(keys, [dir.join("a/f.txt").to_str().unwrap()]);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn tree_check_records_nested_files_skips_ignored_names_and_marks_missing_files_deleted() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("tree");
        std::fs::create_dir_all(dir.join("sub")).unwrap();
        let root = dir.to_str().unwrap().to_string();
        d.cfg.set_root(&me, &root, true, false);
        for name in ["sub/a.txt", ".DS_Store", "b.rsync-tmp"] {
            std::fs::write(dir.join(name), "x").unwrap();
        }
        let gone = dir.join("gone.txt").to_str().unwrap().to_string();
        d.index.insert(gone.clone(), meta(0, 2));
        d.check_tree(&root);
        let a = dir.join("sub/a.txt").to_str().unwrap().to_string();
        let mut keys: Vec<_> = d.index.keys().cloned().collect();
        keys.sort();
        assert_eq!(keys, [gone.clone(), a.clone()]);
        assert!(!d.index[&a].deleted());
        assert!(d.index[&gone].deleted());
        assert_eq!(d.outbox.len(), 2);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn tree_checks_and_file_events_skip_the_state_folder() {
        let mut d = daemon();
        let me = d.me.clone();
        let state = paths::state_dir();
        let dir = state.parent().unwrap();
        std::fs::create_dir_all(state).unwrap();
        std::fs::write(state.join("index.bin"), "x").unwrap();
        std::fs::write(dir.join("notes.txt"), "x").unwrap();
        let root = dir.to_str().unwrap().to_string();
        d.cfg.set_root(&me, &root, true, false);
        d.check_tree(&root);
        d.on_fs(&state.join("index.bin"));
        let notes = dir.join("notes.txt").to_str().unwrap().to_string();
        assert_eq!(d.index.keys().collect::<Vec<_>>(), [&notes]);
        std::fs::remove_file(dir.join("notes.txt")).unwrap();
        std::fs::remove_file(state.join("index.bin")).unwrap();
    }

    #[test]
    fn unreadable_file_is_not_recorded() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = test_dir("unreadable");
        std::fs::create_dir_all(&dir).unwrap();
        let root = dir.to_str().unwrap().to_string();
        d.cfg.set_root(&me, &root, true, false);
        let f = dir.join("secret.txt");
        std::fs::write(&f, "x").unwrap();
        std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o000)).unwrap();
        d.check_tree(&root);
        assert!(d.index.is_empty());
        assert!(d.outbox.is_empty());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn conflict_name_keeps_extension_and_tags_loser() {
        let loser = meta(1_700_000_000_123_456_789, 0xdead_beef_0000_0001);
        assert_eq!(
            conflict_name(Path::new("/d/notes.md"), &loser),
            PathBuf::from("/d/notes.sync-conflict-1700000000-deadbeef.md")
        );
        assert_eq!(
            conflict_name(Path::new("/d/Makefile"), &loser),
            PathBuf::from("/d/Makefile.sync-conflict-1700000000-deadbeef")
        );
        assert_eq!(
            conflict_name(Path::new("/d/a.tar.gz"), &meta(0, 1 << 32)),
            PathBuf::from("/d/a.tar.sync-conflict-0-00000001.gz")
        );
    }

    #[test]
    fn conflict_name_of_a_longest_allowed_name_is_shortened_at_a_character_boundary() {
        let loser = meta(1_700_000_000_123_456_789, 0xdead_beef_0000_0001);
        let tag = "sync-conflict-1700000000-deadbeef";
        let long_stem = Path::new("/d").join(format!("{}.md", "é".repeat(126)));
        assert_eq!(conflict_name(&long_stem, &loser), Path::new("/d").join(format!("{}.{tag}.md", "é".repeat(109))));
        let long_ext = Path::new("/d").join(format!("a.{}", "x".repeat(253)));
        assert_eq!(conflict_name(&long_ext, &loser), Path::new("/d").join(format!("a.{}.{tag}", "x".repeat(219))));
    }

    #[test]
    fn batches_keeps_order_and_skips_empty() {
        assert!(batches(Vec::new()).is_empty());
        let entries: Vec<_> = (0..1000).map(|i| entry(format!("~/{i}"), 1)).collect();
        let out = batches(entries);
        assert!(out.len() > 1);
        assert!(out.iter().all(|b| !b.is_empty()));
        let paths: Vec<_> = out.iter().flatten().map(|e| e.path.clone()).collect();
        assert_eq!(paths, (0..1000).map(|i| format!("~/{i}")).collect::<Vec<_>>());
    }

    #[test]
    fn batches_fit_in_one_message() {
        let long_paths = (0..500).map(|i| entry(format!("~/{}/{i}", "x".repeat(200)), 8));
        let long_histories = (0..50).map(|i| entry(format!("~/{i}"), 100));
        for b in batches(long_paths.chain(long_histories).collect()) {
            let len = postcard::to_stdvec(&Msg::Index(b)).unwrap().len();
            assert!(len <= net::MAX_MSG, "{len}");
        }
    }

    #[test]
    fn batches_keeps_entries_that_reach_the_limit_exactly_together() {
        let half = || entry("x".repeat(20_000 - 100), 0);
        assert_eq!(batches(vec![half(), half()]).len(), 1);
        assert_eq!(batches(vec![half(), half(), entry("y".into(), 0)]).len(), 2);
    }

    #[test]
    fn batches_gives_an_oversized_entry_its_own_batch_even_when_first() {
        let big = entry(format!("~/{}", "b".repeat(50_000)), 1);
        let out = batches(vec![entry("~/a".into(), 1), big.clone(), entry("~/c".into(), 1)]);
        assert_eq!(out.iter().map(Vec::len).collect::<Vec<_>>(), [1, 1, 1]);
        let out = batches(vec![big, entry("~/c".into(), 1)]);
        assert_eq!(out.iter().map(Vec::len).collect::<Vec<_>>(), [1, 1]);
    }

    #[test]
    fn paths_text_lists_active_roots_and_marks_folders() {
        let mut cfg = Config::default();
        assert_eq!(paths_text(&cfg), "  (none) add one with `rust-sync path add <path>`\n");
        cfg.set_root("me", "~/docs", true, false);
        cfg.set_root("me", "~/gone", true, true);
        cfg.set_root("me", "~/notes.md", false, false);
        assert_eq!(paths_text(&cfg), "  ~/docs/\n  ~/notes.md\n");
    }

    #[test]
    fn nodes_text_shows_state_and_pending_nodes() {
        let mut cfg = Config::default();
        let none = BTreeMap::new();
        assert_eq!(nodes_text(&cfg, |_| None, &none), "  (none) add one with `rust-sync node add <host>`\n");
        cfg.set_node("me", "aaaaaaaabbbbbbbb", "a:1", false);
        cfg.set_node("me", "cccccccc", "c:1", false);
        cfg.set_node("me", "dddddddd", "d:1", true);
        let pending = BTreeMap::from([("eeeeeeee".to_string(), "1.2.3.4:5".to_string())]);
        assert_eq!(
            nodes_text(&cfg, |id| Some(id == "cccccccc"), &pending),
            "  AAAAAAAA-BBBBBBBB  a:1  offline\n  CCCCCCCC  c:1  connected\n\n\
             Waiting to be trusted (run `rust-sync node add <address>` to accept):\n  EEEEEEEE  1.2.3.4:5\n"
        );
        assert_eq!(nodes_text(&cfg, |_| None, &none), "  AAAAAAAA-BBBBBBBB  a:1  \n  CCCCCCCC  c:1  \n");
    }

    #[test]
    fn status_text_says_when_daemon_is_not_running() {
        let cfg = Config::default();
        let none = BTreeMap::new();
        let stopped = status_text(&cfg, "abcd", |_| None, &none);
        assert!(stopped.starts_with(&format!("This node: ABCD\nPort: {}\nDaemon: not running", cfg.port)));
        let running = status_text(&cfg, "abcd", |_| Some(true), &none);
        assert_eq!(
            running,
            format!(
                "This node: ABCD\nPort: {}\n\nNodes:\n{}\nPaths:\n{}",
                cfg.port,
                nodes_text(&cfg, |_| Some(true), &none),
                paths_text(&cfg)
            )
        );
    }

    #[tokio::test]
    async fn debounce_merges_paths_and_turns_none_into_rescan() {
        let (in_tx, in_rx) = mpsc::unbounded_channel();
        let (out_tx, mut out_rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(debounce(in_rx, out_tx));
        for p in ["/a", "/b", "/a"] {
            in_tx.send(Some(PathBuf::from(p))).unwrap();
        }
        let Some(Event::Fs(mut got)) = out_rx.recv().await else { panic!() };
        got.sort();
        assert_eq!(got, [PathBuf::from("/a"), PathBuf::from("/b")]);
        in_tx.send(Some(PathBuf::from("/c"))).unwrap();
        in_tx.send(None).unwrap();
        assert!(matches!(out_rx.recv().await, Some(Event::Rescan)));
        drop(in_tx);
        task.await.unwrap();
        assert!(out_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn debounce_sends_paths_after_the_longest_wait_even_while_changes_keep_coming() {
        let (in_tx, in_rx) = mpsc::unbounded_channel();
        let (out_tx, mut out_rx) = mpsc::unbounded_channel();
        drop(tokio::spawn(debounce(in_rx, out_tx)));
        let feed = tokio::spawn(async move {
            while in_tx.send(Some(PathBuf::from("/a"))).is_ok() {
                tokio::time::sleep(SETTLE / 5).await;
            }
        });
        let got = timeout(SETTLE_MAX + SETTLE, out_rx.recv()).await;
        feed.abort();
        assert!(matches!(got, Ok(Some(Event::Fs(_)))));
    }

    #[tokio::test]
    async fn upload_sends_file_in_chunks_then_eof_or_unavailable_when_missing() {
        let lp = test_dir("upload");
        let data: Vec<u8> = (0..net::CHUNK + 5).map(|i| i as u8).collect();
        std::fs::write(&lp, &data).unwrap();
        let (tx, mut rx) = mpsc::channel(8);
        upload(lp.clone(), None, "~/f".into(), [2; 32], tx.clone(), Arc::new(Semaphore::new(1))).await;
        let mut got = Vec::new();
        for (want_offset, want_len, want_eof) in [(0, net::CHUNK, false), (net::CHUNK, 5, false), (net::CHUNK + 5, 0, true)] {
            let Some(Msg::Data { path, hash, offset, bytes, eof }) = rx.recv().await else { panic!() };
            assert_eq!((path.as_str(), hash, offset, bytes.len(), eof), ("~/f", [2; 32], want_offset as u64, want_len, want_eof));
            got.extend(bytes);
        }
        assert_eq!(got, data);
        std::fs::remove_file(&lp).unwrap();
        upload(lp, None, "~/f".into(), [2; 32], tx, Arc::new(Semaphore::new(1))).await;
        assert!(matches!(rx.recv().await, Some(Msg::Unavailable { path, .. }) if path == "~/f"));
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn upload_waits_for_a_free_slot() {
        let lp = test_dir("upload-slot");
        std::fs::write(&lp, "x").unwrap();
        let slots = Arc::new(Semaphore::new(0));
        let (tx, mut rx) = mpsc::channel(8);
        let task = tokio::spawn(upload(lp.clone(), None, "~/f".into(), [2; 32], tx, slots.clone()));
        assert!(timeout(Duration::from_millis(100), rx.recv()).await.is_err());
        slots.add_permits(1);
        assert!(matches!(rx.recv().await, Some(Msg::Data { offset: 0, eof: false, .. })));
        task.await.unwrap();
        std::fs::remove_file(&lp).unwrap();
    }

    #[tokio::test]
    async fn waiting_upload_of_a_file_replaced_by_a_symlink_is_refused() {
        let dir = test_dir("upload-swap");
        let (root, outside) = (dir.join("s"), dir.join("secret"));
        std::fs::create_dir_all(root.join("d")).unwrap();
        std::fs::write(&outside, "secret").unwrap();
        for lp in [root.join("f"), root.join("d/g")] {
            std::fs::write(&lp, "x").unwrap();
            let slots = Arc::new(Semaphore::new(0));
            let (tx, mut rx) = mpsc::channel(8);
            let task = tokio::spawn(upload(lp.clone(), Some(root.clone()), "~/f".into(), [2; 32], tx, slots.clone()));
            tokio::task::yield_now().await;
            if lp.ends_with("f") {
                std::fs::remove_file(&lp).unwrap();
                std::os::unix::fs::symlink(&outside, &lp).unwrap();
            } else {
                std::fs::rename(root.join("d"), dir.join("moved")).unwrap();
                std::os::unix::fs::symlink(&dir, root.join("d")).unwrap();
                std::fs::write(dir.join("g"), "secret").unwrap();
            }
            slots.add_permits(1);
            assert!(matches!(rx.recv().await, Some(Msg::Unavailable { .. })), "{}", lp.display());
            task.await.unwrap();
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn connect_any_falls_back_to_an_address_that_listens_and_fails_when_none_does() {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let s = connect_any(&format!("localhost:{port}")).await.unwrap();
        assert_eq!(s.peer_addr().unwrap(), l.local_addr().unwrap());
        drop((s, l));
        let e = connect_any("127.0.0.1:1").await.unwrap_err();
        assert_eq!(e.kind(), std::io::ErrorKind::ConnectionRefused);
    }

    #[tokio::test]
    async fn dial_reports_done_when_the_peer_cannot_be_reached() {
        let dir = test_dir("dial");
        let keys = Arc::new(net::load_or_create_keys(&dir).unwrap());
        let addr = TcpListener::bind("127.0.0.1:0").await.unwrap().local_addr().unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        dial(addr.to_string(), "ab".repeat(32), true, keys, tx, 0).await;
        let Some(Event::DialDone { id }) = rx.recv().await else { panic!("expected DialDone") };
        assert_eq!(id, "ab".repeat(32));
        assert!(rx.recv().await.is_none());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn connection_rejects_a_peer_whose_id_is_not_the_one_dialled() {
        let dir = test_dir("conn");
        let keys = Arc::new(net::load_or_create_keys(&dir.join("a")).unwrap());
        let other = net::load_or_create_keys(&dir.join("b")).unwrap();
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let server = async {
            let (s, _) = l.accept().await.unwrap();
            let _ = net::handshake(s, &other, false).await;
        };
        let dialled = "ab".repeat(32);
        let s = TcpStream::connect(addr).await.unwrap();
        let client = connection_inner(s, addr.ip(), Some(dialled.clone()), keys, tx, 0);
        let ((), res, quiet) = tokio::join!(server, client, async { rx.recv().await.is_none() });
        let want = format!("expected node {} but found {}", net::pretty_id(&dialled), net::pretty_id(&other.id()));
        assert_eq!(res.unwrap_err().to_string(), want);
        assert!(quiet);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn connection_does_not_come_up_when_the_listener_does_not_trust_the_dialler() {
        let dir = test_dir("trust");
        let a = Arc::new(net::load_or_create_keys(&dir.join("a")).unwrap());
        let b = Arc::new(net::load_or_create_keys(&dir.join("b")).unwrap());
        for (bind, seen) in [("127.0.0.1:0", "127.0.0.1:4000"), ("[::1]:0", "[::1]:4000")] {
            let l = TcpListener::bind(bind).await.unwrap();
            let addr = l.local_addr().unwrap();
            let (a_tx, a_rx) = mpsc::unbounded_channel();
            let (b_tx, b_rx) = mpsc::unbounded_channel();
            let answer = async |mut rx: mpsc::UnboundedReceiver<Event>, trust: bool| {
                let Some(Event::Auth { id, addr, reply }) = rx.recv().await else { panic!() };
                reply.send(trust).unwrap();
                (id, addr, rx.recv().await.is_none())
            };
            let dialler = async {
                let s = TcpStream::connect(addr).await.unwrap();
                connection_inner(s, addr.ip(), Some(b.id()), a.clone(), a_tx, 4000).await
            };
            let listener = async {
                let (s, peer) = l.accept().await.unwrap();
                connection_inner(s, peer.ip(), None, b.clone(), b_tx, 5000).await
            };
            let (a_res, b_res, a_seen, b_seen) =
                tokio::join!(dialler, listener, answer(a_rx, true), answer(b_rx, false));
            a_res.unwrap();
            b_res.unwrap();
            assert_eq!(a_seen, (b.id(), None, true));
            assert_eq!(b_seen, (a.id(), Some(seen.into()), true));
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn connection_does_not_come_up_when_the_dialler_does_not_trust_the_listener() {
        let dir = test_dir("distrust");
        let a = Arc::new(net::load_or_create_keys(&dir.join("a")).unwrap());
        let b = Arc::new(net::load_or_create_keys(&dir.join("b")).unwrap());
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let (a_tx, a_rx) = mpsc::unbounded_channel();
        let (b_tx, b_rx) = mpsc::unbounded_channel();
        let answer = async |mut rx: mpsc::UnboundedReceiver<Event>, trust: bool| {
            let Some(Event::Auth { reply, .. }) = rx.recv().await else { panic!() };
            reply.send(trust).unwrap();
            rx.recv().await.is_none()
        };
        let dialler = async {
            let s = TcpStream::connect(addr).await.unwrap();
            connection_inner(s, addr.ip(), Some(b.id()), a.clone(), a_tx, 4000).await
        };
        let listener = async {
            let (s, peer) = l.accept().await.unwrap();
            connection_inner(s, peer.ip(), None, b.clone(), b_tx, 5000).await
        };
        let (a_res, b_res, a_quiet, b_quiet) = tokio::join!(dialler, listener, answer(a_rx, false), answer(b_rx, true));
        a_res.unwrap();
        b_res.unwrap();
        assert_eq!((a_quiet, b_quiet), (true, true));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn trusted_connection_comes_up_carries_control_and_data_messages_and_goes_down() {
        let dir = test_dir("up");
        let a = Arc::new(net::load_or_create_keys(&dir.join("a")).unwrap());
        let b = Arc::new(net::load_or_create_keys(&dir.join("b")).unwrap());
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let (a_tx, mut a_rx) = mpsc::unbounded_channel();
        let (b_tx, mut b_rx) = mpsc::unbounded_channel();
        let dialler = async {
            let s = TcpStream::connect(addr).await.unwrap();
            connection_inner(s, addr.ip(), Some(b.id()), a.clone(), a_tx, 4000).await
        };
        let listener = async {
            let (s, peer) = l.accept().await.unwrap();
            connection_inner(s, peer.ip(), None, b.clone(), b_tx, 5000).await
        };
        let (got_tx, got_rx) = oneshot::channel();
        let a_side = async {
            let Some(Event::Auth { reply, .. }) = a_rx.recv().await else { panic!() };
            reply.send(true).unwrap();
            let Some(Event::Up { id, conn, outbound: true, ctl, data }) = a_rx.recv().await else { panic!() };
            assert_eq!(id, b.id());
            ctl.send(Msg::Unavailable { path: "x".into(), hash: [0; 32] }).unwrap();
            data.send(Msg::Unavailable { path: "y".into(), hash: [0; 32] }).await.unwrap();
            got_rx.await.unwrap();
            drop(ctl);
            let Some(Event::Down { id, conn: down }) = a_rx.recv().await else { panic!() };
            assert_eq!((id, down), (b.id(), conn));
            assert!(a_rx.recv().await.is_none());
        };
        let b_side = async {
            let Some(Event::Auth { reply, .. }) = b_rx.recv().await else { panic!() };
            reply.send(true).unwrap();
            let Some(Event::Up { id, conn, outbound: false, ctl: _ctl, .. }) = b_rx.recv().await else { panic!() };
            assert_eq!(id, a.id());
            let Some(Event::Peer { conn: from, msg: Msg::Unavailable { path, .. }, .. }) = b_rx.recv().await else { panic!() };
            assert_eq!((from, path.as_str()), (conn, "x"));
            let Ok(Some(Event::Peer { msg: Msg::Unavailable { path, .. }, .. })) =
                timeout(Duration::from_secs(5), b_rx.recv()).await
            else {
                panic!("expected the data message")
            };
            assert_eq!(path, "y");
            got_tx.send(()).unwrap();
            let Some(Event::Down { id, conn: down }) = b_rx.recv().await else { panic!() };
            assert_eq!((id, down), (a.id(), conn));
            assert!(b_rx.recv().await.is_none());
        };
        let (a_res, b_res, (), ()) = tokio::join!(dialler, listener, a_side, b_side);
        a_res.unwrap();
        assert!(b_res.is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn probe_gets_a_hello_back_but_does_not_bring_the_connection_up() {
        let dir = test_dir("probe");
        let a = Arc::new(net::load_or_create_keys(&dir.join("a")).unwrap());
        let b = Arc::new(net::load_or_create_keys(&dir.join("b")).unwrap());
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let prober = async {
            let s = TcpStream::connect(addr).await.unwrap();
            let (mut r, mut w, _) = net::handshake(s, &a, true).await.unwrap();
            w.send(&Msg::Hello { port: 4000, probe: true, trusts_you: true }).await.unwrap();
            r.recv().await.unwrap()
        };
        let listener = async {
            let (s, peer) = l.accept().await.unwrap();
            connection_inner(s, peer.ip(), None, b.clone(), tx, 5000).await
        };
        let answer = async {
            let Some(Event::Auth { id, addr, reply }) = rx.recv().await else { panic!() };
            reply.send(true).unwrap();
            (id, addr, rx.recv().await.is_none())
        };
        let (reply, res, seen) = tokio::join!(prober, listener, answer);
        res.unwrap();
        let Msg::Hello { port: 5000, probe: false, trusts_you: true } = reply else { panic!() };
        assert_eq!(seen, (a.id(), Some("127.0.0.1:4000".into()), true));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn connection_ends_on_the_next_message_once_the_daemon_has_stopped() {
        let dir = test_dir("stopped");
        let a = Arc::new(net::load_or_create_keys(&dir.join("a")).unwrap());
        let b = Arc::new(net::load_or_create_keys(&dir.join("b")).unwrap());
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let (stopped_tx, stopped_rx) = oneshot::channel();
        let peer = async {
            let s = TcpStream::connect(addr).await.unwrap();
            let (mut r, mut w, _) = net::handshake(s, &a, true).await.unwrap();
            w.send(&Msg::Hello { port: 4000, probe: false, trusts_you: true }).await.unwrap();
            r.recv().await.unwrap();
            stopped_rx.await.unwrap();
            w.send(&Msg::Unavailable { path: "x".into(), hash: [0; 32] }).await.unwrap();
            (r, w)
        };
        let listener = async {
            let (s, peer) = l.accept().await.unwrap();
            timeout(Duration::from_secs(5), connection_inner(s, peer.ip(), None, b.clone(), tx, 5000)).await
        };
        let daemon = async {
            let Some(Event::Auth { reply, .. }) = rx.recv().await else { panic!() };
            reply.send(true).unwrap();
            let Some(Event::Up { ctl, .. }) = rx.recv().await else { panic!() };
            drop(rx);
            stopped_tx.send(()).unwrap();
            ctl
        };
        let (_peer, res, _ctl) = tokio::join!(peer, listener, daemon);
        res.expect("connection should end at once").unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn accept_loop_serves_each_incoming_connection_with_its_ipv4_address() {
        let dir = test_dir("accept");
        let a = Arc::new(net::load_or_create_keys(&dir.join("a")).unwrap());
        let b = Arc::new(net::load_or_create_keys(&dir.join("b")).unwrap());
        let l = TcpListener::bind("[::]:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(accept_loop(l, b, tx, 5000));
        for their_port in [4000, 4001] {
            let s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
            let (mut r, mut w, _) = net::handshake(s, &a, true).await.unwrap();
            w.send(&Msg::Hello { port: their_port, probe: true, trusts_you: true }).await.unwrap();
            let Some(Event::Auth { id, addr, reply }) = rx.recv().await else { panic!() };
            assert_eq!((id, addr), (a.id(), Some(format!("127.0.0.1:{their_port}"))));
            reply.send(false).unwrap();
            let Msg::Hello { port: 5000, probe: false, trusts_you: false } = r.recv().await.unwrap() else { panic!() };
        }
        task.abort();
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn ctl_loop_passes_request_to_daemon_and_sends_back_its_reply() {
        let sock = std::env::temp_dir().join(format!("rust-sync-ctl-{}.sock", std::process::id()));
        let _ = std::fs::remove_file(&sock);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(ctl_loop(UnixListener::bind(&sock).unwrap(), tx));
        let (r, mut w) = tokio::net::UnixStream::connect(&sock).await.unwrap().into_split();
        w.write_all(b"{\"RemoveNode\":{\"id\":\"abcd\"}}\n").await.unwrap();
        let Some(Event::Ctl(Req::RemoveNode { id }, reply)) = rx.recv().await else { panic!() };
        assert_eq!(id, "abcd");
        reply.send(Resp { ok: true, text: "done".into() }).unwrap();
        let mut line = String::new();
        BufReader::new(r).read_line(&mut line).await.unwrap();
        assert_eq!(line, "{\"ok\":true,\"text\":\"done\"}\n");
        task.abort();
        std::fs::remove_file(&sock).unwrap();
    }

    #[tokio::test]
    async fn ctl_loop_rejects_a_request_it_cannot_read_instead_of_closing() {
        let sock = std::env::temp_dir().join(format!("rust-sync-ctl-bad-{}.sock", std::process::id()));
        let _ = std::fs::remove_file(&sock);
        let (tx, _rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(ctl_loop(UnixListener::bind(&sock).unwrap(), tx));
        let (r, mut w) = tokio::net::UnixStream::connect(&sock).await.unwrap().into_split();
        w.write_all(b"{\"Pause\":{}}\n").await.unwrap();
        let mut line = String::new();
        BufReader::new(r).read_line(&mut line).await.unwrap();
        let resp: Resp = serde_json::from_str(&line).unwrap();
        assert!(!resp.ok);
        assert!(resp.text.starts_with("the daemon cannot read this request"), "{}", resp.text);
        task.abort();
        std::fs::remove_file(&sock).unwrap();
    }
}
