//! The background process. All state lives in one `Daemon` value, driven by one
//! event loop on a single thread. Other tasks (network, watcher, control socket)
//! only pass events to it.

use crate::config::{self, Config, Shared};
use crate::ctl::{self, Req, Resp};
use crate::index::{self, Action, Entry, FileMeta, Index, Order, Vv};
use crate::net::{self, Keys, Msg};
use crate::log::{self, log};
use crate::paths;
use anyhow::{Context, bail};
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt::Write as _;
use std::fs::File;
use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr};
use std::os::unix::fs::PermissionsExt;
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
}

struct Daemon {
    me: String,
    me_short: u64,
    keys: Arc<Keys>,
    cfg: Config,
    index: Index,
    peers: HashMap<String, Peer>,
    dialling: HashSet<String>,
    /// Nodes we failed to reach, and how long we wait before the next try.
    backoff: HashMap<String, Duration>,
    /// Untrusted nodes that tried to connect: ID -> address.
    pending: BTreeMap<String, String>,
    /// IDs in `pending`, oldest first.
    pending_order: VecDeque<String>,
    downloads: HashMap<String, Download>,
    queued: BTreeMap<String, (String, FileMeta)>,
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
    let mut cfg = Config::load(&paths::config_file())?;
    if let Some(p) = port {
        cfg.port = p;
    }
    cfg.save(&paths::config_file())?;
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
        index: index::load(&dir.join("index.bin")),
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
        let _ = peer.ctl.send(Msg::Config(self.cfg.shared.clone()));
        let entries: Vec<Entry> = self
            .index
            .iter()
            .filter(|(p, _)| self.cfg.root_of(p).is_some())
            .map(|(p, m)| Entry { path: p.clone(), meta: m.clone() })
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
        while self.pending_order.len() > MAX_PENDING {
            if let Some(oldest) = self.pending_order.pop_front() {
                self.pending.remove(&oldest);
            }
        }
    }

    fn remove_pending(&mut self, id: &str) {
        self.pending.remove(id);
        self.pending_order.retain(|x| x != id);
    }

    /// Dial every trusted node we are not connected to, except those waiting to retry.
    fn dial_all(&mut self) {
        let ids: Vec<String> = self
            .cfg
            .shared
            .nodes
            .keys()
            .filter(|id| !self.backoff.contains_key(*id))
            .cloned()
            .collect();
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
        if self.outbox.is_empty() {
            return;
        }
        for batch in batches(std::mem::take(&mut self.outbox)) {
            self.broadcast(&Msg::Index(batch));
        }
    }

    fn on_msg(&mut self, from: &str, msg: Msg) {
        match msg {
            Msg::Hello { .. } | Msg::Ping => {}
            Msg::Config(shared) => self.on_config(&shared),
            Msg::Index(entries) => entries.into_iter().for_each(|e| self.on_entry(from, e)),
            Msg::Request { path, hash } => self.on_request(from, path, hash),
            Msg::Data { path, offset, bytes, eof } => self.on_data(from, &path, offset, &bytes, eof),
            Msg::Unavailable { path } => {
                if self.downloads.get(&path).is_some_and(|d| d.from == from) {
                    self.cancel_download(&path);
                }
            }
        }
    }

    // ---- Shared config ----

    fn active_roots(&self) -> HashSet<String> {
        self.cfg.active_roots().map(|(r, _)| r.clone()).collect()
    }

    fn on_config(&mut self, shared: &Shared) {
        let before = self.active_roots();
        if self.cfg.merge(shared, &self.me) {
            self.config_changed(&before);
        }
    }

    fn config_changed(&mut self, before: &HashSet<String>) {
        if let Err(e) = self.cfg.save(&paths::config_file()) {
            log!("cannot save config: {e}");
        }
        let gone: Vec<String> = self.peers.keys().filter(|id| self.cfg.trusted(id).is_none()).cloned().collect();
        for id in gone {
            self.peers.remove(&id);
            self.drop_downloads_from(&id);
        }
        self.reconcile_watches();
        for root in self.active_roots().difference(before) {
            log!("now syncing {root}");
            self.check_tree(root);
            // Peers may not have seen our entries for this root yet.
            self.outbox.extend(index::under(&self.index, root).map(|(p, m)| Entry { path: p.clone(), meta: m.clone() }));
        }
        self.broadcast(&Msg::Config(self.cfg.shared.clone()));
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
        let mut s = status_text(&self.cfg, &self.me, |id| Some(self.peers.contains_key(id)), &self.pending);
        let files = self.index.iter().filter(|(p, m)| !m.deleted() && self.cfg.root_of(p).is_some()).count();
        let _ = write!(
            s,
            "\nFiles tracked: {files}\nTransfers: {} active, {} queued\n",
            self.downloads.len(),
            self.queued.len()
        );
        s
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
        for r in self.active_roots() {
            self.check_tree(&r);
        }
    }

    fn on_fs(&mut self, lp: &Path) {
        if lp.starts_with(paths::state_dir()) || lp.file_name().and_then(|n| n.to_str()).is_some_and(paths::ignored) {
            return;
        }
        let Some(p) = paths::to_portable(lp) else { return };
        if self.cfg.root_of(&p).is_some() {
            self.check_tree(&p);
        }
    }

    /// Bring the index up to date for `p` and everything under it.
    fn check_tree(&mut self, p: &str) {
        let Some(lp) = paths::to_local(p) else { return };
        let mut seen = HashSet::new();
        match std::fs::symlink_metadata(&lp) {
            Ok(m) if m.is_dir() => self.walk(&lp, &mut seen),
            _ => {
                self.check_file(p, &lp);
                seen.insert(p.to_string());
            }
        }
        let missing: Vec<String> = index::under(&self.index, p)
            .filter(|(k, _)| !seen.contains(*k) && self.cfg.root_of(k).is_some())
            .map(|(k, _)| k.clone())
            .collect();
        for k in missing {
            if let Some(l) = paths::to_local(&k) {
                self.check_file(&k, &l);
            }
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
            {
                self.check_file(&p, &path);
                seen.insert(p);
            }
        }
    }

    /// Compare one file on disk with the index. Records and announces local changes.
    fn check_file(&mut self, p: &str, lp: &Path) {
        let old = self.index.get(p).cloned();
        let me = self.me_short;
        let bump = |old: &Option<FileMeta>| old.as_ref().map_or_else(Vv::default, |m| m.vv.clone()).bumped(me);
        let new = match (index::stat(lp), &old) {
            (None, None) => return,
            (None, Some(m)) if m.deleted() => return,
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
        match index::save(&self.index, &paths::state_dir().join("index.bin")) {
            Ok(()) => self.index_dirty = false,
            Err(e) => log!("cannot save index: {e}"),
        }
    }

    // ---- Remote changes ----

    fn on_entry(&mut self, from: &str, e: Entry) {
        if self.cfg.root_of(&e.path).is_none() {
            return;
        }
        let Some(lp) = paths::to_local(&e.path) else { return };
        if lp.file_name().and_then(|n| n.to_str()).is_none_or(paths::ignored) {
            return;
        }
        // Catch local changes the watcher has not reported yet, before deciding.
        self.check_file(&e.path, &lp);
        self.apply(from, &e.path, &lp, e.meta, None);
    }

    /// Act on a remote version of `p`. `tmp` holds its content once downloaded.
    fn apply(&mut self, from: &str, p: &str, lp: &Path, r: FileMeta, mut tmp: Option<PathBuf>) {
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
                match std::fs::remove_file(lp) {
                    Ok(()) => log!("deleted {p}"),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return log!("cannot delete {p}: {e}"),
                }
                self.set_entry(p, r);
            }
            Action::Take | Action::Conflict => match tmp.take() {
                None => self.start_download(from, p, r),
                Some(t) => {
                    if let (Action::Conflict, Some(l)) = (&action, &local)
                        && lp.exists()
                    {
                        let copy = conflict_name(lp, l);
                        match std::fs::rename(lp, &copy) {
                            Ok(()) => log!("conflict: kept local version of {p} as {}", copy.display()),
                            Err(e) => {
                                let _ = std::fs::remove_file(&t);
                                return log!("cannot keep conflict copy of {p}: {e}");
                            }
                        }
                    }
                    if let Err(e) = std::fs::rename(&t, lp) {
                        let _ = std::fs::remove_file(&t);
                        return log!("cannot write {p}: {e}");
                    }
                    log!("updated {p}");
                    self.set_entry(p, r);
                }
            },
        }
        if let Some(t) = tmp {
            let _ = std::fs::remove_file(t);
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
        if let Some(d) = self.downloads.get(p) {
            if d.meta.vv.compare(&r.vv) != Order::Older {
                return;
            }
            self.cancel_download(p);
        }
        if self.downloads.len() >= MAX_DOWNLOADS {
            if self.queued.get(p).is_none_or(|(_, q)| q.vv.compare(&r.vv) != Order::Newer) {
                self.queued.insert(p.to_string(), (from.to_string(), r));
            }
            return;
        }
        let Some(peer) = self.peers.get(from) else { return };
        let Some(lp) = paths::to_local(p) else { return };
        let (Some(parent), Some(name)) = (lp.parent(), lp.file_name()) else { return };
        let tmp = parent.join(format!(".{}.rsync-tmp", name.to_string_lossy()));
        let file = match std::fs::create_dir_all(parent).and_then(|()| File::create(&tmp)) {
            Ok(f) => f,
            Err(e) => return log!("cannot create {}: {e}", tmp.display()),
        };
        let _ = peer.ctl.send(Msg::Request { path: p.to_string(), hash: r.hash.unwrap() });
        let d = Download { from: from.to_string(), meta: r, tmp, file, hasher: blake3::Hasher::new(), written: 0 };
        self.downloads.insert(p.to_string(), d);
    }

    fn pump_queue(&mut self) {
        while self.downloads.len() < MAX_DOWNLOADS {
            let Some((p, (from, r))) = self.queued.pop_first() else { break };
            if !self.peers.contains_key(&from) {
                continue;
            }
            if let Some(lp) = paths::to_local(&p) {
                // Decide again: things may have changed while it was queued.
                self.check_file(&p, &lp);
                self.apply(&from, &p, &lp, r, None);
            }
        }
    }

    fn on_data(&mut self, from: &str, path: &str, offset: u64, bytes: &[u8], eof: bool) {
        let Some(d) = self.downloads.get_mut(path) else { return };
        if d.from != from {
            return;
        }
        if offset != d.written || d.file.write_all(bytes).is_err() {
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
            let _ = std::fs::remove_file(&d.tmp);
            return;
        }
        let when = UNIX_EPOCH + Duration::from_nanos(d.meta.mtime.max(0).cast_unsigned());
        let _ = d.file.set_modified(when);
        let _ = d.file.set_permissions(std::fs::Permissions::from_mode(d.meta.mode));
        drop(d.file);
        let Some(lp) = paths::to_local(path) else { return };
        self.check_file(path, &lp);
        self.apply(from, path, &lp, d.meta, Some(d.tmp));
    }

    fn cancel_download(&mut self, p: &str) {
        if let Some(d) = self.downloads.remove(p) {
            let _ = std::fs::remove_file(d.tmp);
        }
    }

    fn drop_downloads_from(&mut self, id: &str) {
        for (_, d) in self.downloads.extract_if(|_, d| d.from == id) {
            let _ = std::fs::remove_file(d.tmp);
        }
        self.queued.retain(|_, (from, _)| from != id);
    }

    fn on_request(&mut self, from: &str, path: String, hash: [u8; 32]) {
        let Some(peer) = self.peers.get(from) else { return };
        let ok = self.cfg.root_of(&path).is_some() && self.index.get(&path).is_some_and(|m| m.hash == Some(hash));
        match paths::to_local(&path) {
            Some(lp) if ok => drop(tokio::spawn(upload(lp, path, peer.data.clone(), self.uploads.clone()))),
            _ => drop(peer.ctl.send(Msg::Unavailable { path })),
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
    let mut s = format!("This node: {}\nPort: {}\n", net::pretty_id(me), cfg.port);
    if connected(me).is_none() {
        s.push_str("Daemon: not running (start it with `rust-sync daemon`)\n");
    }
    s.push_str("\nNodes:\n");
    s.push_str(&nodes_text(cfg, connected, pending));
    s.push_str("\nPaths:\n");
    s.push_str(&paths_text(cfg));
    s
}

/// Synced files and folders. Folders end with `/`.
pub fn paths_text(cfg: &Config) -> String {
    let roots: Vec<_> = cfg.active_roots().collect();
    if roots.is_empty() {
        return "  (none) add one with `rust-sync path add <path>`\n".into();
    }
    roots.into_iter().fold(String::new(), |mut s, (p, r)| {
        let _ = writeln!(s, "  {p}{}", if r.is_dir { "/" } else { "" });
        s
    })
}

/// Trusted nodes with their address and connection state, then untrusted nodes
/// waiting to be accepted. `connected` returns `None` when the daemon is not running.
pub fn nodes_text(
    cfg: &Config,
    connected: impl Fn(&str) -> Option<bool>,
    pending: &BTreeMap<String, String>,
) -> String {
    let mut s = String::new();
    let nodes: Vec<_> = cfg.shared.nodes.iter().filter(|(_, n)| !n.removed).collect();
    if nodes.is_empty() {
        s.push_str("  (none) add one with `rust-sync node add <host>`\n");
    }
    for (id, n) in nodes {
        let state = match connected(id) {
            Some(true) => "connected",
            Some(false) => "offline",
            None => "",
        };
        let _ = writeln!(s, "  {}  {}  {state}", net::pretty_id(id), n.addr);
    }
    if !pending.is_empty() {
        s.push_str("\nWaiting to be trusted (run `rust-sync node add <address>` to accept):\n");
        for (id, addr) in pending {
            let _ = writeln!(s, "  {}  {addr}", net::pretty_id(id));
        }
    }
    s
}

/// `notes.md` -> `notes.sync-conflict-<mtime>-<node>.md`, named after the losing
/// version, so every node that holds that version picks the same name.
fn conflict_name(lp: &Path, loser: &FileMeta) -> PathBuf {
    let stem = lp.file_stem().unwrap_or_default().to_string_lossy();
    let tag = format!("sync-conflict-{}-{:08x}", loser.mtime / 1_000_000_000, loser.by >> 32);
    let name = match lp.extension() {
        Some(ext) => format!("{stem}.{tag}.{}", ext.to_string_lossy()),
        None => format!("{stem}.{tag}"),
    };
    lp.with_file_name(name)
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
            let (reply, rx) = oneshot::channel();
            tx.send(Event::Ctl(serde_json::from_str(&line)?, reply))?;
            let mut out = serde_json::to_string(&rx.await?)?;
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
            let ip = s.peer_addr().map_or(IpAddr::from([0u8; 4]), |a| a.ip().to_canonical());
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
        let trusted = auth(Some(format!("{ip}:{their_port}"))).await.unwrap_or(false);
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
async fn upload(lp: PathBuf, path: String, tx: mpsc::Sender<Msg>, slots: Arc<Semaphore>) {
    let Ok(_slot) = slots.acquire().await else { return };
    let Ok(mut f) = File::open(&lp) else {
        let _ = tx.send(Msg::Unavailable { path }).await;
        return;
    };
    let mut buf = vec![0u8; net::CHUNK];
    let mut offset = 0u64;
    loop {
        let msg = match f.read(&mut buf) {
            Ok(0) => Msg::Data { path: path.clone(), offset, bytes: Vec::new(), eof: true },
            Ok(n) => Msg::Data { path: path.clone(), offset, bytes: buf[..n].to_vec(), eof: false },
            Err(_) => Msg::Unavailable { path: path.clone() },
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

    fn daemon() -> Daemon {
        let keys = Arc::new(Keys { private: vec![0; 32], public: vec![1; 32] });
        let me = keys.id();
        Daemon {
            me_short: net::short_id(&me),
            me,
            keys,
            cfg: Config::default(),
            index: Index::new(),
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

    #[tokio::test]
    async fn tick_pings_peers_and_dials_offline_nodes_not_waiting_to_retry() {
        let mut d = daemon();
        let me = d.me.clone();
        for id in ["aa", "bb", "cc"] {
            d.cfg.set_node(&me, id, "127.0.0.1:1", false);
        }
        let (ctl, mut rx) = mpsc::unbounded_channel();
        d.handle(Event::Up { id: "aa".into(), conn: 1, outbound: true, ctl, data: mpsc::channel(1).0 });
        while rx.try_recv().is_ok() {}
        d.backoff.insert("bb".into(), Duration::from_secs(1));
        d.handle(Event::Tick);
        assert!(matches!(rx.try_recv(), Ok(Msg::Ping)));
        assert_eq!(d.dialling, HashSet::from(["cc".to_string()]));
        d.handle(Event::Redial { id: "bb".into() });
        assert_eq!(d.dialling, HashSet::from(["bb".to_string(), "cc".to_string()]));
    }

    #[tokio::test]
    async fn only_the_current_connection_going_down_marks_the_node_offline() {
        let mut d = daemon();
        let me = d.me.clone();
        d.cfg.set_node(&me, "aa", "h:1", false);
        let (ctl, _rx) = mpsc::unbounded_channel();
        d.handle(Event::Up { id: "aa".into(), conn: 1, outbound: true, ctl, data: mpsc::channel(1).0 });
        d.handle(Event::Down { id: "aa".into(), conn: 2 });
        assert!(d.on_ctl(&Req::ListNodes).text.contains("  connected"));
        d.handle(Event::Down { id: "aa".into(), conn: 1 });
        assert!(d.on_ctl(&Req::ListNodes).text.contains("  offline"));
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
        let ctl = mpsc::unbounded_channel().0;
        d.handle(Event::Up { id: "aa".into(), conn: 1, outbound: true, ctl, data: mpsc::channel(1).0 });
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
            let ctl = mpsc::unbounded_channel().0;
            d.handle(Event::Up { id: id.into(), conn, outbound: true, ctl, data: mpsc::channel(1).0 });
            let tmp = std::env::temp_dir().join(format!("rust-sync-down-{id}-{}", std::process::id()));
            let file = File::create(&tmp).unwrap();
            let dl = Download { from: id.into(), meta: meta(0, 0), tmp: tmp.clone(), file, hasher: blake3::Hasher::new(), written: 0 };
            d.downloads.insert(format!("~/s/{id}"), dl);
            tmps.push(tmp);
        }
        d.handle(Event::Down { id: "aa".into(), conn: 1 });
        assert!(!tmps[0].exists());
        assert!(tmps[1].exists());
        d.cancel_download("~/s/bb");
    }

    #[test]
    fn request_is_refused_outside_shared_folders_or_for_another_version() {
        let mut d = daemon();
        let me = d.me.clone();
        d.cfg.set_node(&me, "aa", "h:1", false);
        d.cfg.set_root(&me, "~/s", true, false);
        d.index.insert("~/s/f".into(), meta(0, 0));
        d.index.insert("~/x".into(), meta(0, 0));
        let (ctl, mut rx) = mpsc::unbounded_channel();
        d.handle(Event::Up { id: "aa".into(), conn: 1, outbound: true, ctl, data: mpsc::channel(1).0 });
        while rx.try_recv().is_ok() {}
        for (path, hash) in [("~/s/f", [2; 32]), ("~/x", [1; 32])] {
            d.handle(Event::Peer { id: "aa".into(), conn: 1, msg: Msg::Request { path: path.into(), hash } });
            assert!(matches!(rx.try_recv(), Ok(Msg::Unavailable { path: p }) if p == path));
        }
    }

    #[test]
    fn messages_from_a_stale_connection_are_ignored() {
        let mut d = daemon();
        let me = d.me.clone();
        d.cfg.set_node(&me, "aa", "h:1", false);
        let (ctl, mut rx) = mpsc::unbounded_channel();
        d.handle(Event::Up { id: "aa".into(), conn: 1, outbound: true, ctl, data: mpsc::channel(1).0 });
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
        let (ctl, mut rx) = mpsc::unbounded_channel();
        d.handle(Event::Up { id: "aa".into(), conn: 1, outbound: true, ctl, data: mpsc::channel(1).0 });
        assert!(matches!(rx.try_recv(), Ok(Msg::Config(_))));
        let Ok(Msg::Index(entries)) = rx.try_recv() else { panic!("no index sent") };
        assert_eq!(entries.iter().map(|e| e.path.as_str()).collect::<Vec<_>>(), ["~/s/f"]);
        assert!(rx.try_recv().is_err());
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
        d.queued.insert("~/s/c".into(), ("aa".into(), meta(0, 0)));
        let text = d.on_ctl(&Req::Status).text;
        assert!(text.contains("\nFiles tracked: 2\nTransfers: 0 active, 1 queued\n"), "{text}");
    }

    #[test]
    fn rejected_request_replies_with_the_error_and_changes_nothing() {
        let mut d = daemon();
        let me = d.me.clone();
        d.cfg.set_node(&me, "aa", "h:1", false);
        let (ctl, mut rx) = mpsc::unbounded_channel();
        d.handle(Event::Up { id: "aa".into(), conn: 1, outbound: true, ctl, data: mpsc::channel(1).0 });
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
    fn unavailable_cancels_a_download_only_when_sent_by_its_source() {
        let mut d = daemon();
        let me = d.me.clone();
        for (id, conn) in [("aa", 1), ("bb", 2)] {
            d.cfg.set_node(&me, id, "h:1", false);
            let ctl = mpsc::unbounded_channel().0;
            d.handle(Event::Up { id: id.into(), conn, outbound: true, ctl, data: mpsc::channel(1).0 });
        }
        let tmp = std::env::temp_dir().join(format!("rust-sync-unavailable-{}", std::process::id()));
        let file = File::create(&tmp).unwrap();
        let dl = Download { from: "aa".into(), meta: meta(0, 0), tmp: tmp.clone(), file, hasher: blake3::Hasher::new(), written: 0 };
        d.downloads.insert("~/s/f".into(), dl);
        d.handle(Event::Peer { id: "bb".into(), conn: 2, msg: Msg::Unavailable { path: "~/s/f".into() } });
        assert!(tmp.exists());
        d.handle(Event::Peer { id: "aa".into(), conn: 1, msg: Msg::Unavailable { path: "~/s/f".into() } });
        assert!(!tmp.exists());
    }

    #[test]
    fn data_is_written_only_from_the_source_and_a_gap_cancels_the_download() {
        let mut d = daemon();
        let me = d.me.clone();
        for (id, conn) in [("aa", 1), ("bb", 2)] {
            d.cfg.set_node(&me, id, "h:1", false);
            let ctl = mpsc::unbounded_channel().0;
            d.handle(Event::Up { id: id.into(), conn, outbound: true, ctl, data: mpsc::channel(1).0 });
        }
        let tmp = std::env::temp_dir().join(format!("rust-sync-data-{}", std::process::id()));
        let file = File::create(&tmp).unwrap();
        let dl = Download { from: "aa".into(), meta: meta(0, 0), tmp: tmp.clone(), file, hasher: blake3::Hasher::new(), written: 0 };
        d.downloads.insert("~/s/f".into(), dl);
        let data = |offset, bytes: &[u8]| Msg::Data { path: "~/s/f".into(), offset, bytes: bytes.to_vec(), eof: false };
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
        let ctl = mpsc::unbounded_channel().0;
        d.handle(Event::Up { id: "aa".into(), conn: 1, outbound: true, ctl, data: mpsc::channel(1).0 });
        let tmp = std::env::temp_dir().join(format!("rust-sync-mismatch-{}", std::process::id()));
        let file = File::create(&tmp).unwrap();
        let dl = Download { from: "aa".into(), meta: meta(0, 0), tmp: tmp.clone(), file, hasher: blake3::Hasher::new(), written: 0 };
        d.downloads.insert("~/s/f".into(), dl);
        let msg = Msg::Data { path: "~/s/f".into(), offset: 0, bytes: b"abc".to_vec(), eof: true };
        d.handle(Event::Peer { id: "aa".into(), conn: 1, msg });
        assert!(d.downloads.is_empty());
        assert!(!tmp.exists());
    }

    #[test]
    fn a_running_download_is_replaced_only_by_a_newer_version() {
        let mut d = daemon();
        let tmp = std::env::temp_dir().join(format!("rust-sync-replace-{}", std::process::id()));
        let file = File::create(&tmp).unwrap();
        let mut running = meta(0, 0);
        running.vv = Vv(vec![(1, 2)]);
        let dl = Download { from: "aa".into(), meta: running, tmp: tmp.clone(), file, hasher: blake3::Hasher::new(), written: 0 };
        d.downloads.insert("~/s/f".into(), dl);
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
    fn when_downloads_are_full_the_queue_keeps_the_newest_version() {
        let mut d = daemon();
        let tmp = std::env::temp_dir().join(format!("rust-sync-queue-{}", std::process::id()));
        for n in 0..MAX_DOWNLOADS {
            let file = File::create(&tmp).unwrap();
            let dl = Download { from: "aa".into(), meta: meta(0, 0), tmp: tmp.clone(), file, hasher: blake3::Hasher::new(), written: 0 };
            d.downloads.insert(format!("~/s/{n}"), dl);
        }
        let queued = |d: &Daemon| d.queued.get("~/s/f").map(|(from, m)| (from.clone(), m.vv.clone()));
        let mut r = meta(0, 0);
        for (from, n, want) in [("aa", 2, ("aa", 2)), ("bb", 1, ("aa", 2)), ("bb", 2, ("bb", 2)), ("cc", 3, ("cc", 3))] {
            r.vv = Vv(vec![(1, n)]);
            d.start_download(from, "~/s/f", r.clone());
            assert_eq!(queued(&d), Some((want.0.to_string(), Vv(vec![(1, want.1)]))));
        }
        assert_eq!(d.downloads.len(), MAX_DOWNLOADS);
        std::fs::remove_file(tmp).unwrap();
    }

    #[test]
    fn queue_waits_for_a_free_slot_and_drops_files_from_offline_nodes() {
        let mut d = daemon();
        let tmp = std::env::temp_dir().join(format!("rust-sync-pump-{}", std::process::id()));
        for n in 0..MAX_DOWNLOADS {
            let file = File::create(&tmp).unwrap();
            let dl = Download { from: "aa".into(), meta: meta(0, 0), tmp: tmp.clone(), file, hasher: blake3::Hasher::new(), written: 0 };
            d.downloads.insert(format!("~/s/{n}"), dl);
        }
        d.queued.insert("~/s/f".into(), ("aa".into(), meta(0, 0)));
        d.queued.insert("~/s/g".into(), ("bb".into(), meta(0, 0)));
        d.pump_queue();
        assert_eq!(d.queued.len(), 2);
        d.cancel_download("~/s/0");
        d.pump_queue();
        assert!(d.queued.is_empty());
        assert_eq!(d.downloads.len(), MAX_DOWNLOADS - 1);
        let _ = std::fs::remove_file(tmp);
    }

    #[test]
    fn downloaded_conflict_winner_replaces_the_file_and_keeps_the_local_copy() {
        let mut d = daemon();
        let dir = std::env::temp_dir().join(format!("rust-sync-conflict-{}", std::process::id()));
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
    fn newer_remote_deletion_removes_the_local_file() {
        let mut d = daemon();
        let dir = std::env::temp_dir().join(format!("rust-sync-delete-{}", std::process::id()));
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
    fn downloaded_conflict_loser_changes_nothing_and_removes_the_temp_file() {
        let mut d = daemon();
        let dir = std::env::temp_dir().join(format!("rust-sync-local-wins-{}", std::process::id()));
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
        let dir = std::env::temp_dir().join(format!("rust-sync-check-{}", std::process::id()));
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
    fn remote_entries_outside_roots_or_for_ignored_names_are_skipped_and_unseen_local_changes_count() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = std::env::temp_dir().join(format!("rust-sync-entry-{}", std::process::id()));
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
    fn file_events_are_checked_only_inside_roots_and_skip_ignored_names() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = std::env::temp_dir().join(format!("rust-sync-fs-{}", std::process::id()));
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
    fn watches_follow_active_roots_with_files_watched_through_their_folder() {
        let mut d = daemon();
        let me = d.me.clone();
        let dir = std::env::temp_dir().join(format!("rust-sync-watch-{}", std::process::id()));
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
        let dir = std::env::temp_dir().join(format!("rust-sync-rescan-{}", std::process::id()));
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
        let dir = std::env::temp_dir().join(format!("rust-sync-tree-{}", std::process::id()));
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
        let entries: Vec<_> = (0..500).map(|i| entry(format!("~/{}/{i}", "x".repeat(200)), 8)).collect();
        for b in batches(entries) {
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
    fn batches_gives_an_oversized_entry_its_own_batch() {
        let big = entry(format!("~/{}", "b".repeat(50_000)), 1);
        let out = batches(vec![entry("~/a".into(), 1), big, entry("~/c".into(), 1)]);
        assert_eq!(out.iter().map(Vec::len).collect::<Vec<_>>(), [1, 1, 1]);
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
    async fn upload_sends_file_in_chunks_then_eof_or_unavailable_when_missing() {
        let lp = std::env::temp_dir().join(format!("rust-sync-upload-{}", std::process::id()));
        let data: Vec<u8> = (0..net::CHUNK + 5).map(|i| i as u8).collect();
        std::fs::write(&lp, &data).unwrap();
        let (tx, mut rx) = mpsc::channel(8);
        upload(lp.clone(), "~/f".into(), tx.clone(), Arc::new(Semaphore::new(1))).await;
        let mut got = Vec::new();
        for (want_offset, want_len, want_eof) in [(0, net::CHUNK, false), (net::CHUNK, 5, false), (net::CHUNK + 5, 0, true)] {
            let Some(Msg::Data { path, offset, bytes, eof }) = rx.recv().await else { panic!() };
            assert_eq!((path.as_str(), offset, bytes.len(), eof), ("~/f", want_offset as u64, want_len, want_eof));
            got.extend(bytes);
        }
        assert_eq!(got, data);
        std::fs::remove_file(&lp).unwrap();
        upload(lp, "~/f".into(), tx, Arc::new(Semaphore::new(1))).await;
        assert!(matches!(rx.recv().await, Some(Msg::Unavailable { path }) if path == "~/f"));
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn connect_any_falls_back_to_an_address_that_listens_and_fails_when_none_does() {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let s = connect_any(&format!("localhost:{port}")).await.unwrap();
        assert_eq!(s.peer_addr().unwrap(), l.local_addr().unwrap());
        drop((s, l));
        assert!(connect_any("127.0.0.1:1").await.is_err());
    }

    #[tokio::test]
    async fn dial_reports_done_when_the_peer_cannot_be_reached() {
        let dir = std::env::temp_dir().join(format!("rust-sync-dial-{}", std::process::id()));
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
        let dir = std::env::temp_dir().join(format!("rust-sync-conn-{}", std::process::id()));
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
        let ((), res) = tokio::join!(server, client);
        let want = format!("expected node {} but found {}", net::pretty_id(&dialled), net::pretty_id(&other.id()));
        assert_eq!(res.unwrap_err().to_string(), want);
        assert!(rx.recv().await.is_none());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn connection_does_not_come_up_when_the_listener_does_not_trust_the_dialler() {
        let dir = std::env::temp_dir().join(format!("rust-sync-trust-{}", std::process::id()));
        let a = Arc::new(net::load_or_create_keys(&dir.join("a")).unwrap());
        let b = Arc::new(net::load_or_create_keys(&dir.join("b")).unwrap());
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
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
        let (a_res, b_res, a_seen, b_seen) = tokio::join!(dialler, listener, answer(a_rx, true), answer(b_rx, false));
        a_res.unwrap();
        b_res.unwrap();
        assert_eq!(a_seen, (b.id(), None, true));
        assert_eq!(b_seen, (a.id(), Some("127.0.0.1:4000".into()), true));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn trusted_connection_comes_up_carries_messages_and_goes_down() {
        let dir = std::env::temp_dir().join(format!("rust-sync-up-{}", std::process::id()));
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
        let a_side = async {
            let Some(Event::Auth { reply, .. }) = a_rx.recv().await else { panic!() };
            reply.send(true).unwrap();
            let Some(Event::Up { id, conn, outbound: true, ctl, .. }) = a_rx.recv().await else { panic!() };
            assert_eq!(id, b.id());
            ctl.send(Msg::Unavailable { path: "x".into() }).unwrap();
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
            let Some(Event::Peer { conn: from, msg: Msg::Unavailable { path }, .. }) = b_rx.recv().await else { panic!() };
            assert_eq!((from, path.as_str()), (conn, "x"));
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
        let dir = std::env::temp_dir().join(format!("rust-sync-probe-{}", std::process::id()));
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
    async fn accept_loop_serves_each_incoming_connection_with_its_ipv4_address() {
        let dir = std::env::temp_dir().join(format!("rust-sync-accept-{}", std::process::id()));
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
}
