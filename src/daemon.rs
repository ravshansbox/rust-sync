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
use std::collections::{BTreeMap, HashMap, HashSet};
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
    let mut cfg = Config::load(&dir.join("config.json"))?;
    if let Some(p) = port {
        cfg.port = p;
    }
    cfg.save(&dir.join("config.json"))?;
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
    for p in d.downloads.keys().cloned().collect::<Vec<_>>() {
        d.cancel_download(&p);
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
                self.broadcast(Msg::Ping);
                self.dial_all();
                if self.ticks.is_multiple_of(RESCAN_TICKS) {
                    self.rescan_all();
                }
                self.save_index();
            }
            Event::Auth { id, addr, reply } => {
                let ok = self.cfg.trusted(&id).is_some();
                if let (false, Some(addr)) = (ok, addr) {
                    log!("untrusted node {} at {addr} tried to connect", net::pretty_id(&id));
                    self.pending.insert(id, addr);
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
                let resp = self.on_ctl(req);
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
        self.pending.remove(&id);
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

    fn broadcast(&self, msg: Msg) {
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
            self.broadcast(Msg::Index(batch));
        }
    }

    fn on_msg(&mut self, from: &str, msg: Msg) {
        match msg {
            Msg::Hello { .. } | Msg::Ping => {}
            Msg::Config(shared) => self.on_config(&shared),
            Msg::Index(entries) => entries.into_iter().for_each(|e| self.on_entry(from, e)),
            Msg::Request { path, hash } => self.on_request(from, path, hash),
            Msg::Data { path, offset, bytes, eof } => self.on_data(from, path, offset, &bytes, eof),
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
            self.config_changed(before);
        }
    }

    fn config_changed(&mut self, before: HashSet<String>) {
        if let Err(e) = self.cfg.save(&paths::state_dir().join("config.json")) {
            log!("cannot save config: {e}");
        }
        let gone: Vec<String> = self.peers.keys().filter(|id| self.cfg.trusted(id).is_none()).cloned().collect();
        for id in gone {
            self.peers.remove(&id);
            self.drop_downloads_from(&id);
        }
        self.reconcile_watches();
        for root in self.active_roots().difference(&before) {
            log!("now syncing {root}");
            self.check_tree(root);
            // Peers may not have seen our entries for this root yet.
            let known: Vec<Entry> = index::under(&self.index, root)
                .map(|p| Entry { path: p.clone(), meta: self.index[p].clone() })
                .collect();
            self.outbox.extend(known);
        }
        self.broadcast(Msg::Config(self.cfg.shared.clone()));
        self.dial_all();
    }

    fn on_ctl(&mut self, req: Req) -> Resp {
        let before = self.active_roots();
        let me = self.me.clone();
        let text = match req {
            Req::Status => return Resp { ok: true, text: self.status() },
            Req::ListNodes => {
                let text = nodes_text(&self.cfg, |id| Some(self.peers.contains_key(id)), &self.pending);
                return Resp { ok: true, text };
            }
            Req::AddPath { path, is_dir } => {
                self.cfg.set_root(&me, &path, is_dir, false);
                format!("Now syncing {path}")
            }
            Req::RemovePath { path } => {
                let Some(r) = self.cfg.shared.roots.get(&path).filter(|r| !r.removed) else {
                    return Resp { ok: false, text: format!("{path} is not synced") };
                };
                let is_dir = r.is_dir;
                self.cfg.set_root(&me, &path, is_dir, true);
                format!("Stopped syncing {path}. The files stay on disk.")
            }
            Req::AddNode { id, addr } => {
                self.cfg.set_node(&me, &id, &addr, false);
                self.pending.remove(&id);
                self.backoff.remove(&id);
                format!("Added node {}", net::pretty_id(&id))
            }
            Req::RemoveNode { id } => {
                let Some(addr) = self.cfg.trusted(&id).map(|n| n.addr.clone()) else {
                    return Resp { ok: false, text: "No such node".into() };
                };
                self.cfg.set_node(&me, &id, &addr, true);
                format!("Removed node {}", net::pretty_id(&id))
            }
        };
        self.config_changed(before);
        Resp { ok: true, text }
    }

    fn status(&self) -> String {
        let mut s = status_text(&self.cfg, &self.me, |id| Some(self.peers.contains_key(id)), &self.pending);
        let files = self.index.iter().filter(|(p, m)| !m.deleted() && self.cfg.root_of(p).is_some()).count();
        s.push_str(&format!(
            "\nFiles tracked: {files}\nTransfers: {} active, {} queued\n",
            self.downloads.len(),
            self.queued.len()
        ));
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
        let stale: Vec<PathBuf> =
            self.watched.iter().filter(|(p, m)| want.get(*p) != Some(m)).map(|(p, _)| p.clone()).collect();
        for p in stale {
            let _ = self.watcher.unwatch(&p);
            self.watched.remove(&p);
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
        let roots: Vec<String> = self.active_roots().into_iter().collect();
        for r in roots {
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
            .filter(|k| !seen.contains(*k) && self.cfg.root_of(k).is_some())
            .cloned()
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

    fn on_data(&mut self, from: &str, path: String, offset: u64, bytes: &[u8], eof: bool) {
        let Some(d) = self.downloads.get_mut(&path) else { return };
        if d.from != from {
            return;
        }
        if offset != d.written || d.file.write_all(bytes).is_err() {
            return self.cancel_download(&path);
        }
        d.hasher.update(bytes);
        d.written += bytes.len() as u64;
        if !eof {
            return;
        }
        let d = self.downloads.remove(&path).unwrap();
        if Some(*d.hasher.finalize().as_bytes()) != d.meta.hash {
            log!("{path} changed while being sent; will retry");
            let _ = std::fs::remove_file(&d.tmp);
            return;
        }
        let when = UNIX_EPOCH + Duration::from_nanos(d.meta.mtime.max(0) as u64);
        let _ = d.file.set_modified(when);
        let _ = d.file.set_permissions(std::fs::Permissions::from_mode(d.meta.mode));
        drop(d.file);
        let Some(lp) = paths::to_local(&path) else { return };
        self.check_file(&path, &lp);
        self.apply(from, &path, &lp, d.meta, Some(d.tmp));
    }

    fn cancel_download(&mut self, p: &str) {
        if let Some(d) = self.downloads.remove(p) {
            let _ = std::fs::remove_file(d.tmp);
        }
    }

    fn drop_downloads_from(&mut self, id: &str) {
        let paths: Vec<String> = self.downloads.iter().filter(|(_, d)| d.from == id).map(|(p, _)| p.clone()).collect();
        for p in paths {
            self.cancel_download(&p);
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
        s.push_str(&format!("  {}  {}  {state}\n", net::pretty_id(id), n.addr));
    }
    if !pending.is_empty() {
        s.push_str("\nWaiting to be trusted (run `rust-sync node add <address>` to accept):\n");
        for (id, addr) in pending {
            s.push_str(&format!("  {}  {addr}\n", net::pretty_id(id)));
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
                drop(tokio::spawn(connection(s, addr.ip().to_canonical(), None, keys.clone(), tx.clone(), port)))
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
        tokio::net::lookup_host(addr).await?.partition(|a| a.is_ipv6());
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
            _ = tokio::time::sleep(ATTEMPT_DELAY), if next.len() > 0 => {}
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
