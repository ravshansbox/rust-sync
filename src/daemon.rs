//! The background process. All state lives in one `Daemon` value, driven by one
//! event loop on a single thread. Other tasks (network, watcher, control socket)
//! only pass events to it.

use crate::config::Config;
use crate::ctl::{self, Req, Resp};
use crate::index::{self, Entry, FileMeta, Index};
use crate::net::{self, Keys, Msg};
use crate::log::{self, log};
use crate::paths;
use anyhow::{Context, bail};
use notify::{RecommendedWatcher, RecursiveMode};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs::File;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, UnixListener};
use tokio::sync::{Semaphore, mpsc, oneshot};

mod batch;
mod config;
mod conn;
mod download;
mod peers;
mod remote;
mod scan;
mod tasks;
mod text;
mod upload;

use batch::batches;
use conn::accept_loop;
use tasks::{ctl_loop, debounce};
pub use text::{nodes_text, paths_text, status_text};

const TICK: Duration = Duration::from_secs(30);
/// Full rescan every this many ticks (15 minutes), in case the watcher missed something.
const RESCAN_TICKS: u64 = 30;
const MAX_DOWNLOADS: usize = 8;
const MAX_UPLOADS: usize = 8;
/// Untrusted nodes remembered for `node list`. Older ones are forgotten, so repeated
/// connection attempts can't grow the list without end.
const MAX_PENDING: usize = 20;

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
                let ok = self.on_auth(id, addr);
                let _ = reply.send(ok);
            }
            Event::Up { id, conn, outbound, ctl, data } => self.peer_up(id, Peer { conn, outbound, ctl, data }),
            Event::Down { id, conn } => self.peer_down(id, conn),
            Event::DialDone { id } => self.dial_done(id),
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
            Msg::Unavailable { path, hash } => self.on_unavailable(from, path, hash),
        }
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
}

#[cfg(test)]
mod tests;
