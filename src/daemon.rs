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
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
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
use conn::{accept_loop, bind};
use download::Download;
use peers::Peer;
use tasks::{ctl_listen, ctl_loop, shutdown_on_signal, tick_loop, watch};
pub use text::{nodes_text, paths_text, status_text};
use upload::MAX_UPLOADS;

/// Every this often, ping peers, redial missing nodes and save the index.
const TICK: Duration = Duration::from_secs(30);
/// Wait this long before accepting again after a failed accept.
const ACCEPT_RETRY: Duration = Duration::from_secs(1);
/// Full rescan every this many ticks (15 minutes), in case the watcher missed something.
const RESCAN_TICKS: u64 = 30;

/// Something for the daemon's event loop to handle, sent by the watcher, timers, connections and the control socket.
enum Event {
    /// The watcher saw changes to these paths.
    Fs(Vec<PathBuf>),
    /// The watcher lost track of changes, so every root must be scanned again.
    Rescan,
    /// The periodic timer fired.
    Tick,
    /// A peer finished the handshake. Reply with whether we trust it.
    Auth { id: String, addr: Option<String>, reply: oneshot::Sender<bool> },
    /// A trusted peer connected. `ctl` and `data` send messages to it; `data` is bounded so file data cannot flood memory.
    Up { id: String, conn: u64, outbound: bool, ctl: mpsc::UnboundedSender<Msg>, data: mpsc::Sender<Msg> },
    /// Connection `conn` to a peer closed.
    Down { id: String, conn: u64 },
    /// An outgoing dial attempt ended, whether or not it connected.
    DialDone { id: String },
    /// The backoff wait for a node is over, so try to reach it again.
    Redial { id: String },
    /// A peer sent a message on connection `conn`.
    Peer { id: String, conn: u64, msg: Msg },
    /// A request from the control socket, with a channel for the reply.
    Ctl(Req, oneshot::Sender<Resp>),
    /// Stop the daemon.
    Shutdown,
}

/// The running daemon's state, owned by the event loop.
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

/// Refuse to start if a daemon is already running, then start the watcher, listeners and timers and run the event loop until shutdown.
async fn run_inner(port: Option<u16>) -> anyhow::Result<()> {
    let dir = paths::state_dir();
    std::fs::create_dir_all(dir)?;
    if ctl::call(&Req::Status).await?.is_some() {
        bail!("a daemon is already running for {}", dir.display());
    }
    let keys = Arc::new(net::load_or_create_keys(dir)?);
    let config_file = paths::config_file();
    let index_file = paths::index_file();
    let cfg = load_config(&config_file, port)?;
    let (tx, rx) = mpsc::unbounded_channel();

    let watcher = watch(tx.clone())?;

    let listener = bind(cfg.port).await?;
    tokio::spawn(accept_loop(listener, keys.clone(), tx.clone(), cfg.port));

    let sock = ctl::socket_path()?;
    tokio::spawn(ctl_loop(ctl_listen(&sock)?, tx.clone()));

    tokio::spawn(tick_loop(tx.clone()));
    tokio::spawn(shutdown_on_signal(tx.clone()));

    let index = index::load(&index_file);
    let mut d = Daemon::new(keys, cfg, config_file, index, index_file, watcher, tx);
    d.start();

    d.event_loop(rx).await;

    log!("stopping");
    d.save_index();
    let _ = std::fs::remove_file(sock);
    Ok(())
}

/// Load the config, override its port with `port` if given, and save it back.
fn load_config(config_file: &Path, port: Option<u16>) -> anyhow::Result<Config> {
    let mut cfg = Config::load(config_file)?;
    if let Some(p) = port {
        cfg.port = p;
    }
    cfg.save(config_file)?;
    Ok(cfg)
}

impl Daemon {
    fn new(
        keys: Arc<Keys>,
        cfg: Config,
        config_file: PathBuf,
        index: Index,
        index_file: PathBuf,
        watcher: RecommendedWatcher,
        tx: mpsc::UnboundedSender<Event>,
    ) -> Self {
        let me = keys.id();
        Daemon {
            me_short: net::short_id(&me),
            me,
            keys,
            cfg,
            config_file,
            index,
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
        }
    }

    fn start(&mut self) {
        log!("rust-sync node {}", net::pretty_id(&self.me));
        log!("listening on port {}", self.cfg.port);
        self.reconcile_watches();
        self.rescan_all();
        self.outbox.clear();
        self.dial_all();
    }

    async fn event_loop(&mut self, mut rx: mpsc::UnboundedReceiver<Event>) {
        while let Some(ev) = rx.recv().await {
            let stop = matches!(ev, Event::Shutdown);
            self.handle(ev);
            if stop {
                break;
            }
            self.pump_queue();
            self.flush();
        }
    }

    fn handle(&mut self, ev: Event) {
        match ev {
            Event::Fs(paths) => paths.iter().for_each(|p| self.on_fs(p)),
            Event::Rescan => self.rescan_all(),
            Event::Tick => self.on_tick(),
            Event::Auth { id, addr, reply } => {
                let ok = self.on_auth(id, addr);
                let _ = reply.send(ok);
            }
            Event::Up { id, conn, outbound, ctl, data } => self.peer_up(id, Peer { conn, outbound, ctl, data }),
            Event::Down { id, conn } => self.peer_down(id, conn),
            Event::DialDone { id } => self.dial_done(id),
            Event::Redial { id } => self.dial(&id),
            Event::Peer { id, conn, msg } => {
                if self.is_current(&id, conn) {
                    self.on_msg(&id, msg);
                }
            }
            Event::Ctl(req, reply) => {
                let resp = self.on_ctl(&req);
                let _ = reply.send(resp);
            }
            Event::Shutdown => self.cancel_all_downloads(),
        }
    }

    fn on_tick(&mut self) {
        self.ticks += 1;
        self.broadcast(&Msg::Ping);
        self.dial_all();
        if self.ticks.is_multiple_of(RESCAN_TICKS) {
            self.rescan_all();
        }
        self.save_index();
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
