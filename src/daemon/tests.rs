use super::*;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use crate::index::Vv;
use crate::paths::test_dir;
use tokio::time::timeout;

mod apply;
mod config;
mod conn;
mod ctl;
mod download;
mod nodes;
mod scan;
mod upload;
mod watch;

fn meta(mtime: i64, by: u64) -> FileMeta {
    FileMeta { hash: Some([1; 32]), size: 1, mtime, mode: 0o644, vv: Vv::default(), by }
}

fn download(from: &str, meta: FileMeta, tmp: &Path) -> Download {
    let file = File::create(tmp).unwrap();
    Download::new(from, meta, tmp.to_path_buf(), file)
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
