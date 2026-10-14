use super::*;
use crate::index::Vv;
use crate::paths::test_dir;
use std::fs::File;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
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
    Daemon::new(
        keys,
        Config::default(),
        test_dir("no-state").join("config.json"),
        Index::new(),
        test_dir("no-state").join("index.bin"),
        notify::recommended_watcher(|_: notify::Result<notify::Event>| {}).unwrap(),
        mpsc::unbounded_channel().0,
    )
}
