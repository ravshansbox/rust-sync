//! Daemon log lines, each starting with a UTC timestamp. Lines go to stderr, or
//! to a log file that is rotated at `MAX_SIZE`, keeping one older file (`<name>.1`).

use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Mutex, PoisonError};

mod time;
use time::timestamp;

/// Rotate the log file once a write would take it past this many bytes.
const MAX_SIZE: u64 = 1024 * 1024;

/// The open log file, with its path for rotation and its current size in bytes.
struct LogFile {
    path: PathBuf,
    file: File,
    size: u64,
}

static FILE: Mutex<Option<LogFile>> = Mutex::new(None);

/// Write log lines to `path` instead of stderr.
pub fn to_file(path: PathBuf) -> std::io::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let log = LogFile::open(path)?;
    *FILE.lock().unwrap_or_else(PoisonError::into_inner) = Some(log);
    Ok(())
}

/// Write one timestamped log line to the log file, or to stderr if none is set.
pub fn line(msg: std::fmt::Arguments) {
    let text = format!("{} {msg}\n", timestamp());
    let mut file = FILE.lock().unwrap_or_else(PoisonError::into_inner);
    match file.as_mut() {
        None => drop(std::io::stderr().write_all(text.as_bytes())),
        Some(f) => f.write(&text),
    }
}

impl LogFile {
    fn open(path: PathBuf) -> std::io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let size = file.metadata()?.len();
        Ok(Self { path, file, size })
    }

    fn write(&mut self, text: &str) {
        if self.size > 0 && self.size + text.len() as u64 > MAX_SIZE {
            let _ = self.rotate();
        }
        if self.file.write_all(text.as_bytes()).is_ok() {
            self.size += text.len() as u64;
        }
    }

    /// `rust-sync.log` -> `rust-sync.log.1` (replacing an older one), then start a new file.
    fn rotate(&mut self) -> std::io::Result<()> {
        let mut old = OsString::from(&self.path);
        old.push(".1");
        match std::fs::rename(&self.path, &old) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e),
            _ => {}
        }
        *self = Self::open(self.path.clone())?;
        Ok(())
    }
}

macro_rules! log {
    ($($arg:tt)*) => { $crate::log::line(format_args!($($arg)*)) };
}
pub(crate) use log;

#[cfg(test)]
mod tests;
