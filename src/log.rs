//! Daemon log lines, each starting with a UTC timestamp. Lines go to stderr, or
//! to a log file that is rotated at `MAX_SIZE`, keeping one older file (`<name>.1`).

use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Mutex, PoisonError};

mod time;
use time::timestamp;

const MAX_SIZE: u64 = 1024 * 1024;

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
mod tests {
    use super::*;

    fn test_dir(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("rust-sync-log-{name}-test-{}", std::process::id()))
    }

    fn close_file() {
        *FILE.lock().unwrap_or_else(PoisonError::into_inner) = None;
    }

    #[test]
    fn write_rotates_file_when_next_line_would_exceed_max_size_replacing_an_older_rotated_file() {
        let dir = test_dir("line");
        let path = dir.join("test.log");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("test.log.1"), "older").unwrap();
        std::fs::write(&path, vec![b'x'; MAX_SIZE as usize - 10]).unwrap();
        let mut log = LogFile::open(path.clone()).unwrap();
        log.write(&format!("{} hello rotation\n", timestamp()));
        drop(log);
        assert_eq!(std::fs::metadata(dir.join("test.log.1")).unwrap().len(), MAX_SIZE - 10);
        assert!(!dir.join("test.log.1.1").exists());
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.ends_with(" hello rotation\n"));
        assert!(text.len() < MAX_SIZE as usize / 2);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn write_counts_written_lines_towards_max_size() {
        let dir = test_dir("count");
        let path = dir.join("test.log");
        std::fs::create_dir_all(&dir).unwrap();
        let line_len = timestamp().len() + " hello\n".len();
        std::fs::write(&path, vec![b'x'; MAX_SIZE as usize - line_len]).unwrap();
        let mut log = LogFile::open(path.clone()).unwrap();
        log.write(&format!("{} hello\n", timestamp()));
        log.write(&format!("{} hello\n", timestamp()));
        drop(log);
        assert_eq!(std::fs::metadata(dir.join("test.log.1")).unwrap().len(), MAX_SIZE);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), line_len as u64);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn line_longer_than_max_size_does_not_rotate_an_empty_file_over_the_older_one() {
        let dir = test_dir("long");
        let path = dir.join("test.log");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("test.log.1"), "older").unwrap();
        let mut log = LogFile::open(path.clone()).unwrap();
        log.write(&"x".repeat(MAX_SIZE as usize + 1));
        drop(log);
        assert_eq!(std::fs::read_to_string(dir.join("test.log.1")).unwrap(), "older");
        assert_eq!(std::fs::metadata(&path).unwrap().len(), MAX_SIZE + 1);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn write_starts_a_new_file_at_max_size_when_the_log_file_was_deleted() {
        let dir = test_dir("deleted");
        let path = dir.join("test.log");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(&path, vec![b'x'; MAX_SIZE as usize - 10]).unwrap();
        let mut log = LogFile::open(path.clone()).unwrap();
        std::fs::remove_file(&path).unwrap();
        log.write(&format!("{} after delete\n", timestamp()));
        drop(log);
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.ends_with(" after delete\n"));
        assert!(!dir.join("test.log.1").exists());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn to_file_creates_missing_folder_and_appends_across_reopens() {
        let dir = test_dir("to-file");
        let path = dir.join("nested/test.log");
        to_file(path.clone()).unwrap();
        line(format_args!("first"));
        to_file(path.clone()).unwrap();
        line(format_args!("second"));
        close_file();
        let text = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<_> = text.lines().map(|l| &l[timestamp().len() + 1..]).filter(|l| ["first", "second"].contains(l)).collect();
        assert_eq!(lines, ["first", "second"]);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
