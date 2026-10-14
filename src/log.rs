//! Daemon log lines, each starting with a UTC timestamp. Lines go to stderr, or
//! to a log file that is rotated at `MAX_SIZE`, keeping one older file (`<name>.1`).

use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Mutex, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

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
        Some(f) => {
            if f.size + text.len() as u64 > MAX_SIZE {
                let _ = f.rotate();
            }
            if f.file.write_all(text.as_bytes()).is_ok() {
                f.size += text.len() as u64;
            }
        }
    }
}

impl LogFile {
    fn open(path: PathBuf) -> std::io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let size = file.metadata()?.len();
        Ok(Self { path, file, size })
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

/// Current time as `2026-09-27 12:34:56Z`.
fn timestamp() -> String {
    let secs = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
    let (y, m, d) = civil_from_days((secs / 86_400).cast_signed());
    let s = secs % 86_400;
    format!("{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}Z", s / 3600, s % 3600 / 60, s % 60)
}

/// Days since 1970-01-01 -> (year, month, day). Howard Hinnant's `civil_from_days`.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = u32::try_from(z.rem_euclid(146_097)).unwrap_or_default();
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (i64::from(yoe) + era * 400 + i64::from(m <= 2), m, d)
}

macro_rules! log {
    ($($arg:tt)*) => { $crate::log::line(format_args!($($arg)*)) };
}
pub(crate) use log;

#[cfg(test)]
mod tests {
    use super::*;

    static FILE_TESTS: Mutex<()> = Mutex::new(());

    fn test_dir(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("rust-sync-log-{name}-test-{}", std::process::id()))
    }

    fn close_file() {
        *FILE.lock().unwrap_or_else(PoisonError::into_inner) = None;
    }

    #[test]
    fn civil_from_days_converts_known_dates() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(-1), (1969, 12, 31));
        assert_eq!(civil_from_days(59), (1970, 3, 1));
        assert_eq!(civil_from_days(11_016), (2000, 2, 29));
        assert_eq!(civil_from_days(11_017), (2000, 3, 1));
        assert_eq!(civil_from_days(20_723), (2026, 9, 27));
    }

    #[test]
    fn timestamp_shows_current_utc_time() {
        let now = || SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let (secs, t) = loop {
            let before = now();
            let t = timestamp();
            if now() == before {
                break (before, t);
            }
        };
        let (y, m, d) = civil_from_days((secs / 86_400).cast_signed());
        assert_eq!(t.len(), 20);
        assert_eq!(&t[..11], format!("{y:04}-{m:02}-{d:02} "));
        let field = |r: std::ops::Range<usize>| t[r].parse::<u64>().unwrap();
        assert_eq!(field(11..13) * 3600 + field(14..16) * 60 + field(17..19), secs % 86_400);
        assert_eq!((&t[13..14], &t[16..17], &t[19..]), (":", ":", "Z"));
    }

    #[test]
    fn line_rotates_file_when_next_line_would_exceed_max_size_replacing_an_older_rotated_file() {
        let _guard = FILE_TESTS.lock().unwrap_or_else(PoisonError::into_inner);
        let dir = test_dir("line");
        let path = dir.join("test.log");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("test.log.1"), "older").unwrap();
        std::fs::write(&path, vec![b'x'; MAX_SIZE as usize - 10]).unwrap();
        to_file(path.clone()).unwrap();
        line(format_args!("hello rotation"));
        close_file();
        assert_eq!(std::fs::metadata(dir.join("test.log.1")).unwrap().len(), MAX_SIZE - 10);
        assert!(!dir.join("test.log.1.1").exists());
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.ends_with(" hello rotation\n"));
        assert!(text.len() < MAX_SIZE as usize / 2);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn line_keeps_file_when_next_line_reaches_max_size_exactly() {
        let _guard = FILE_TESTS.lock().unwrap_or_else(PoisonError::into_inner);
        let dir = test_dir("limit");
        let path = dir.join("test.log");
        std::fs::create_dir_all(&dir).unwrap();
        let line_len = timestamp().len() + " hello\n".len();
        std::fs::write(&path, vec![b'x'; MAX_SIZE as usize - line_len]).unwrap();
        to_file(path.clone()).unwrap();
        line(format_args!("hello"));
        close_file();
        assert!(!dir.join("test.log.1").exists());
        assert_eq!(std::fs::metadata(&path).unwrap().len(), MAX_SIZE);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn line_counts_written_lines_towards_max_size() {
        let _guard = FILE_TESTS.lock().unwrap_or_else(PoisonError::into_inner);
        let dir = test_dir("count");
        let path = dir.join("test.log");
        std::fs::create_dir_all(&dir).unwrap();
        let line_len = timestamp().len() + " hello\n".len();
        std::fs::write(&path, vec![b'x'; MAX_SIZE as usize - line_len]).unwrap();
        to_file(path.clone()).unwrap();
        line(format_args!("hello"));
        line(format_args!("hello"));
        close_file();
        assert_eq!(std::fs::metadata(dir.join("test.log.1")).unwrap().len(), MAX_SIZE);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), line_len as u64);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn line_starts_a_new_file_at_max_size_when_the_log_file_was_deleted() {
        let _guard = FILE_TESTS.lock().unwrap_or_else(PoisonError::into_inner);
        let dir = test_dir("deleted");
        let path = dir.join("test.log");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(&path, vec![b'x'; MAX_SIZE as usize - 10]).unwrap();
        to_file(path.clone()).unwrap();
        std::fs::remove_file(&path).unwrap();
        line(format_args!("after delete"));
        close_file();
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.ends_with(" after delete\n"));
        assert!(!dir.join("test.log.1").exists());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn to_file_creates_missing_folder_and_appends_across_reopens() {
        let _guard = FILE_TESTS.lock().unwrap_or_else(PoisonError::into_inner);
        let dir = test_dir("to-file");
        let path = dir.join("nested/test.log");
        to_file(path.clone()).unwrap();
        line(format_args!("first"));
        to_file(path.clone()).unwrap();
        line(format_args!("second"));
        close_file();
        let text = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<_> = text.lines().map(|l| &l[timestamp().len() + 1..]).collect();
        assert_eq!(lines, ["first", "second"]);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
