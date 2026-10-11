//! Daemon log lines, each starting with a UTC timestamp. Lines go to stderr, or
//! to a log file that is rotated at `MAX_SIZE`, keeping one older file (`<name>.1`).

use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Mutex;
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
    let file = OpenOptions::new().create(true).append(true).open(&path)?;
    let size = file.metadata()?.len();
    *FILE.lock().unwrap_or_else(|e| e.into_inner()) = Some(LogFile { path, file, size });
    Ok(())
}

pub fn line(msg: std::fmt::Arguments) {
    let text = format!("{} {msg}\n", timestamp());
    let mut file = FILE.lock().unwrap_or_else(|e| e.into_inner());
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
    /// `rust-sync.log` -> `rust-sync.log.1` (replacing an older one), then start a new file.
    fn rotate(&mut self) -> std::io::Result<()> {
        let mut old = OsString::from(&self.path);
        old.push(".1");
        std::fs::rename(&self.path, &old)?;
        self.file = OpenOptions::new().create(true).append(true).open(&self.path)?;
        self.size = 0;
        Ok(())
    }
}

/// Current time as `2026-09-27 12:34:56Z`.
fn timestamp() -> String {
    let secs = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
    let (y, m, d) = civil_from_days((secs / 86_400) as i64);
    let s = secs % 86_400;
    format!("{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}Z", s / 3600, s % 3600 / 60, s % 60)
}

/// Days since 1970-01-01 -> (year, month, day). Howard Hinnant's `civil_from_days`.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

macro_rules! log {
    ($($arg:tt)*) => { $crate::log::line(format_args!($($arg)*)) };
}
pub(crate) use log;

#[cfg(test)]
mod tests {
    use super::*;

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
    fn timestamp_has_fixed_format() {
        let t = timestamp();
        assert_eq!(t.len(), 20);
        assert_eq!(&t[4..5], "-");
        assert_eq!(&t[10..11], " ");
        assert!(t.ends_with('Z'));
    }

    #[test]
    fn rotate_moves_file_aside_and_starts_empty() {
        let dir = std::env::temp_dir().join(format!("rust-sync-log-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.log");
        std::fs::write(&path, "old\n").unwrap();
        let file = OpenOptions::new().append(true).open(&path).unwrap();
        let mut log = LogFile { path: path.clone(), file, size: 4 };
        log.rotate().unwrap();
        log.file.write_all(b"new\n").unwrap();
        assert_eq!(log.size, 0);
        assert_eq!(std::fs::read_to_string(dir.join("test.log.1")).unwrap(), "old\n");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "new\n");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
