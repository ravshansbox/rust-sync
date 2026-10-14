use std::time::{SystemTime, UNIX_EPOCH};

/// Current time as `2026-09-27 12:34:56Z`.
pub(super) fn timestamp() -> String {
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
}
