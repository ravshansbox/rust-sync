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
