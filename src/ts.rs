//! Timestamps (rules.md "Timestamps"): F1 `YYYY-MM-DDTHH:MM:SSZ`, F2 `...+HH:MM`, F3 ASCII digits.

pub const SECOND: i64 = 1_000;
pub const DAY: i64 = 86_400_000;

pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let day_of_year = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year - 719_468
}

#[inline]
fn is_leap(year: u32) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

#[inline]
fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        2 => 28 + is_leap(year) as u32,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

#[inline(always)]
fn two(b: &[u8], at: usize) -> Option<u32> {
    let hi = b[at].wrapping_sub(b'0');
    let lo = b[at + 1].wrapping_sub(b'0');
    if hi < 10 && lo < 10 {
        Some(hi as u32 * 10 + lo as u32)
    } else {
        None
    }
}

/// Epoch milliseconds, or None for a bad form. An F3 value too large for i64 is also None:
/// it would be outside every window anyway, so the outcome is the same.
#[inline]
pub fn parse(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if (n == 20 || n == 25) && b[4] == b'-' {
        return parse_civil(b);
    }
    parse_digits(b)
}

#[inline]
fn parse_digits(b: &[u8]) -> Option<i64> {
    if b.is_empty() {
        return None;
    }
    let mut value: i64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        value = value.checked_mul(10)?.checked_add(d as i64)?;
    }
    Some(value)
}

#[inline]
fn parse_civil(b: &[u8]) -> Option<i64> {
    let year = two(b, 0)? * 100 + two(b, 2)?;
    if b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let month = two(b, 5)?;
    let day = two(b, 8)?;
    let hour = two(b, 11)?;
    let minute = two(b, 14)?;
    let second = two(b, 17)?;
    if !(1..=12).contains(&month)
        || day == 0
        || day > days_in_month(year, month)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return None;
    }
    let offset_minutes: i64 = if b.len() == 20 {
        if b[19] != b'Z' {
            return None;
        }
        0
    } else {
        let sign = match b[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        if b[22] != b':' {
            return None;
        }
        let oh = two(b, 20)?;
        let om = two(b, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        sign * (oh as i64 * 60 + om as i64)
    };
    let days = days_from_civil(year as i64, month as i64, day as i64);
    let local = (days * 86_400 + hour as i64 * 3_600 + minute as i64 * 60 + second as i64) * SECOND;
    Some(local - offset_minutes * 60 * SECOND)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forms() {
        let f1 = parse(b"2026-10-03T23:58:10Z").unwrap();
        assert_eq!(parse(b"2026-10-03T19:58:10-04:00"), Some(f1));
        assert_eq!(parse(b"1791071890000"), Some(f1));
        assert_eq!(parse(b"0001791071890000"), Some(f1));
        assert_eq!(parse(b"2026-10-04T01:30:00+02:00"), parse(b"2026-10-03T23:30:00Z"));
        assert_eq!(parse(b"2026-10-03T10:00:00-00:00"), parse(b"2026-10-03T10:00:00Z"));
        for bad in [
            &b""[..], b"2026-10-03 10:00:00Z", b"2026-10-03t10:00:00Z", b"2026-10-03T10:00:00z",
            b"2026-10-03T10:00:00", b"2026-10-03T10:00:00.000Z", b"1791071890000.0", b"1.79e12", b"-5",
            b"2026-13-03T10:00:00Z", b"2026-02-30T10:00:00Z", b"2027-02-29T10:00:00Z", b"2026-10-03T24:00:00Z",
            b"2026-10-03T10:00:60Z", b"2026-10-03T10:00:00+24:00", b"2026-10-03T10:00:00+05:60",
            b"2026-10-03T10:00:00+0200", b"999999999999999999999",
        ] {
            assert_eq!(parse(bad), None, "{:?}", String::from_utf8_lossy(bad));
        }
        assert!(parse(b"2028-02-29T10:00:00Z").is_some());
        assert!(parse("２０26-10-03T10:00:00Z".as_bytes()).is_none());
        assert!(parse("٢٥٠".as_bytes()).is_none());
    }
}
