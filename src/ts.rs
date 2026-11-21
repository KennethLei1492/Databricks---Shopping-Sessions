//! Timestamps (rules.md "Timestamps"): F1 `YYYY-MM-DDTHH:MM:SSZ`, F2 `...+HH:MM`, F3 ASCII digits.

pub const SECOND: i64 = 1_000;
pub const DAY: i64 = 86_400_000;

pub const fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let day_of_year = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year - 719_468
}

const TABLE_FIRST_YEAR: u32 = 1900;
const TABLE_YEARS: usize = 300;

/// Days since the epoch of the first of each month, 1900-01 to 2199-12 (built at compile time).
static MONTH_START: [i32; TABLE_YEARS * 12] = {
    let mut t = [0i32; TABLE_YEARS * 12];
    let mut i = 0;
    while i < TABLE_YEARS * 12 {
        t[i] = days_from_civil(TABLE_FIRST_YEAR as i64 + (i / 12) as i64, (i % 12) as i64 + 1, 1) as i32;
        i += 1;
    }
    t
};

#[inline(always)]
fn days(year: u32, month: u32, day: u32) -> i64 {
    let at = (year.wrapping_sub(TABLE_FIRST_YEAR) as usize) * 12 + month as usize - 1;
    match MONTH_START.get(at) {
        Some(&first) if year >= TABLE_FIRST_YEAR => first as i64 + day as i64 - 1,
        _ => days_from_civil(year as i64, month as i64, day as i64),
    }
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

#[cfg(test)]
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
        let head: &[u8; 20] = b[..20].try_into().unwrap();
        return civil_fast(head, &b[19..]);
    }
    if n == 0 {
        None
    } else if n <= 18 {
        // At most 18 digits cannot overflow; validity is checked once for the whole value.
        let mut v: u64 = 0;
        let mut bad = false;
        for &c in b {
            let d = c.wrapping_sub(b'0');
            bad |= d > 9;
            v = v.wrapping_mul(10).wrapping_add(d as u64);
        }
        (!bad).then_some(v as i64)
    } else {
        parse_digits(b)
    }
}

/// `parse(b)`, where `w` holds the 16 bytes starting at `b` (bytes past the end of `b` are
/// arbitrary): F3 of up to 16 digits and the head of F1/F2 are parsed with SIMD.
#[cfg(all(target_feature = "avx2", target_feature = "bmi2"))]
#[inline(always)]
pub fn parse_wide(w: &[u8; 16], b: &[u8]) -> Option<i64> {
    let n = b.len();
    if (n == 20 || n == 25) && b[4] == b'-' {
        return civil_simd(b);
    }
    if (1..=16).contains(&n) {
        return digits16(w, n).map(|v| v as i64);
    }
    parse(b)
}

#[cfg(not(all(target_feature = "avx2", target_feature = "bmi2")))]
#[inline(always)]
pub fn parse_wide(_w: &[u8; 16], b: &[u8]) -> Option<i64> {
    parse(b)
}

/// The value of the `len` ASCII digits at the start of `w`, or None if any is not a digit.
#[cfg(all(target_feature = "avx2", target_feature = "bmi2"))]
#[inline(always)]
fn digits16(w: &[u8; 16], len: usize) -> Option<u64> {
    use std::arch::x86_64::*;
    // RIGHT[n]: shuffle that moves lanes 0..n to the top n lanes and zeroes the rest.
    static RIGHT: [[u8; 16]; 17] = {
        let mut t = [[0x80u8; 16]; 17];
        let mut n = 0;
        while n <= 16 {
            let mut i = 16 - n;
            while i < 16 {
                t[n][i] = (i - (16 - n)) as u8;
                i += 1;
            }
            n += 1;
        }
        t
    };
    // SAFETY: AVX2 is enabled at compile time (cfg above); loads are unaligned-safe.
    unsafe {
        let v = _mm_loadu_si128(w.as_ptr() as *const __m128i);
        let d = _mm_sub_epi8(v, _mm_set1_epi8(b'0' as i8));
        let ok = _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_min_epu8(d, _mm_set1_epi8(9)), d)) as u32;
        let need = (1u32 << len) - 1;
        if ok & need != need {
            return None;
        }
        let r = _mm_shuffle_epi8(d, _mm_loadu_si128(RIGHT[len].as_ptr() as *const __m128i));
        let two = _mm_maddubs_epi16(r, _mm_setr_epi8(10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1));
        let four = _mm_madd_epi16(two, _mm_setr_epi16(100, 1, 100, 1, 100, 1, 100, 1));
        let four16 = _mm_packus_epi32(four, four);
        let eight = _mm_madd_epi16(four16, _mm_setr_epi16(10000, 1, 10000, 1, 10000, 1, 10000, 1));
        let hi = _mm_cvtsi128_si32(eight) as u32 as u64;
        let lo = _mm_extract_epi32::<1>(eight) as u32 as u64;
        Some(hi * 100_000_000 + lo)
    }
}

/// (days since the epoch of the first of the month, days in the month), 1900-01 to 2199-12.
static MONTHS: [(i32, u32); TABLE_YEARS * 12] = {
    let mut t = [(0i32, 0u32); TABLE_YEARS * 12];
    let mut i = 0;
    while i < TABLE_YEARS * 12 {
        let (y, m) = (TABLE_FIRST_YEAR as i64 + (i / 12) as i64, (i % 12) as i64 + 1);
        let first = days_from_civil(y, m, 1);
        let next = if m == 12 { days_from_civil(y + 1, 1, 1) } else { days_from_civil(y, m + 1, 1) };
        t[i] = (first as i32, (next - first) as u32);
        i += 1;
    }
    t
};

/// F1 / F2 (`b` is 20 or 25 bytes): `-MM-DDTHH:MM:SS` is checked and converted in one 16-byte
/// SIMD pass (separators, digits, ranges, time of day), the year with a 4-byte integer trick, and
/// the date through one table entry.
#[cfg(all(target_feature = "avx2", target_feature = "bmi2"))]
#[inline(always)]
fn civil_simd(b: &[u8]) -> Option<i64> {
    use std::arch::x86_64::*;
    const SEPS: u32 = 0b0001_0010_0100_1001; // body bytes 0, 3, 6, 9, 12
    const DIGITS: u32 = 0b0110_1101_1011_0110; // body bytes 1-2, 4-5, 7-8, 10-11, 13-14
    let y = u32::from_le_bytes(b[..4].try_into().unwrap()).wrapping_sub(0x3030_3030);
    // A byte above 9 sets its top bit, in y itself or after adding 118.
    let y_bad = (y | y.wrapping_add(0x7676_7676)) & 0x8080_8080;
    let year = (y & 0xFF) * 1000 + (y >> 8 & 0xFF) * 100 + (y >> 16 & 0xFF) * 10 + (y >> 24);
    let body: &[u8; 16] = b[4..20].try_into().unwrap();
    // SAFETY: AVX2 (and so SSE4.1) is enabled at compile time (cfg above); loads are unaligned-safe.
    let (month, day, secs) = unsafe {
        let v = _mm_loadu_si128(body.as_ptr() as *const __m128i);
        let (dash, tee, colon) = (b'-' as i8, b'T' as i8, b':' as i8);
        let template = _mm_setr_epi8(dash, 0, 0, dash, 0, 0, tee, 0, 0, colon, 0, 0, colon, 0, 0, 0);
        let seps = _mm_movemask_epi8(_mm_cmpeq_epi8(v, template)) as u32;
        let d = _mm_sub_epi8(v, _mm_set1_epi8(b'0' as i8));
        let digits = _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_min_epu8(d, _mm_set1_epi8(9)), d)) as u32;
        if (seps & SEPS != SEPS) | (digits & DIGITS != DIGITS) | (y_bad != 0) {
            return None;
        }
        let gather = _mm_setr_epi8(1, 2, 4, 5, 7, 8, 10, 11, 13, 14, -128, -128, -128, -128, -128, -128);
        // u16 lanes: month, day, hour, minute, second, 0, 0, 0
        let pairs = _mm_maddubs_epi16(_mm_shuffle_epi8(d, gather), _mm_setr_epi8(10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 0, 0, 0, 0, 0, 0));
        // month 1..=12, day 1..=31, hour <= 23, minute <= 59, second <= 59: after subtracting the
        // minimum (wrapping), anything above the span leaves a non-zero saturated difference.
        let shifted = _mm_sub_epi16(pairs, _mm_setr_epi16(1, 1, 0, 0, 0, 0, 0, 0));
        let over = _mm_subs_epu16(shifted, _mm_setr_epi16(11, 30, 23, 59, 59, 0, 0, 0));
        if _mm_testz_si128(over, over) == 0 {
            return None;
        }
        // i32 lanes: 0, hour * 3600 + minute * 60, second, 0
        let tod = _mm_madd_epi16(pairs, _mm_setr_epi16(0, 0, 3600, 60, 1, 0, 0, 0));
        (
            _mm_extract_epi16::<0>(pairs) as u32,
            _mm_extract_epi16::<1>(pairs) as u32,
            (_mm_extract_epi32::<1>(tod) + _mm_extract_epi32::<2>(tod)) as i64,
        )
    };
    let at = (year.wrapping_sub(TABLE_FIRST_YEAR) as usize) * 12 + month as usize - 1;
    let days = match MONTHS.get(at) {
        Some(&(first, len)) => {
            if day > len {
                return None;
            }
            first as i64 + day as i64 - 1
        }
        None => {
            if day > days_in_month(year, month) {
                return None;
            }
            days_from_civil(year as i64, month as i64, day as i64)
        }
    };
    let offset_minutes: i64 = if b.len() == 20 {
        if b[19] != b'Z' {
            return None;
        }
        0
    } else {
        let z: &[u8; 6] = b[19..].try_into().ok()?;
        let sign = match z[0] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        let (a, b2, c, e) = (z[1].wrapping_sub(b'0'), z[2].wrapping_sub(b'0'), z[4].wrapping_sub(b'0'), z[5].wrapping_sub(b'0'));
        if (a > 9) | (b2 > 9) | (c > 9) | (e > 9) | (z[3] != b':') {
            return None;
        }
        let (oh, om) = (a as i64 * 10 + b2 as i64, c as i64 * 10 + e as i64);
        if oh > 23 || om > 59 {
            return None;
        }
        sign * (oh * 60 + om)
    };
    Some((days * 86_400 + secs) * SECOND - offset_minutes * 60 * SECOND)
}

/// `ingest_ts`, which rules.md guarantees is always valid F1: a 20-byte value is converted
/// without the separator, digit and range checks of `civil_simd`; any other length (never
/// produced) goes through `parse_wide`, so it is still rejected or read correctly.
#[cfg(all(target_feature = "avx2", target_feature = "bmi2"))]
#[inline(always)]
pub fn parse_trusted_f1(w: &[u8; 16], b: &[u8]) -> Option<i64> {
    use std::arch::x86_64::*;
    if b.len() != 20 {
        return parse_wide(w, b);
    }
    let y = u32::from_le_bytes(b[..4].try_into().unwrap()).wrapping_sub(0x3030_3030);
    let year = (y & 0xFF) * 1000 + (y >> 8 & 0xFF) * 100 + (y >> 16 & 0xFF) * 10 + (y >> 24);
    let body: &[u8; 16] = b[4..20].try_into().unwrap();
    // SAFETY: AVX2 (and so SSE4.1) is enabled at compile time (cfg above); loads are unaligned-safe.
    let (month, day, secs) = unsafe {
        let d = _mm_sub_epi8(_mm_loadu_si128(body.as_ptr() as *const __m128i), _mm_set1_epi8(b'0' as i8));
        let gather = _mm_setr_epi8(1, 2, 4, 5, 7, 8, 10, 11, 13, 14, -128, -128, -128, -128, -128, -128);
        let pairs = _mm_maddubs_epi16(_mm_shuffle_epi8(d, gather), _mm_setr_epi8(10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 0, 0, 0, 0, 0, 0));
        let tod = _mm_madd_epi16(pairs, _mm_setr_epi16(0, 0, 3600, 60, 1, 0, 0, 0));
        (
            _mm_extract_epi16::<0>(pairs) as u32,
            _mm_extract_epi16::<1>(pairs) as u32,
            (_mm_extract_epi32::<1>(tod) + _mm_extract_epi32::<2>(tod)) as i64,
        )
    };
    let at = (year.wrapping_sub(TABLE_FIRST_YEAR) as usize) * 12 + (month as usize).wrapping_sub(1);
    let days = match MONTHS.get(at) {
        Some(&(first, _)) => first as i64 + day as i64 - 1,
        None => return parse_wide(w, b),
    };
    Some((days * 86_400 + secs) * SECOND)
}

#[cfg(not(all(target_feature = "avx2", target_feature = "bmi2")))]
#[inline(always)]
pub fn parse_trusted_f1(w: &[u8; 16], b: &[u8]) -> Option<i64> {
    parse_wide(w, b)
}

/// F1 / F2 on a fixed-size head (no bounds checks); `zone` is `Z` or `+HH:MM` / `-HH:MM`.
#[inline(always)]
fn civil_fast(h: &[u8; 20], zone: &[u8]) -> Option<i64> {
    let d = |i: usize| h[i].wrapping_sub(b'0') as u32;
    let (y0, y1, y2, y3) = (d(0), d(1), d(2), d(3));
    let (mo0, mo1, d0, d1) = (d(5), d(6), d(8), d(9));
    let (h0, h1, mi0, mi1, s0, s1) = (d(11), d(12), d(14), d(15), d(17), d(18));
    let bad_digit = (y0 > 9) | (y1 > 9) | (y2 > 9) | (y3 > 9) | (mo0 > 9) | (mo1 > 9) | (d0 > 9) | (d1 > 9)
        | (h0 > 9) | (h1 > 9) | (mi0 > 9) | (mi1 > 9) | (s0 > 9) | (s1 > 9);
    let bad_sep = (h[7] != b'-') | (h[10] != b'T') | (h[13] != b':') | (h[16] != b':');
    if bad_digit | bad_sep {
        return None;
    }
    let year = y0 * 1000 + y1 * 100 + y2 * 10 + y3;
    let month = mo0 * 10 + mo1;
    let day = d0 * 10 + d1;
    let hour = h0 * 10 + h1;
    let minute = mi0 * 10 + mi1;
    let second = s0 * 10 + s1;
    if month.wrapping_sub(1) > 11 || day == 0 || day > days_in_month(year, month) || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let offset_minutes: i64 = if zone.len() == 1 {
        if zone[0] != b'Z' {
            return None;
        }
        0
    } else {
        let z: &[u8; 6] = zone.try_into().ok()?;
        let sign = match z[0] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        let (a, b2, c, e) = (z[1].wrapping_sub(b'0'), z[2].wrapping_sub(b'0'), z[4].wrapping_sub(b'0'), z[5].wrapping_sub(b'0'));
        if (a > 9) | (b2 > 9) | (c > 9) | (e > 9) | (z[3] != b':') {
            return None;
        }
        let (oh, om) = (a as i64 * 10 + b2 as i64, c as i64 * 10 + e as i64);
        if oh > 23 || om > 59 {
            return None;
        }
        sign * (oh * 60 + om)
    };
    let days = days(year, month, day);
    let local = (days * 86_400 + hour as i64 * 3_600 + minute as i64 * 60 + second as i64) * SECOND;
    Some(local - offset_minutes * 60 * SECOND)
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

/// The straightforward civil parser, kept as the oracle for `civil_fast` in tests.
#[cfg(test)]
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
    let days = days(year, month, day);
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
        // The month table agrees with the formula everywhere, including outside its range.
        for year in [0u32, 1, 1899, 1900, 1970, 2000, 2024, 2028, 2100, 2199, 2200, 9999] {
            for month in 1..=12u32 {
                for day in [1u32, 15, 28] {
                    assert_eq!(days(year, month, day), days_from_civil(year as i64, month as i64, day as i64));
                }
            }
        }
        assert_eq!(parse(b"0000-01-01T00:00:00Z"), Some(days_from_civil(0, 1, 1) * DAY));
        assert_eq!(parse(b"1970-01-01T00:00:00Z"), Some(0));
    }

    /// The unchecked ingest_ts path equals the checked parser on every valid F1 value.
    #[test]
    fn trusted_f1_matches_parse() {
        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let mut checked = 0;
        for i in 0..200_000u64 {
            let year = if i % 7 == 0 { [0u64, 1899, 1900, 2199, 2200, 9999][(i / 7 % 6) as usize] } else { 1900 + next() % 300 };
            let month = 1 + next() % 12;
            let dim = days_in_month(year as u32, month as u32) as u64;
            let day = if i % 5 == 0 { dim } else { 1 + next() % dim };
            let (h, m, s) = (next() % 24, next() % 60, next() % 60);
            let text = format!("{year:04}-{month:02}-{day:02}T{h:02}:{m:02}:{s:02}Z");
            let b = text.as_bytes();
            let mut w = [b'#'; 16];
            w.copy_from_slice(&b[..16]);
            let want = parse(b);
            assert!(want.is_some(), "{text}");
            assert_eq!(parse_trusted_f1(&w, b), want, "{text}");
            checked += 1;
        }
        // Other lengths go through the checked parser unchanged.
        for other in [&b"1791071890000"[..], b"2026-10-03T19:58:10-04:00", b"", b"2026-10-03T10:00:00"] {
            let mut w = [b'#'; 16];
            let n = other.len().min(16);
            w[..n].copy_from_slice(&other[..n]);
            assert_eq!(parse_trusted_f1(&w, other), parse(other));
        }
        assert_eq!(checked, 200_000);
    }

    #[test]
    fn fast_parser_matches_oracle() {
        let oracle = |b: &[u8]| {
            if (b.len() == 20 || b.len() == 25) && b[4] == b'-' {
                parse_civil(b)
            } else {
                parse_digits(b)
            }
        };
        let seeds: [&[u8]; 9] = [
            b"2026-10-03T23:58:10Z", b"2026-10-03T19:58:10-04:00", b"1791071890000", b"0001791071890000",
            b"2028-02-29T00:00:00+12:00", b"000000000000000001", b"1234567890123456789", b"2027-02-28T23:59:59Z",
            b"9223372036854775807",
        ];
        let alphabet = b"0123456789-+:TZtz .e";
        let mut x: u64 = 0x2545_F491_4F6C_DD1D;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for _ in 0..300_000 {
            let mut v = seeds[(next() % seeds.len() as u64) as usize].to_vec();
            for _ in 0..(next() % 4) {
                let at = (next() % v.len() as u64) as usize;
                v[at] = alphabet[(next() % alphabet.len() as u64) as usize];
            }
            if next() % 8 == 0 {
                v.truncate((next() % (v.len() as u64 + 1)) as usize);
            }
            assert_eq!(parse(&v), oracle(&v), "{:?}", String::from_utf8_lossy(&v));
        }
    }

    /// Every calendar and clock edge, against the straightforward parser.
    #[test]
    fn wide_parser_grid() {
        let check = |text: String| {
            let v = text.as_bytes();
            let mut w = [b'7'; 16];
            let n = v.len().min(16);
            w[..n].copy_from_slice(&v[..n]);
            assert_eq!(parse_wide(&w, v), parse_civil(v), "{text}");
        };
        for year in [0u32, 1899, 1900, 2000, 2023, 2024, 2027, 2028, 2100, 2199, 2200, 2400, 9999] {
            for month in 0..20 {
                for day in 0..40 {
                    check(format!("{year:04}-{month:02}-{day:02}T12:34:56Z"));
                    check(format!("{year:04}-{month:02}-{day:02}T00:00:00+05:30"));
                }
            }
        }
        for hour in 0..30 {
            for minute in [0, 1, 30, 59, 60, 61, 99] {
                for second in [0, 1, 59, 60, 99] {
                    check(format!("2026-10-03T{hour:02}:{minute:02}:{second:02}Z"));
                    check(format!("2026-10-03T{hour:02}:{minute:02}:{second:02}-10:00"));
                }
            }
        }
        for oh in 0..30 {
            for om in [0, 30, 45, 59, 60, 99] {
                check(format!("2028-02-29T23:59:59+{oh:02}:{om:02}"));
                check(format!("2028-02-29T00:00:00-{oh:02}:{om:02}"));
            }
        }
    }

    #[test]
    fn wide_parser_matches_scalar() {
        let seeds: [&[u8]; 10] = [
            b"2026-10-03T23:58:10Z", b"2026-10-03T19:58:10-04:00", b"1791071890000", b"0001791071890000",
            b"2028-02-29T00:00:00+12:00", b"1", b"9999999999999999", b"2027-02-28T23:59:59Z",
            b"12345678901234567", b"0000000000000000",
        ];
        let alphabet = b"0123456789-+:TZtz .e/";
        let mut x: u64 = 0xD1B5_4A32_D192_ED03;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for _ in 0..300_000 {
            let mut v = seeds[(next() % seeds.len() as u64) as usize].to_vec();
            for _ in 0..(next() % 3) {
                let at = (next() % v.len() as u64) as usize;
                v[at] = alphabet[(next() % alphabet.len() as u64) as usize];
            }
            if next() % 6 == 0 {
                v.truncate((next() % (v.len() as u64 + 1)) as usize);
            }
            // The window: the field, then arbitrary bytes.
            let mut w = [0u8; 16];
            for (i, b) in w.iter_mut().enumerate() {
                *b = if i < v.len() { v[i] } else { alphabet[(next() % alphabet.len() as u64) as usize] };
            }
            assert_eq!(parse_wide(&w, &v), parse(&v), "{:?}", String::from_utf8_lossy(&v));
        }
        assert!(parse("２０26-10-03T10:00:00Z".as_bytes()).is_none());
        assert!(parse("٢٥٠".as_bytes()).is_none());
    }
}
