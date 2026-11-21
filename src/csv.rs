//! The CSV dialect of rules.md: only `\n` ends a line, the first line of every file is its header,
//! a field is quoted only as a whole, and a line with the wrong field count or a stray quote is
//! dropped. Files are memory-mapped and cut into line-aligned chunks for parallel parsing.

use memchr::{memchr, memchr_iter};
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

const NL: u8 = 10;

/// Input files, memory-mapped and parsed in place (no copy into user buffers). Each chunk's
/// pages are mapped in one call (MADV_POPULATE_READ, ignored where unsupported) just before it is
/// parsed. `SS_MMAP=1` skips that call; `SS_MMAP=0` reads with `pread` into per-thread buffers,
/// which is also how the last bytes of each file are read (the parser needs SLACK bytes after
/// a chunk).
pub struct Input {
    files: Vec<File>,
    sizes: Vec<usize>,
    maps: Vec<Option<memmap2::Mmap>>,
    populate: bool,
}

#[derive(Clone, Copy)]
pub struct Chunk {
    pub file: usize,
    pub start: usize,
    pub end: usize,
    /// DIAGNOSTIC: which replica of the input this chunk stands for (SS_REPLICATE).
    pub copy: u8,
}

/// DIAGNOSTIC: how many times every input chunk is parsed (each time with shifted IDs).
pub fn replicate() -> u8 {
    std::env::var("SS_REPLICATE").ok().and_then(|v| v.parse().ok()).unwrap_or(1).max(1)
}

pub fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(entries) => entries
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "csv") && p.is_file())
            .collect(),
        Err(_) => Vec::new(),
    };
    files.sort();
    files
}

impl Input {
    pub fn open(paths: &[PathBuf]) -> Input {
        let mode: u32 = std::env::var("SS_MMAP").ok().and_then(|v| v.parse().ok()).unwrap_or(2);
        let mut files = Vec::new();
        let mut sizes = Vec::new();
        let mut maps = Vec::new();
        for path in paths {
            let file = File::open(path).unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
            let len = file.metadata().map(|m| m.len() as usize).unwrap_or(0);
            if len > 0 {
                // SAFETY: the input is a read-only mount that nothing modifies during the run.
                maps.push(if mode > 0 { unsafe { memmap2::Mmap::map(&file) }.ok() } else { None });
                files.push(file);
                sizes.push(len);
            }
        }
        Input { files, sizes, maps, populate: mode > 1 }
    }

    pub fn bytes(&self) -> usize {
        self.sizes.iter().sum::<usize>() * replicate() as usize
    }

    /// The offset just after the first newline at or after `at` (or the file size).
    fn line_end(&self, file: usize, mut at: usize) -> usize {
        let size = self.sizes[file];
        let mut probe = [0u8; 4096];
        while at < size {
            let n = (size - at).min(probe.len());
            self.files[file].read_exact_at(&mut probe[..n], at as u64).expect("read");
            if let Some(nl) = memchr(NL, &probe[..n]) {
                return at + nl + 1;
            }
            at += n;
        }
        size
    }

    /// Line-aligned chunks of about `target` bytes, largest first.
    pub fn chunks(&self, target: usize) -> Vec<Chunk> {
        let mut out = Vec::new();
        for file in 0..self.files.len() {
            let size = self.sizes[file];
            let mut start = 0;
            while start < size {
                let end = if start + target >= size { size } else { self.line_end(file, start + target) };
                for copy in 0..replicate() {
                    out.push(Chunk { file, start, end, copy });
                }
                start = end;
            }
        }
        out.sort_by_key(|c| std::cmp::Reverse(c.end - c.start));
        out
    }

    /// Calls `f` for every line of the chunk, skipping the file's header line. `buf` is reused.
    #[inline]
    pub fn lines(&self, chunk: Chunk, buf: &mut Vec<u8>, mut f: impl FnMut(&[u8])) {
        let len = chunk.end - chunk.start;
        if buf.len() < len {
            buf.resize(len, 0);
        }
        self.files[chunk.file].read_exact_at(&mut buf[..len], chunk.start as u64).expect("read");
        let data = &buf[..len];
        let mut at = 0;
        if chunk.start == 0 {
            match memchr(NL, data) {
                Some(nl) => at = nl + 1,
                None => return,
            }
        }
        let base = at;
        for nl in memchr_iter(NL, &data[base..]) {
            let end = base + nl;
            f(&data[at..end]);
            at = end + 1;
        }
        // A last line without its newline is still a line (the generator always ends with one).
        if at < data.len() {
            f(&data[at..]);
        }
    }

    /// Calls `f` with the unquoted fields of every well-formed N-field line of the chunk, skipping
    /// the file's header line. Same results as `lines` + `split`, in one SIMD pass.
    #[inline]
    pub fn records<const N: usize>(&self, chunk: Chunk, buf: &mut Vec<u8>, mut f: impl FnMut(&Rec<'_, N>)) {
        let len = chunk.end - chunk.start;
        // In place from the mapping when SLACK bytes after the chunk are still inside the file.
        if let Some(map) = &self.maps[chunk.file] {
            if chunk.end + SLACK <= self.sizes[chunk.file] {
                if self.populate {
                    let _ = map.advise_range(memmap2::Advice::PopulateRead, chunk.start, len + SLACK);
                }
                let data = &map[chunk.start..chunk.end + SLACK];
                let mut at = 0;
                if chunk.start == 0 {
                    match memchr(NL, &data[..len]) {
                        Some(nl) => at = nl + 1,
                        None => return,
                    }
                }
                parse_records::<N>(data, len, at, &mut f);
                return;
            }
        }
        if buf.len() < len + SLACK {
            buf.resize(len + SLACK, 0);
        }
        self.files[chunk.file].read_exact_at(&mut buf[..len], chunk.start as u64).expect("read");
        let mut at = 0;
        if chunk.start == 0 {
            match memchr(NL, &buf[..len]) {
                Some(nl) => at = nl + 1,
                None => return,
            }
        }
        parse_records::<N>(&buf[..len + SLACK], len, at, &mut f);
    }
}

/// Readable bytes after the parsed data, so a field can always be read as a 16-byte window.
pub const SLACK: usize = 64;

/// One well-formed line: field ranges into a buffer that has SLACK readable bytes past the data.
pub struct Rec<'a, const N: usize> {
    data: &'a [u8],
    at: [(usize, usize); N],
}

impl<'a, const N: usize> Rec<'a, N> {
    /// Field `i`, unquoted.
    #[inline(always)]
    pub fn get(&self, i: usize) -> &'a [u8] {
        let (s, e) = self.at[i];
        &self.data[s..e]
    }

    /// The length of field `i`, unquoted.
    #[inline(always)]
    pub fn len(&self, i: usize) -> usize {
        let (s, e) = self.at[i];
        e - s
    }

    /// 16 bytes starting at field `i` (bytes past the field are arbitrary).
    #[inline(always)]
    pub fn wide(&self, i: usize) -> &'a [u8; 16] {
        let s = self.at[i].0;
        self.data[s..s + 16].try_into().unwrap()
    }

    pub fn fields(&self) -> [&'a [u8]; N] {
        std::array::from_fn(|i| self.get(i))
    }
}

/// Bitmasks of newline, comma and quote bytes in 64 bytes.
#[cfg(target_feature = "avx2")]
#[inline(always)]
fn masks(block: &[u8; 64]) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    // SAFETY: the binary is compiled with AVX2 enabled (cfg above); loads are unaligned-safe.
    unsafe {
        let a = _mm256_loadu_si256(block.as_ptr() as *const __m256i);
        let b = _mm256_loadu_si256(block.as_ptr().add(32) as *const __m256i);
        let eq = |c: u8| -> u64 {
            let c = _mm256_set1_epi8(c as i8);
            let lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, c)) as u32 as u64;
            let hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, c)) as u32 as u64;
            lo | hi << 32
        };
        (eq(NL), eq(b','), eq(b'"'))
    }
}

#[cfg(not(target_feature = "avx2"))]
#[inline(always)]
fn masks(block: &[u8; 64]) -> (u64, u64, u64) {
    let (mut nl, mut comma, mut quote) = (0u64, 0u64, 0u64);
    for (i, &c) in block.iter().enumerate() {
        nl |= ((c == NL) as u64) << i;
        comma |= ((c == b',') as u64) << i;
        quote |= ((c == b'"') as u64) << i;
    }
    (nl, comma, quote)
}

/// `x` with bits `n` and above cleared; all of `x` when `n >= 64`.
#[inline(always)]
fn low_bits(x: u64, n: usize) -> u64 {
    #[cfg(target_feature = "bmi2")]
    // SAFETY: BMI2 is enabled at compile time (cfg above). BZHI keeps `x` for an index >= 64;
    // callers pass n < 256, the range of the index byte it reads.
    unsafe {
        std::arch::x86_64::_bzhi_u64(x, n as u32)
    }
    #[cfg(not(target_feature = "bmi2"))]
    if n >= 64 {
        x
    } else {
        x & ((1u64 << n) - 1)
    }
}

/// Splits `data[start..len]` into lines and fields; `data` must hold SLACK bytes past `len`.
/// A line is well-formed when it has exactly N-1 commas and every quote is the first or last
/// byte of a field that starts and ends with one (length >= 2): then the line's quote count is
/// exactly 2 per quoted field.
///
/// Line by line, from the bitmasks of the 128 bytes at the line's start: the newline is one
/// trailing-zero count, the field count one popcount, and the N-1 comma positions come out with
/// conditional moves, so a well-formed line runs no data-dependent branch or loop. A line with no
/// newline in its first 128 bytes (very long, or a file's last line without one) takes the simple
/// `split` path.
pub fn parse_records<const N: usize>(data: &[u8], len: usize, start: usize, f: &mut impl FnMut(&Rec<'_, N>)) {
    const { assert!(N >= 2 && N <= 16) };
    assert!(data.len() >= len + SLACK);
    let mut ls = start;
    let mut pad = [0u8; 128];
    while ls < len {
        // The window must not show bytes past `len` (another chunk's lines, or stale buffer).
        let window: &[u8; 128] = if ls + 128 <= len {
            data[ls..ls + 128].try_into().unwrap()
        } else {
            pad.fill(0);
            pad[..len - ls].copy_from_slice(&data[ls..len]);
            &pad
        };
        let (n0, c0, q0) = masks(window[..64].try_into().unwrap());
        // Short records (links, clicks) nearly always end in the first word; event lines are
        // split evenly around 64 bytes, so they always take both words (no branch to mispredict).
        let (n1, c1, q1) = if N >= 5 || n0 == 0 { masks(window[64..].try_into().unwrap()) } else { (0, 0, 0) };
        // One place hands records on (`f` is called from a single site, so it can be inlined).
        let mut at = [(0usize, 0usize); N];
        let ok;
        if n0 | n1 == 0 {
            let end = memchr(NL, &data[ls..len]).map_or(len, |p| ls + p);
            ok = slow_line::<N>(data, ls, end, &mut at);
            ls = end + 1;
        } else {
            let t0 = n0.trailing_zeros() as usize;
            let nl = t0 + if t0 == 64 { n1.trailing_zeros() as usize } else { 0 };
            let nl_hi = nl.saturating_sub(64);
            let mut lo = low_bits(c0, nl);
            let mut hi = low_bits(c1, nl_hi);
            let commas = (lo.count_ones() + hi.count_ones()) as usize;
            let (quoted, quotes_ok) = quoted_fields(lo, hi, low_bits(q0, nl), low_bits(q1, nl_hi), nl);
            // The next comma: from the low word while it has one, then from the high word, without
            // a branch (trailing_zeros of 0 is 64, and x & (x - 0) leaves x unchanged).
            // A quoted field (bit i of `quoted`) loses its first and last byte.
            let mut s = ls;
            for (i, r) in at.iter_mut().enumerate().take(N - 1) {
                let lo_empty = (lo == 0) as u64;
                let pos = lo.trailing_zeros() as usize + (hi.trailing_zeros() as usize & 0usize.wrapping_sub(lo_empty as usize));
                hi &= hi.wrapping_sub(lo_empty);
                lo &= lo.wrapping_sub(1);
                let e = ls + pos;
                let q = (quoted >> i) as usize & 1;
                *r = (s + q, e - q);
                s = e + 1;
            }
            let q = (quoted >> (N - 1)) as usize & 1;
            at[N - 1] = (s + q, ls + nl - q);
            ok = commas == N - 1 && quotes_ok;
            ls += nl + 1;
        }
        if ok {
            f(&Rec { data, at });
        }
    }
}

/// The bits of `x` selected by `mask`, packed into the low bits.
#[inline(always)]
fn pext(x: u64, mask: u64) -> u64 {
    #[cfg(target_feature = "bmi2")]
    // SAFETY: BMI2 is enabled at compile time (cfg above).
    unsafe {
        std::arch::x86_64::_pext_u64(x, mask)
    }
    #[cfg(not(target_feature = "bmi2"))]
    {
        let (mut out, mut m, mut k) = (0u64, mask, 0);
        while m != 0 {
            out |= (x >> m.trailing_zeros() & 1) << k;
            k += 1;
            m &= m - 1;
        }
        out
    }
}

/// Which fields of a line are wrapped in quotes (bit i = field i), and whether every quote in the
/// line is such a wrapper. Works on the line's 128-bit comma and quote masks (`*_lo` are bytes
/// 0..64 from the line start, `*_hi` bytes 64..128; all bits at or after the newline at `nl` are
/// already cleared), so it loads no bytes and has no loop over fields.
///
/// A field is fine when it has no quote, or starts and ends with one, is at least 2 long and has
/// none in between. So with ST = field starts and SEP = the separator ending each field:
///   opens  = quotes at a field start                          (bit at the field start)
///   closes = quotes immediately before a separator            (bit at the separator)
/// the line is fine when the fields that open are exactly the fields that close, every quote is
/// one or the other, and no quote is both (a field that is a single quote).
#[inline(always)]
fn quoted_fields(c_lo: u64, c_hi: u64, q_lo: u64, q_hi: u64, nl: usize) -> (u64, bool) {
    let nl_bit = 1u64 << (nl & 63);
    let sep_lo = c_lo | (nl_bit & ((nl < 64) as u64).wrapping_neg());
    let sep_hi = c_hi | (nl_bit & ((nl >= 64) as u64).wrapping_neg());
    let st_lo = c_lo << 1 | 1;
    let st_hi = c_hi << 1 | c_lo >> 63;
    let open_lo = q_lo & st_lo;
    let open_hi = q_hi & st_hi;
    // Quotes moved up one byte meet the separator that follows them.
    let close_lo = (q_lo << 1) & sep_lo;
    let close_hi = (q_hi << 1 | q_lo >> 63) & sep_hi;
    let opens = pext(open_lo, st_lo) | pext(open_hi, st_hi).wrapping_shl(st_lo.count_ones());
    let closes = pext(close_lo, sep_lo) | pext(close_hi, sep_hi).wrapping_shl(sep_lo.count_ones());
    // The closing quotes, back at their own positions.
    let closing_lo = close_lo >> 1 | close_hi << 63;
    let closing_hi = close_hi >> 1;
    let stray = (q_lo & !(open_lo | closing_lo)) | (q_hi & !(open_hi | closing_hi));
    // A quote that both opens and closes: its separator comes right after a field start.
    let lone = (close_lo & (st_lo << 1)) | (close_hi & (st_hi << 1 | st_lo >> 63));
    (opens, (opens == closes) & (stray == 0) & (lone == 0))
}

/// One line through the simple splitter (a very long line, or a last line without a newline).
#[cold]
fn slow_line<const N: usize>(data: &[u8], line_start: usize, line_end: usize, at: &mut [(usize, usize); N]) -> bool {
    match split::<N>(&data[line_start..line_end]) {
        Some(fields) => {
            let base = data.as_ptr() as usize;
            for (r, field) in at.iter_mut().zip(fields.iter()) {
                let s = field.as_ptr() as usize - base;
                *r = (s, s + field.len());
            }
            true
        }
        None => false,
    }
}

/// Splits a line into exactly N fields and strips whole-field quotes; None if malformed.
#[inline(always)]
pub fn split<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    let mut out: [&[u8]; N] = [&[]; N];
    let mut i = 0;
    let mut start = 0;
    for pos in memchr_iter(b',', line) {
        if i + 1 >= N {
            return None;
        }
        out[i] = &line[start..pos];
        i += 1;
        start = pos + 1;
    }
    if i + 1 != N {
        return None;
    }
    out[N - 1] = &line[start..];
    if memchr(b'"', line).is_some() {
        for field in out.iter_mut() {
            if memchr(b'"', field).is_some() {
                let n = field.len();
                if n < 2 || field[0] != b'"' || field[n - 1] != b'"' || memchr(b'"', &field[1..n - 1]).is_some() {
                    return None;
                }
                *field = &field[1..n - 1];
            }
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dialect() {
        assert_eq!(split::<2>(b"a,b"), Some([&b"a"[..], b"b"]));
        assert_eq!(split::<2>(b"\"a\",\"\""), Some([&b"a"[..], b""]));
        assert_eq!(split::<2>(b"\" a\",b\r"), Some([&b" a"[..], b"b\r"]));
        assert_eq!(split::<2>(b""), None);
        assert_eq!(split::<2>(b"a"), None);
        assert_eq!(split::<2>(b"a,b,c"), None);
        assert_eq!(split::<2>(b"a,b\""), None);
        assert_eq!(split::<2>(b"a,\""), None);
        assert_eq!(split::<2>(b"a,\"b\"c\""), None);
        assert_eq!(split::<2>(b"a,b\"c"), None);
        assert_eq!(split::<2>(b",u3"), Some([&b""[..], b"u3"]));
    }

    /// The SIMD splitter must agree with the simple line + split path on random dirty input.
    fn check_agrees<const N: usize>(seed: u64) {
        let alphabet = b"ab,,\"\"\n \r1";
        let mut x = seed | 1;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for case in 0..2_000 {
            // Every fourth case is long enough to span many 64-byte blocks.
            let len = if case % 4 == 3 { (next() % 12_000) as usize } else { (next() % 400) as usize };
            let mut data: Vec<u8> = (0..len).map(|_| alphabet[(next() % alphabet.len() as u64) as usize]).collect();
            if case % 2 == 1 {
                // Long lines: most newlines, commas and quotes become letters, so lines run
                // 60 to 300 bytes and cross the splitter's 64- and 128-byte boundaries.
                for c in data.iter_mut() {
                    let keep = match *c {
                        NL => next() % 8 == 0,
                        b',' => next() % 3 == 0,
                        b'"' => next() % 6 == 0,
                        _ => true,
                    };
                    if !keep {
                        *c = b'x';
                    }
                }
            }
            let start = (next() % (len as u64 + 1)) as usize;
            let mut expected: Vec<Vec<Vec<u8>>> = Vec::new();
            let body = &data[start..];
            let mut lines: Vec<&[u8]> = body.split(|&c| c == NL).collect();
            if body.ends_with(b"\n") || body.is_empty() {
                lines.pop();
            }
            for line in lines {
                if let Some(fields) = split::<N>(line) {
                    expected.push(fields.iter().map(|f| f.to_vec()).collect());
                }
            }
            let mut actual: Vec<Vec<Vec<u8>>> = Vec::new();
            let mut padded = data.clone();
            padded.extend_from_slice(&[b'"'; SLACK]);
            parse_records::<N>(&padded, data.len(), start, &mut |r: &Rec<'_, N>| {
                actual.push(r.fields().iter().map(|f| f.to_vec()).collect())
            });
            assert_eq!(actual, expected, "data {:?} start {start}", String::from_utf8_lossy(&data));
        }
    }

    /// Mostly well-formed lines (the generator's shape: some fields quoted, a few lines damaged),
    /// so the block splitter's fast path carries most lines.
    fn check_agrees_clean<const N: usize>(seed: u64) {
        let mut x = seed | 1;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for _ in 0..300 {
            let mut data: Vec<u8> = Vec::new();
            let lines = (next() % 400) as usize;
            for _ in 0..lines {
                let fields = if next() % 50 == 0 { (next() % (N as u64 + 2)) as usize } else { N };
                for i in 0..fields {
                    if i > 0 {
                        data.push(b',');
                    }
                    let flen = (next() % 24) as usize;
                    let quoted = next() % 5 == 0;
                    if quoted {
                        data.push(b'"');
                    }
                    for _ in 0..flen {
                        data.push(b"abcxyz0189-:T Z"[(next() % 15) as usize]);
                    }
                    if quoted {
                        data.push(b'"');
                    }
                    // Damage: a stray or missing quote, or a quoted comma.
                    match next() % 300 {
                        0 => data.push(b'"'),
                        1 if data.last() == Some(&b'"') => {
                            data.pop();
                        }
                        2 => data.extend_from_slice(b"\"x,y\""),
                        _ => {}
                    }
                }
                if next() % 200 != 0 {
                    data.push(NL);
                }
            }
            let start = if next() % 2 == 0 { 0 } else { (next() % (data.len() as u64 + 1)) as usize };
            let body = &data[start..];
            let mut lines: Vec<&[u8]> = body.split(|&c| c == NL).collect();
            if body.ends_with(b"\n") || body.is_empty() {
                lines.pop();
            }
            let expected: Vec<Vec<Vec<u8>>> =
                lines.iter().filter_map(|l| split::<N>(l)).map(|f| f.iter().map(|x| x.to_vec()).collect()).collect();
            let mut padded = data.clone();
            padded.extend_from_slice(&[b'"'; SLACK]);
            let mut actual: Vec<Vec<Vec<u8>>> = Vec::new();
            parse_records::<N>(&padded, data.len(), start, &mut |r: &Rec<'_, N>| {
                actual.push(r.fields().iter().map(|f| f.to_vec()).collect())
            });
            assert!(expected.len() > lines.len() / 2 || lines.len() < 20, "generator should be mostly clean: {} of {} lines, N {N}", expected.len(), lines.len());
            assert_eq!(actual, expected, "clean blocks: data {:?} start {start}", String::from_utf8_lossy(&data));
        }
    }

    #[test]
    fn block_splitter_matches_on_mostly_clean_input() {
        for seed in 1..30 {
            check_agrees_clean::<2>(seed * 31);
            check_agrees_clean::<4>(seed * 7907);
            check_agrees_clean::<7>(seed * 104_723);
        }
    }

    #[test]
    fn simd_splitter_matches_simple_split() {
        for seed in 1..40 {
            check_agrees::<2>(seed);
            check_agrees::<4>(seed * 7919);
            check_agrees::<7>(seed * 104_729);
        }
    }
}
