//! The CSV dialect of rules.md: only `\n` ends a line, the first line of every file is its header,
//! a field is quoted only as a whole, and a line with the wrong field count or a stray quote is
//! dropped. Files are read with pread and cut into line-aligned chunks for parallel parsing.

use memchr::{memchr, memchr_iter};
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

const NL: u8 = 10;

/// Input files read with `pread` into per-thread buffers (no mmap: page faults are costly in VMs).
pub struct Input {
    files: Vec<File>,
    sizes: Vec<usize>,
}

#[derive(Clone, Copy)]
pub struct Chunk {
    pub file: usize,
    pub start: usize,
    pub end: usize,
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
        let mut files = Vec::new();
        let mut sizes = Vec::new();
        for path in paths {
            let file = File::open(path).unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
            let len = file.metadata().map(|m| m.len() as usize).unwrap_or(0);
            if len > 0 {
                files.push(file);
                sizes.push(len);
            }
        }
        Input { files, sizes }
    }

    pub fn bytes(&self) -> usize {
        self.sizes.iter().sum()
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
                out.push(Chunk { file, start, end });
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

/// Splits `data[start..len]` into lines and fields with one bitmask pass; `data` must hold SLACK
/// bytes past `len`. A line is well-formed when it has exactly N-1 commas and every quote is the
/// first or last byte of a field that starts and ends with one (length >= 2): then the line's
/// quote count is exactly 2 per quoted field.
pub fn parse_records<const N: usize>(data: &[u8], len: usize, start: usize, f: &mut impl FnMut(&Rec<'_, N>)) {
    const { assert!(N >= 2 && N <= 16) };
    assert!(data.len() >= len + SLACK);
    let mut cuts = [0usize; 16];
    let mut commas = 0usize;
    let mut line_start = start;
    let mut quotes_before = 0u32; // quote bytes of the current line in earlier blocks
    let mut block_at = start;
    let mut tail = [0u8; 64];
    while block_at < len {
        let block: &[u8; 64] = if block_at + 64 <= len {
            data[block_at..block_at + 64].try_into().unwrap()
        } else {
            tail.fill(0);
            tail[..len - block_at].copy_from_slice(&data[block_at..len]);
            &tail
        };
        let (nl, comma, quote) = masks(block);
        let mut seps = nl | comma;
        let mut quotes_left = quote;
        while seps != 0 {
            let b = seps.trailing_zeros();
            seps &= seps - 1;
            let bit = 1u64 << b;
            let pos = block_at + b as usize;
            if comma & bit != 0 {
                if commas < N - 1 {
                    cuts[commas] = pos;
                }
                commas += 1;
            } else {
                let below = bit - 1;
                let q = quotes_before + (quotes_left & below).count_ones();
                quotes_left &= !(below | bit);
                quotes_before = 0;
                emit::<N>(data, line_start, pos, &cuts, commas, q, f);
                commas = 0;
                line_start = pos + 1;
            }
        }
        quotes_before += quotes_left.count_ones();
        block_at += 64;
    }
    // A last line without its newline is still a line.
    if line_start < len {
        emit::<N>(data, line_start, len, &cuts, commas, quotes_before, f);
    }
}

#[inline(always)]
fn emit<const N: usize>(
    data: &[u8],
    line_start: usize,
    line_end: usize,
    cuts: &[usize; 16],
    commas: usize,
    quotes: u32,
    f: &mut impl FnMut(&Rec<'_, N>),
) {
    if commas != N - 1 {
        return; // wrong field count; an empty line has 0 commas
    }
    let mut at = [(0usize, 0usize); N];
    let mut s = line_start;
    for (i, r) in at.iter_mut().enumerate() {
        let e = if i < N - 1 { cuts[i] } else { line_end };
        *r = (s, e);
        s = e + 1;
    }
    if quotes != 0 {
        // Strip fields that start with a quote (they must end with one and be at least 2 long).
        // Any other quote (inside, or only at the end) leaves the count unmatched.
        let mut stripped = 0u32;
        for r in at.iter_mut() {
            let (s, e) = *r;
            if e > s && data[s] == b'"' {
                if e - s < 2 || data[e - 1] != b'"' {
                    return;
                }
                *r = (s + 1, e - 1);
                stripped += 2;
            }
        }
        if stripped != quotes {
            return;
        }
    }
    f(&Rec { data, at });
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
        for _ in 0..2_000 {
            let len = (next() % 400) as usize;
            let data: Vec<u8> = (0..len).map(|_| alphabet[(next() % alphabet.len() as u64) as usize]).collect();
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

    #[test]
    fn simd_splitter_matches_simple_split() {
        for seed in 1..40 {
            check_agrees::<2>(seed);
            check_agrees::<4>(seed * 7919);
            check_agrees::<7>(seed * 104_729);
        }
    }
}
