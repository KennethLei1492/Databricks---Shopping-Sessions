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
}
