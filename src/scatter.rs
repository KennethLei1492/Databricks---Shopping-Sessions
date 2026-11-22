//! Software write-combining scatter into many buckets.
//!
//! Scattering records into hundreds of buckets per thread touches hundreds of cache lines at once,
//! so each line is read from memory before it is written (read-for-ownership) and is often evicted
//! half-written. Here each bucket stages one 64-byte line in L1 and writes it out whole with
//! non-temporal stores, which skip the read and bypass the caches.

use std::mem::{align_of, size_of};

/// One cache line of K records.
#[derive(Clone, Copy)]
#[repr(C, align(64))]
pub struct Line<T: Copy, const K: usize>([T; K]);

/// A bucket: whole lines, of which the first `len` records are valid.
pub struct Lines<T: Copy, const K: usize> {
    lines: Vec<Line<T, K>>,
    len: usize,
}

impl<T: Copy, const K: usize> Lines<T, K> {
    pub fn as_slice(&self) -> &[T] {
        // SAFETY: Line is repr(C) of [T; K] with size 64 = K * size_of::<T>() (checked in
        // Scatter::new), so the lines are a contiguous [T] of lines.len() * K records, of which
        // the first `len` are initialized values (the rest are zeros or older values of T).
        unsafe { std::slice::from_raw_parts(self.lines.as_ptr() as *const T, self.len) }
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

pub struct Scatter<T: Copy, const K: usize> {
    buckets: Vec<Lines<T, K>>,
    stage: Vec<Line<T, K>>,
    fill: Vec<u8>,
}

impl<T: Copy + Default, const K: usize> Scatter<T, K> {
    /// `n` buckets with room for about `per_bucket` records each before any reallocation.
    pub fn new(n: usize, per_bucket: usize) -> Self {
        const { assert!(size_of::<[T; K]>() == 64 && align_of::<T>() <= 64) };
        let lines = per_bucket.div_ceil(K);
        Scatter {
            buckets: (0..n).map(|_| Lines { lines: Vec::with_capacity(lines), len: 0 }).collect(),
            stage: vec![Line([T::default(); K]); n],
            fill: vec![0; n],
        }
    }

    /// Adds `x` to bucket `b` (`b < n`, which is checked only in debug builds: callers compute
    /// it with a multiply-shift that cannot exceed n - 1).
    #[inline(always)]
    pub fn push(&mut self, b: usize, x: T) {
        debug_assert!(b < self.fill.len());
        // SAFETY: b < n = fill.len() = stage.len() = buckets.len() (all made in `new`), and
        // fill[b] < K always (it wraps to 0 when it reaches K).
        unsafe {
            let fill = self.fill.get_unchecked_mut(b);
            let f = *fill as usize;
            let stage = self.stage.get_unchecked_mut(b);
            *stage.0.get_unchecked_mut(f) = x;
            if f + 1 == K {
                *fill = 0;
                let bucket = self.buckets.get_unchecked_mut(b);
                if bucket.lines.len() == bucket.lines.capacity() {
                    grow(&mut bucket.lines);
                }
                // The destination is 64-byte aligned (Line is align(64)) and inside the
                // capacity checked above; 64 bytes are written before the length grows.
                let dst = bucket.lines.as_mut_ptr().add(bucket.lines.len());
                stream_line(dst as *mut u8, stage as *const Line<T, K> as *const u8);
                bucket.lines.set_len(bucket.lines.len() + 1);
                bucket.len += K;
            } else {
                *fill = (f + 1) as u8;
            }
        }
    }

    /// Flushes the partial lines and returns the buckets.
    pub fn finish(mut self) -> Vec<Lines<T, K>> {
        for (b, bucket) in self.buckets.iter_mut().enumerate() {
            let f = self.fill[b] as usize;
            if f > 0 {
                bucket.lines.push(self.stage[b]);
                bucket.len += f;
            }
        }
        // Non-temporal stores are weakly ordered: make them visible before the buckets are
        // handed to other threads.
        #[cfg(target_arch = "x86_64")]
        // SAFETY: a store fence has no preconditions.
        unsafe {
            std::arch::x86_64::_mm_sfence()
        };
        self.buckets
    }
}

/// Room for more lines in a bucket (rare: capacities are estimated with headroom).
#[cold]
#[inline(never)]
fn grow<L>(lines: &mut Vec<L>) {
    lines.reserve(lines.len().max(16));
}

/// Copies 64 bytes from `src` to the 64-byte aligned `dst` with non-temporal stores.
#[inline(always)]
unsafe fn stream_line(dst: *mut u8, src: *const u8) {
    #[cfg(target_feature = "avx2")]
    {
        use std::arch::x86_64::*;
        let a = _mm256_loadu_si256(src as *const __m256i);
        let b = _mm256_loadu_si256(src.add(32) as *const __m256i);
        _mm256_stream_si256(dst as *mut __m256i, a);
        _mm256_stream_si256(dst.add(32) as *mut __m256i, b);
    }
    #[cfg(not(target_feature = "avx2"))]
    std::ptr::copy_nonoverlapping(src, dst, 64);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scatter_keeps_every_record_in_order() {
        let mut s: Scatter<u128, 4> = Scatter::new(7, 3);
        let mut expected: Vec<Vec<u128>> = vec![Vec::new(); 7];
        for i in 0..10_000u128 {
            let b = (i * 2_654_435_761 % 7) as usize;
            s.push(b, i);
            expected[b].push(i);
        }
        let got = s.finish();
        for b in 0..7 {
            assert_eq!(got[b].as_slice(), &expected[b][..]);
            assert_eq!(got[b].len(), expected[b].len());
        }
    }
}
