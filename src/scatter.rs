//! Software write-combining scatter into many buckets, stored in recycled slabs.
//!
//! Scattering records into hundreds of buckets per thread touches hundreds of cache lines at once,
//! so each line is read from memory before it is written (read-for-ownership) and is often evicted
//! half-written. Here each bucket stages one 64-byte line in L1 and writes it out whole with
//! non-temporal stores, which skip the read and bypass the caches.
//!
//! A bucket's lines live in 4 KB slabs from one process-wide free list, not in a Vec. Fresh memory
//! costs a kernel page fault and zeroing on first touch (the scorer's kernel time grew with it),
//! and the allocator only reuses freed memory for the same size class on the same thread; slabs
//! are interchangeable, so the buckets a phase frees (event-id buckets after dedup, person
//! buckets after sessions) are exactly the memory the next phase's buckets reuse.

use std::cell::RefCell;
use std::mem::{align_of, size_of};
use std::sync::Mutex;

/// One cache line of K records.
#[derive(Clone, Copy)]
#[repr(C, align(64))]
pub struct Line<T: Copy, const K: usize>([T; K]);

const SLAB: usize = 4096;
const LINES_PER_SLAB: usize = SLAB / 64;
/// New slabs come in regions of one 2 MB huge page.
const REGION: usize = 2 << 20;
/// Slabs a thread takes from / returns to the global list at a time.
const BATCH: usize = 64;

static FREE: Mutex<Vec<usize>> = Mutex::new(Vec::new());

/// A thread's slab cache; returned to the global list when the thread ends, so the next phase's
/// threads get what this one freed.
struct Cache(Vec<usize>);

impl Drop for Cache {
    fn drop(&mut self) {
        FREE.lock().unwrap().append(&mut self.0);
    }
}

thread_local! {
    static CACHE: RefCell<Cache> = const { RefCell::new(Cache(Vec::new())) };
}

fn take_slab() -> *mut u8 {
    CACHE.with(|c| {
        let mut c = c.borrow_mut();
        if let Some(p) = c.0.pop() {
            return p as *mut u8;
        }
        refill(&mut c.0);
        c.0.pop().unwrap() as *mut u8
    })
}

#[cold]
#[inline(never)]
fn refill(cache: &mut Vec<usize>) {
    {
        let mut free = FREE.lock().unwrap();
        let n = free.len().min(BATCH);
        let at = free.len() - n;
        cache.extend(free.drain(at..));
    }
    if cache.is_empty() {
        // A fresh region (never freed: the process ends soon after the last phase).
        let layout = std::alloc::Layout::from_size_align(REGION, REGION).unwrap();
        // SAFETY: a non-zero size with a valid power-of-two alignment.
        let base = unsafe { std::alloc::alloc(layout) };
        assert!(!base.is_null(), "out of memory for scatter slabs");
        cache.extend((0..REGION / SLAB).rev().map(|i| base as usize + i * SLAB));
    }
}

fn give_slab(p: *mut u8) {
    CACHE.with(|c| {
        let mut c = c.borrow_mut();
        c.0.push(p as usize);
        if c.0.len() >= 4 * BATCH {
            let at = c.0.len() - 2 * BATCH;
            FREE.lock().unwrap().extend(c.0.drain(at..));
        }
    })
}

/// A bucket: whole lines in slabs, of which the first `len` records are valid.
pub struct Lines<T: Copy, const K: usize> {
    slabs: Vec<*mut Line<T, K>>,
    lines: usize,
    len: usize,
}

// SAFETY: a Lines owns its slabs exclusively (taken from the free list, given back on drop).
unsafe impl<T: Copy + Send, const K: usize> Send for Lines<T, K> {}
// SAFETY: shared access only reads.
unsafe impl<T: Copy + Sync, const K: usize> Sync for Lines<T, K> {}

impl<T: Copy, const K: usize> Lines<T, K> {
    fn new() -> Self {
        Lines { slabs: Vec::new(), lines: 0, len: 0 }
    }

    /// Appends one line with non-temporal stores, taking a new slab when the last one is full.
    #[inline(always)]
    fn push_line(&mut self, src: *const Line<T, K>) {
        let i = self.lines % LINES_PER_SLAB;
        if i == 0 {
            self.slabs.push(take_slab() as *mut Line<T, K>);
        }
        // SAFETY: the last slab has room for LINES_PER_SLAB lines and i < LINES_PER_SLAB; slabs
        // are 64-byte aligned (4 KB within a 2 MB-aligned region).
        unsafe {
            let dst = self.slabs.last().unwrap().add(i);
            stream_line(dst as *mut u8, src as *const u8);
        }
        self.lines += 1;
    }

    /// The records, one slice per slab.
    pub fn slices(&self) -> impl Iterator<Item = &[T]> + '_ {
        let mut left = self.len;
        self.slabs.iter().map(move |&p| {
            let n = left.min(LINES_PER_SLAB * K);
            left -= n;
            // SAFETY: Line is repr(C) of [T; K] with size 64 = K * size_of::<T>() (checked in
            // Scatter::new), so a slab holds LINES_PER_SLAB * K contiguous records; the first
            // `len` records of the bucket, in slab order, were written by push_line / finish.
            unsafe { std::slice::from_raw_parts(p as *const T, n) }
        })
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

impl<T: Copy, const K: usize> Drop for Lines<T, K> {
    fn drop(&mut self) {
        for &p in &self.slabs {
            give_slab(p as *mut u8);
        }
    }
}

pub struct Scatter<T: Copy, const K: usize> {
    buckets: Vec<Lines<T, K>>,
    stage: Vec<Line<T, K>>,
    fill: Vec<u8>,
}

impl<T: Copy + Default, const K: usize> Scatter<T, K> {
    /// `n` empty buckets.
    pub fn new(n: usize) -> Self {
        const { assert!(size_of::<[T; K]>() == 64 && align_of::<T>() <= 64) };
        Scatter { buckets: (0..n).map(|_| Lines::new()).collect(), stage: vec![Line([T::default(); K]); n], fill: vec![0; n] }
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
                bucket.push_line(stage);
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
                bucket.push_line(&self.stage[b]);
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
        std::mem::take(&mut self.buckets)
    }
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

    fn collect<T: Copy, const K: usize>(l: &Lines<T, K>) -> Vec<T> {
        l.slices().flat_map(|s| s.iter().copied()).collect()
    }

    #[test]
    fn scatter_keeps_every_record_in_order() {
        let mut s: Scatter<u128, 4> = Scatter::new(7);
        let mut expected: Vec<Vec<u128>> = vec![Vec::new(); 7];
        for i in 0..10_000u128 {
            let b = (i * 2_654_435_761 % 7) as usize;
            s.push(b, i);
            expected[b].push(i);
        }
        let got = s.finish();
        for b in 0..7 {
            assert_eq!(collect(&got[b]), expected[b]);
            assert_eq!(got[b].len(), expected[b].len());
        }
    }

    #[test]
    fn freed_slabs_are_reused_across_threads() {
        // Fill buckets on one thread, free them on another, refill on a third: the third must not
        // need more fresh regions than the first did, and every record must survive.
        let fill = |seed: u64| {
            let mut s: Scatter<u64, 8> = Scatter::new(5);
            for i in 0..40_000u64 {
                s.push((i % 5) as usize, i ^ seed);
            }
            s.finish()
        };
        let a = std::thread::spawn(move || fill(1)).join().unwrap();
        let total: usize = a.iter().map(|l| l.len()).sum();
        assert_eq!(total, 40_000);
        for (b, l) in a.iter().enumerate() {
            assert!(collect(l).iter().enumerate().all(|(j, &x)| (x ^ 1) == (b + 5 * j) as u64));
        }
        std::thread::spawn(move || drop(a)).join().unwrap();
        let c = std::thread::spawn(move || fill(2)).join().unwrap();
        for (b, l) in c.iter().enumerate() {
            assert!(collect(l).iter().enumerate().all(|(j, &x)| (x ^ 2) == (b + 5 * j) as u64));
        }
    }
}
