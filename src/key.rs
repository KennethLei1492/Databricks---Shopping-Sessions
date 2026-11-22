//! Exact string keys in a u64.
//!
//! Fast form (top 4 bits 0): a string of at most 10 bytes from `[0-9a-z]` (every generator ID:
//! `e`, `u` or `k` plus at most 8 base-36 digits) as left-aligned 6-bit codes, 1..=36 in byte
//! order and 0 as padding, first byte most significant. It is injective, and integer order equals
//! byte-wise order (a prefix pads with 0, so it sorts first). It fits in 60 bits.
//!
//! Slow form (top bit 1): any other string (one ID in eight carries a 32-character hex suffix),
//! as a 62-bit hash of its bytes. Event IDs, which are only compared for equality, use the hash
//! alone (`pack_wide_anon`). IDs whose bytes are needed later (user and click IDs: names, byte
//! order, output) are also recorded in a sharded dictionary keyed by the hash, which checks that
//! no two different strings share one; `cmp` and `bytes` read the strings from it.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::{Mutex, OnceLock};

pub type Key = u64;
pub const EMPTY: Key = u64::MAX; // never produced: a slow key's bit 62 is always 0
const SLOW: u64 = 1 << 63;
const SHARDS: usize = 256;
pub const MAX_FAST: usize = 10;

static CODE: [u8; 256] = {
    let mut t = [0u8; 256];
    let mut c = 0;
    while c < 10 {
        t[b'0' as usize + c] = 1 + c as u8;
        c += 1;
    }
    let mut c = 0;
    while c < 26 {
        t[b'a' as usize + c] = 11 + c as u8;
        c += 1;
    }
    t
};

static CHAR: [u8; 37] = *b"\x000123456789abcdefghijklmnopqrstuvwxyz";

/// A hasher for keys that are already hashes: the key itself.
#[derive(Default)]
struct KeyHasher(u64);

impl Hasher for KeyHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, _: &[u8]) {
        unreachable!("only u64 keys")
    }
    fn write_u64(&mut self, k: u64) {
        self.0 = k;
    }
}

/// One shard of the dictionary: key -> (offset, length) of its string in the shard's arena.
struct Shard {
    at: HashMap<Key, (u32, u32), BuildHasherDefault<KeyHasher>>,
    arena: Vec<u8>,
}

fn dict() -> &'static Vec<Mutex<Shard>> {
    static DICT: OnceLock<Vec<Mutex<Shard>>> = OnceLock::new();
    DICT.get_or_init(|| (0..SHARDS).map(|_| Mutex::new(Shard { at: HashMap::default(), arena: Vec::new() })).collect())
}

/// The shard of a slow key: high hash bits (the shard's map indexes by the low ones).
#[inline(always)]
fn shard_of(k: Key) -> usize {
    (k >> 48) as usize % SHARDS
}

/// 64-bit hash of a byte string: its length, then 8-byte words, each mixed in with a 128-bit
/// multiply folded to 64 bits.
#[inline]
pub fn hash_bytes(s: &[u8]) -> u64 {
    #[inline(always)]
    fn mix(x: u64, m: u64) -> u64 {
        let p = x as u128 * m as u128;
        (p >> 64) as u64 ^ p as u64
    }
    let mut h = mix(s.len() as u64 ^ 0x243F_6A88_85A3_08D3, 0x9E37_79B9_7F4A_7C15);
    let mut chunks = s.chunks_exact(8);
    for c in &mut chunks {
        h = mix(h ^ u64::from_le_bytes(c.try_into().unwrap()), 0xD6E8_FEB8_6659_FD93);
    }
    let rest = chunks.remainder();
    if !rest.is_empty() {
        let mut w = [0u8; 8];
        w[..rest.len()].copy_from_slice(rest);
        h = mix(h ^ u64::from_le_bytes(w), 0xD6E8_FEB8_6659_FD93);
    }
    mix(h, 0xA076_1D64_78BD_642F)
}

/// The slow-form key of `s`: the top bit, and 62 bits of its hash (bit 62 stays 0, so a slow key
/// is never EMPTY).
#[inline]
fn slow_key(s: &[u8]) -> Key {
    SLOW | (hash_bytes(s) >> 2)
}

/// The fast-form key, or None when the string does not fit it. Never interns. (Scalar reference.)
#[inline]
pub fn pack_fast(s: &[u8]) -> Option<Key> {
    if s.len() > MAX_FAST {
        return None;
    }
    let mut v: u64 = 0;
    let mut ok = true;
    for (i, &c) in s.iter().enumerate() {
        let d = CODE[c as usize];
        ok &= d != 0;
        v |= (d as u64) << (6 * (MAX_FAST - 1 - i));
    }
    ok.then_some(v)
}

/// The same as `pack_fast(&w[..len])`, from a 16-byte window that starts at the string (bytes
/// past `len` are ignored): one SIMD classification and two `pext`s instead of a byte loop.
#[cfg(all(target_feature = "avx2", target_feature = "bmi2"))]
#[inline(always)]
pub fn pack_fast_wide(w: &[u8; 16], len: usize) -> Option<Key> {
    use std::arch::x86_64::*;
    static LEN_MASK: [[u8; 16]; 17] = {
        let mut t = [[0u8; 16]; 17];
        let mut n = 0;
        while n <= 16 {
            let mut i = 0;
            while i < n {
                t[n][i] = 0xFF;
                i += 1;
            }
            n += 1;
        }
        t
    };
    if len > MAX_FAST {
        return None;
    }
    // SAFETY: AVX2/BMI2 are enabled at compile time (cfg above); loads are unaligned-safe.
    unsafe {
        let v = _mm_loadu_si128(w.as_ptr() as *const __m128i);
        let keep = _mm_loadu_si128(LEN_MASK[len].as_ptr() as *const __m128i);
        let d = _mm_sub_epi8(v, _mm_set1_epi8(b'0' as i8));
        let is_digit = _mm_cmpeq_epi8(_mm_min_epu8(d, _mm_set1_epi8(9)), d);
        let l = _mm_sub_epi8(v, _mm_set1_epi8(b'a' as i8));
        let is_lower = _mm_cmpeq_epi8(_mm_min_epu8(l, _mm_set1_epi8(25)), l);
        let valid = _mm_or_si128(is_digit, is_lower);
        // Every kept byte must be a valid character.
        if _mm_movemask_epi8(_mm_andnot_si128(valid, keep)) != 0 {
            return None;
        }
        let code = _mm_or_si128(
            _mm_and_si128(is_digit, _mm_add_epi8(d, _mm_set1_epi8(1))),
            _mm_and_si128(is_lower, _mm_add_epi8(l, _mm_set1_epi8(11))),
        );
        let code = _mm_and_si128(code, keep);
        let lo = _mm_cvtsi128_si64(code) as u64; // bytes 0..8
        let hi = _mm_extract_epi16::<4>(code) as u16; // bytes 8, 9
        let a = _pext_u64(lo.swap_bytes(), 0x3F3F_3F3F_3F3F_3F3F); // 48 bits, byte 0 on top
        let b = _pext_u64(hi.swap_bytes() as u64, 0x3F3F); // 12 bits, byte 8 on top
        Some(a << 12 | b)
    }
}

#[cfg(not(all(target_feature = "avx2", target_feature = "bmi2")))]
#[inline(always)]
pub fn pack_fast_wide(w: &[u8; 16], len: usize) -> Option<Key> {
    pack_fast(&w[..len.min(16)])
}

/// The key of replica `copy` of an ID (SS_REPLICATE benchmarking): the code of its second character rotated by
/// `copy` within 1..=36 (fast keys only; a bijection, so replicas keep their structure).
#[inline(always)]
pub fn salt(k: Key, copy: u8) -> Key {
    let code = (k >> 48) & 63;
    if copy == 0 || k >> 60 != 0 || code == 0 {
        return k;
    }
    let new = (code - 1 + copy as u64) % 36 + 1;
    (k & !(63 << 48)) | new << 48
}

#[inline(always)]
pub fn pack(s: &[u8]) -> Key {
    match pack_fast(s) {
        Some(k) => k,
        None => pack_slow(s),
    }
}

/// `pack` from a 16-byte window starting at the string.
#[inline(always)]
pub fn pack_wide(w: &[u8; 16], s: &[u8]) -> Key {
    match pack_fast_wide(w, s.len()) {
        Some(k) => k,
        None => pack_noted(s),
    }
}

/// A slow key whose string is noted on this thread (no lock) and reaches the dictionary at
/// `publish`.
fn pack_noted(s: &[u8]) -> Key {
    let k = slow_key(s);
    LOCAL.with(|l| l.borrow_mut().0.note(k, s));
    k
}

/// `pack_wide` for an ID that is only ever compared for equality (event IDs): a slow-form key is
/// the hash alone, with no dictionary entry, so `bytes` and `cmp` must never see it.
#[inline(always)]
pub fn pack_wide_anon(w: &[u8; 16], s: &[u8]) -> Key {
    match pack_fast_wide(w, s.len()) {
        Some(k) => k,
        None => slow_key(s),
    }
}

/// The slow-form key of `s`, with `s` recorded in the dictionary (see the module docs).
fn pack_slow(s: &[u8]) -> Key {
    let k = slow_key(s);
    record(k, s);
    k
}

/// Puts `s` in the dictionary under `k`, checking that a key already there names the same bytes.
fn record(k: Key, s: &[u8]) {
    let mut shard = dict()[shard_of(k)].lock().unwrap();
    match shard.at.get(&k) {
        Some(&(at, n)) => {
            let known = &shard.arena[at as usize..(at + n) as usize];
            assert!(known == s, "two different IDs share a 62-bit hash: {:?} {:?}", known, s);
        }
        None => {
            let at = shard.arena.len();
            shard.arena.extend_from_slice(s);
            shard.at.insert(k, (at as u32, s.len() as u32));
        }
    }
}

/// Slow keys a thread made with `pack_wide` and the strings behind them, kept on the thread so
/// parsing takes no lock: a set of the keys it has seen (open addressing, 0 = empty, which no
/// slow key is) and the first occurrence of each. When the thread ends they move to PENDING;
/// `publish` puts them in the dictionary before anything reads names.
#[derive(Default)]
struct Local {
    seen: Vec<u64>,
    n: usize,
    arena: Vec<u8>,
    entries: Vec<(Key, u32, u32)>,
}

impl Local {
    #[inline]
    fn note(&mut self, k: Key, s: &[u8]) {
        if 2 * (self.n + 1) > self.seen.len() {
            self.grow();
        }
        let mask = self.seen.len() - 1;
        let mut at = k as usize & mask;
        loop {
            let v = self.seen[at];
            if v == k {
                return;
            }
            if v == 0 {
                self.seen[at] = k;
                self.n += 1;
                self.entries.push((k, self.arena.len() as u32, s.len() as u32));
                self.arena.extend_from_slice(s);
                return;
            }
            at = (at + 1) & mask;
        }
    }

    #[cold]
    fn grow(&mut self) {
        let cap = (self.seen.len() * 2).max(1 << 12);
        let old = std::mem::replace(&mut self.seen, vec![0; cap]);
        let mask = cap - 1;
        for k in old.into_iter().filter(|&k| k != 0) {
            let mut at = k as usize & mask;
            while self.seen[at] != 0 {
                at = (at + 1) & mask;
            }
            self.seen[at] = k;
        }
    }
}

struct LocalCell(Local);

impl Drop for LocalCell {
    fn drop(&mut self) {
        if !self.0.entries.is_empty() {
            let mut local = std::mem::take(&mut self.0);
            local.seen = Vec::new();
            PENDING.lock().unwrap().push(local);
        }
    }
}

static PENDING: Mutex<Vec<Local>> = Mutex::new(Vec::new());

thread_local! {
    static LOCAL: std::cell::RefCell<LocalCell> = std::cell::RefCell::new(LocalCell(Local::default()));
}

/// Puts every pending string (from threads that have ended) in the
/// dictionary, on `threads` threads. Must run before `bytes`/`cmp`/`append_bytes` read a key made
/// by `pack_wide`.
pub fn publish(threads: usize) {
    let lists = std::mem::take(&mut *PENDING.lock().unwrap());
    let next = std::sync::atomic::AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for _ in 0..threads.max(1) {
            scope.spawn(|| loop {
                let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let Some(list) = lists.get(i) else { break };
                for &(k, at, n) in &list.entries {
                    record(k, &list.arena[at as usize..(at + n) as usize]);
                }
            });
        }
    });
}

#[inline(always)]
pub fn is_slow(k: Key) -> bool {
    k & SLOW != 0
}

pub fn bytes(k: Key) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    append_bytes(k, &mut out);
    out
}

/// Appends the string behind `k` to `out`.
#[inline]
pub fn append_bytes(k: Key, out: &mut Vec<u8>) {
    if is_slow(k) {
        let shard = dict()[shard_of(k)].lock().unwrap();
        let &(at, n) = shard.at.get(&k).expect("slow key without a dictionary entry");
        out.extend_from_slice(&shard.arena[at as usize..(at + n) as usize]);
    } else {
        // All ten codes into a stack buffer, then one copy of the non-padding prefix.
        let mut buf = [0u8; MAX_FAST];
        let mut n = 0;
        for (i, b) in buf.iter_mut().enumerate() {
            let d = ((k >> (6 * (MAX_FAST - 1 - i))) & 63) as usize;
            *b = CHAR[d];
            n += (d != 0) as usize;
        }
        out.extend_from_slice(&buf[..n]);
    }
}

/// Byte-wise order of the strings behind two keys.
#[inline]
pub fn cmp(a: Key, b: Key) -> Ordering {
    if (a | b) & SLOW == 0 {
        a.cmp(&b)
    } else {
        bytes(a).cmp(&bytes(b))
    }
}

/// A 64-bit mix of the key: one 128-bit multiply, folded. Every output bit depends on every key
/// bit, which matters because callers take different bit ranges for buckets, tables and bins.
/// (It runs several times per event, so it is kept to three instructions.)
#[inline(always)]
pub fn hash(k: Key) -> u64 {
    let m = (k ^ 0x9E37_79B9_7F4A_7C15) as u128 * 0xD6E8_FEB8_6659_FD93u128;
    (m >> 64) as u64 ^ m as u64
}

/// Open-addressing map from Key to a Copy value (linear probing, power-of-two capacity).
pub struct KeyMap<V: Copy> {
    keys: Vec<Key>,
    vals: Vec<V>,
    mask: usize,
    len: usize,
}

impl<V: Copy + Default> KeyMap<V> {
    pub fn with_capacity(n: usize) -> Self {
        let cap = (n.max(8) * 2).next_power_of_two();
        KeyMap { keys: vec![EMPTY; cap], vals: vec![V::default(); cap], mask: cap - 1, len: 0 }
    }

    #[inline(always)]
    fn slot(&self, k: Key) -> usize {
        hash(k) as usize & self.mask
    }

    /// The slot for k, inserting `default` first when absent. Returns (value, was present).
    #[inline]
    pub fn entry(&mut self, k: Key, default: V) -> (&mut V, bool) {
        if (self.len + 1) * 2 > self.keys.len() {
            self.grow();
        }
        let mut at = self.slot(k);
        loop {
            let found = self.keys[at];
            if found == k {
                return (&mut self.vals[at], true);
            }
            if found == EMPTY {
                self.keys[at] = k;
                self.vals[at] = default;
                self.len += 1;
                return (&mut self.vals[at], false);
            }
            at = (at + 1) & self.mask;
        }
    }

    fn grow(&mut self) {
        let mut bigger = KeyMap::with_capacity(self.keys.len());
        for (i, &k) in self.keys.iter().enumerate() {
            if k != EMPTY {
                *bigger.entry(k, self.vals[i]).0 = self.vals[i];
            }
        }
        *self = bigger;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_matches_bytes() {
        let words: Vec<&[u8]> = vec![
            b"", b"a", b"a\0", b"ab", b"b", b"U1", b"u1", b" u1", b"u1 ", b"k1", b"k10", b"k2", b"0", b"9", b"z",
            b"zzzzzzzzzzzz", b"zzzzzzzzzzzzz", b"e00000000000", b"e0000000000", b"uduffvac", b"u8w84o3t0",
            b"exactly15bytes!", b"this one is longer than 15", b"\xff", b"c_1", b"p1", b"P1",
        ];
        for a in &words {
            for b in &words {
                assert_eq!(cmp(pack(a), pack(b)), a.cmp(b), "{:?} {:?}", a, b);
                assert_eq!(pack(a) == pack(b), a == b);
            }
            assert_eq!(bytes(pack(a)), a.to_vec());
            assert_ne!(pack(a), EMPTY);
        }
        assert!(pack_fast(b"zzzzzzzzzz").unwrap() < SLOW);
        assert_eq!(pack_fast(b"zzzzzzzzzzz"), None);
        assert_eq!(pack_fast(b"U1"), None);
    }

    /// Keys from `pack_wide` on worker threads reach the dictionary at `publish`, once per string.
    #[test]
    fn noted_keys_published() {
        let words: Vec<String> = (0..20_000u64).map(|i| format!("k{:032x}", i.wrapping_mul(0x9E37_79B9_7F4A_7C15))).collect();
        let keys: Vec<Vec<Key>> = std::thread::scope(|scope| {
            let hs: Vec<_> = (0..4)
                .map(|t| {
                    let words = &words;
                    scope.spawn(move || {
                        words
                            .iter()
                            .skip(t)
                            .step_by(2)
                            .map(|w| {
                                let mut win = [0u8; 16];
                                win.copy_from_slice(&w.as_bytes()[..16]);
                                pack_wide(&win, w.as_bytes())
                            })
                            .collect()
                    })
                })
                .collect();
            hs.into_iter().map(|h| h.join().unwrap()).collect()
        });
        publish(4);
        for (t, ks) in keys.iter().enumerate() {
            for (i, &k) in ks.iter().enumerate() {
                assert_eq!(bytes(k), words[t + 2 * i].as_bytes().to_vec());
            }
        }
    }

    /// Long IDs (a letter and 32 hex digits, as one ID in eight in the data) take the slow form:
    /// equal strings give equal keys, different strings different keys, the bytes come back, and
    /// the anonymous form (event IDs) agrees with the recorded one.
    #[test]
    fn long_ids() {
        let mut x: u64 = 0x1234_5678_9ABC_DEF1;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let mut seen = std::collections::HashMap::new();
        for i in 0..100_000 {
            let s = format!("u{:016x}{:016x}", next(), next());
            let mut w = [0u8; 16];
            w.copy_from_slice(&s.as_bytes()[..16]);
            let k = pack_wide(&w, s.as_bytes());
            assert!(is_slow(k) && k != EMPTY);
            assert_eq!(pack_wide_anon(&w, s.as_bytes()), k);
            assert_eq!(pack(s.as_bytes()), k);
            assert_eq!(bytes(k), s.as_bytes().to_vec());
            assert!(seen.insert(k, s.clone()).is_none(), "distinct strings, distinct keys ({i})");
        }
        // Byte order against short IDs too.
        let a = pack(b"u0000000000000000000000000000000001");
        let b = pack(b"u1");
        assert_eq!(cmp(a, b), Ordering::Less);
        assert_eq!(cmp(b, a), Ordering::Greater);
    }

    /// Generator-like IDs (a letter plus base-36 digits) must spread evenly over every bit range
    /// the pipeline uses: buckets (bits 40..), tables (low bits), bins (bits 16..), filter (20..).
    #[test]
    fn hash_spreads_packed_ids() {
        const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
        let n = 400_000u64;
        for (shift, buckets) in [(40u32, 4096usize), (0, 4096), (16, 4096), (20, 4096), (58, 64)] {
            let mut counts = vec![0u32; buckets];
            for i in 0..n {
                // scrambled like the generator: sequential numbers through a 40-bit bijection
                let mut v = i.wrapping_mul(0x9E37_79B9_7F4B) & ((1 << 40) - 1);
                v ^= v >> 17;
                let mut id = vec![b'u'];
                let mut digits = Vec::new();
                loop {
                    digits.push(DIGITS[(v % 36) as usize]);
                    v /= 36;
                    if v == 0 {
                        break;
                    }
                }
                id.extend(digits.iter().rev());
                let h = hash(pack(&id));
                counts[(h >> shift) as usize % buckets] += 1;
            }
            let mean = n as f64 / buckets as f64;
            let (min, max) = (*counts.iter().min().unwrap() as f64, *counts.iter().max().unwrap() as f64);
            assert!(min > mean * 0.6 && max < mean * 1.5, "shift {shift}: min {min} max {max} mean {mean}");
        }
    }

    #[test]
    fn wide_matches_scalar() {
        let alphabet = b"0123456789abcdefghijklmnopqrstuvwxyzAZ_ ,\x00\xff";
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for _ in 0..200_000 {
            let mut w = [0u8; 16];
            for b in w.iter_mut() {
                *b = alphabet[(next() % alphabet.len() as u64) as usize];
            }
            // Mostly valid prefixes, so the fast path is exercised.
            let len = (next() % 14) as usize;
            if next() % 3 != 0 {
                for b in w[..len].iter_mut() {
                    *b = alphabet[(next() % 36) as usize];
                }
            }
            assert_eq!(pack_fast_wide(&w, len), pack_fast(&w[..len]), "{:?} {len}", w);
            if let Some(k) = pack_fast(&w[..len]) {
                assert_eq!(bytes(k), w[..len].to_vec());
            }
        }
    }
}
