//! Exact string keys in a u64.
//!
//! Fast form (top 4 bits 0): a string of at most 10 bytes from `[0-9a-z]` (every generator ID:
//! `e`, `u` or `k` plus at most 8 base-36 digits) as left-aligned 6-bit codes, 1..=36 in byte
//! order and 0 as padding, first byte most significant. It is injective, and integer order equals
//! byte-wise order (a prefix pads with 0, so it sorts first). It fits in 60 bits.
//!
//! Slow form (top bit 1): any other string, through a sharded global interner. Rare in the data
//! (upper-cased dirt, hand-made edge cases); `cmp` falls back to the bytes.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

pub type Key = u64;
pub const EMPTY: Key = u64::MAX; // never produced: slow ids stay far below 2^63 - 1
const SLOW: u64 = 1 << 63;
const SHARDS: usize = 64;
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

struct Shard {
    ids: HashMap<Vec<u8>, u64>,
    strings: Vec<Vec<u8>>,
}

fn interner() -> &'static Vec<Mutex<Shard>> {
    static INTERNER: OnceLock<Vec<Mutex<Shard>>> = OnceLock::new();
    INTERNER.get_or_init(|| {
        (0..SHARDS).map(|_| Mutex::new(Shard { ids: HashMap::new(), strings: Vec::new() })).collect()
    })
}

fn shard_of(s: &[u8]) -> usize {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &c in s {
        h = (h ^ c as u64).wrapping_mul(0x100_0000_01b3);
    }
    (h % SHARDS as u64) as usize
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
        None => pack_slow(s),
    }
}

#[cold]
fn pack_slow(s: &[u8]) -> Key {
    let at = shard_of(s);
    let mut shard = interner()[at].lock().unwrap();
    let local = match shard.ids.get(s) {
        Some(&id) => id,
        None => {
            let id = shard.strings.len() as u64;
            shard.strings.push(s.to_vec());
            shard.ids.insert(s.to_vec(), id);
            id
        }
    };
    SLOW | (local * SHARDS as u64 + at as u64)
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
        let id = k & !SLOW;
        let shard = interner()[(id % SHARDS as u64) as usize].lock().unwrap();
        out.extend_from_slice(&shard.strings[(id / SHARDS as u64) as usize]);
    } else {
        for i in 0..MAX_FAST {
            let d = (k >> (6 * (MAX_FAST - 1 - i))) & 63;
            if d == 0 {
                break;
            }
            out.push(CHAR[d as usize]);
        }
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

#[inline(always)]
pub fn hash(k: Key) -> u64 {
    let mut x = k ^ 0x9E37_79B9_7F4A_7C15;
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^ (x >> 33)
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

    /// Starts loading the cache line where `k` would be, for a lookup a few iterations later.
    #[inline(always)]
    pub fn prefetch(&self, k: Key) {
        #[cfg(target_arch = "x86_64")]
        // SAFETY: prefetch is a hint; the address is inside the keys allocation.
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            _mm_prefetch(self.keys.as_ptr().add(self.slot(k)) as *const i8, _MM_HINT_T0);
        }
    }

    #[inline]
    pub fn get(&self, k: Key) -> Option<V> {
        let mut at = self.slot(k);
        loop {
            let found = self.keys[at];
            if found == k {
                return Some(self.vals[at]);
            }
            if found == EMPTY {
                return None;
            }
            at = (at + 1) & self.mask;
        }
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

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn iter(&self) -> impl Iterator<Item = (Key, V)> + '_ {
        self.keys.iter().zip(self.vals.iter()).filter(|(k, _)| **k != EMPTY).map(|(k, v)| (*k, *v))
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
