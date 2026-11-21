//! Exact string keys. A string of at most 15 bytes is packed into a u128 (bytes big-endian, zero
//! padding, length in the lowest byte) so integer order equals byte-wise order. Longer strings
//! (rare: only hand-made edge cases) go through a sharded global interner and carry 0xFF in the
//! lowest byte; `cmp` falls back to their bytes.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

pub type Key = u128;
pub const EMPTY: Key = u128::MAX; // never a valid key: short keys have length <= 15 in the low byte
const LONG: u128 = 0xFF;
const SHARDS: usize = 64;

struct Shard {
    ids: HashMap<Vec<u8>, u64>,
    strings: Vec<Vec<u8>>,
}

fn interner() -> &'static Vec<Mutex<Shard>> {
    static INTERNER: OnceLock<Vec<Mutex<Shard>>> = OnceLock::new();
    INTERNER.get_or_init(|| {
        (0..SHARDS)
            .map(|_| Mutex::new(Shard { ids: HashMap::new(), strings: Vec::new() }))
            .collect()
    })
}

fn long_shard(s: &[u8]) -> usize {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &c in s {
        h = (h ^ c as u64).wrapping_mul(0x100_0000_01b3);
    }
    (h % SHARDS as u64) as usize
}

#[inline(always)]
pub fn pack(s: &[u8]) -> Key {
    if s.len() <= 15 {
        let mut b = [0u8; 16];
        b[..s.len()].copy_from_slice(s);
        b[15] = s.len() as u8;
        u128::from_be_bytes(b)
    } else {
        pack_long(s)
    }
}

#[cold]
fn pack_long(s: &[u8]) -> Key {
    let shard_at = long_shard(s);
    let mut shard = interner()[shard_at].lock().unwrap();
    let local = match shard.ids.get(s) {
        Some(&id) => id,
        None => {
            let id = shard.strings.len() as u64;
            shard.strings.push(s.to_vec());
            shard.ids.insert(s.to_vec(), id);
            id
        }
    };
    ((local * SHARDS as u64 + shard_at as u64) as u128) << 8 | LONG
}

/// A key for lookups only: None when the string is long (lookup tables keep long strings apart).
#[inline(always)]
pub fn pack_short(s: &[u8]) -> Option<Key> {
    (s.len() <= 15).then(|| pack(s))
}

#[inline(always)]
pub fn is_long(k: Key) -> bool {
    k as u8 == 0xFF
}

pub fn bytes(k: Key) -> Vec<u8> {
    if is_long(k) {
        let id = (k >> 8) as u64;
        let shard = interner()[(id % SHARDS as u64) as usize].lock().unwrap();
        shard.strings[(id / SHARDS as u64) as usize].clone()
    } else {
        let b = k.to_be_bytes();
        b[..b[15] as usize].to_vec()
    }
}

/// Byte-wise order of the strings behind two keys.
#[inline]
pub fn cmp(a: Key, b: Key) -> Ordering {
    if !is_long(a) && !is_long(b) {
        a.cmp(&b)
    } else {
        bytes(a).cmp(&bytes(b))
    }
}

#[inline(always)]
pub fn hash(k: Key) -> u64 {
    let lo = k as u64;
    let hi = (k >> 64) as u64;
    let mut x = lo ^ hi.wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left(29);
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^ (x >> 33)
}

#[inline(always)]
pub fn split(k: Key) -> [u64; 2] {
    [(k >> 64) as u64, k as u64]
}

#[inline(always)]
pub fn join(a: [u64; 2]) -> Key {
    (a[0] as u128) << 64 | a[1] as u128
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

    #[inline]
    pub fn get(&self, k: Key) -> Option<V> {
        let mut at = hash(k) as usize & self.mask;
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

    /// The slot for k, inserting `default` first when absent. Returns (slot value, was present).
    #[inline]
    pub fn entry(&mut self, k: Key, default: V) -> (&mut V, bool) {
        if (self.len + 1) * 2 > self.keys.len() {
            self.grow();
        }
        let mut at = hash(k) as usize & self.mask;
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
            b"", b"a", b"a\0", b"ab", b"b", b"U1", b"u1", b" u1", b"u1 ", b"k1", b"k10", b"k2",
            b"exactly15bytes!", b"this one is longer than 15", b"this one is longer than 15!", b"\xff",
        ];
        for a in &words {
            for b in &words {
                assert_eq!(cmp(pack(a), pack(b)), a.cmp(b), "{:?} {:?}", a, b);
                assert_eq!(pack(a) == pack(b), a == b);
            }
            assert_eq!(bytes(pack(a)), a.to_vec());
            assert_ne!(pack(a), EMPTY);
        }
    }
}
