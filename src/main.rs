//! Shopping Sessions pipeline. See rules.md for the rules and DESIGN.md for the plan.
//!
//! Stages (data-parallel over NCPU threads; buckets are handed over through taken Mutex slots):
//!   1. lookups + manifest
//!   2. links: parallel hash-partitioned dense IDs, serial union-find on u32 arrays; then parse
//!      events into buckets by hash(event_id) and clicks into per-thread lists
//!   3. per event-id bucket: dedup (min ingest_ts), keep valid survivors, map user -> person
//!      (prefetched), bucket by hash(person) as packed u128 (person | ts | price,category);
//!      click lists: map user -> person, bucket by hash(person)
//!   4. per person bucket, largest first: sort, sessionize, credit + assists, aggregate
//!   5. write Parquet

mod csv;
mod key;
mod out;
mod scatter;
mod ts;

use key::{pack, Key, KeyMap};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Instant;
use ts::DAY;

// mimalloc keeps freed memory for reuse instead of returning large blocks to the OS (glibc
// munmap/mmap churn costs page faults and serializes threads on the mmap lock).
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const GAP: i64 = 1_800_000;
const SEVEN_DAYS: i64 = 7 * DAY;
// L2-sized read chunks: the buffer is still in cache when the parser reads it, instead of
// making a round trip through memory between pread and parsing.
const CHUNK: usize = 256 << 10;
const INVALID: u32 = u32::MAX;
const NONE: u32 = u32::MAX;
const NO_CATEGORY: u16 = u16::MAX;
const PREFETCH: usize = 16;

/// Chunk size; SS_CHUNK overrides it so tests can force many chunk boundaries.
fn chunk_size() -> usize {
    std::env::var("SS_CHUNK").ok().and_then(|v| v.parse().ok()).unwrap_or(CHUNK)
}

/// A parsed event copy. `ts == INVALID` marks a copy that fails validation (it still competes in
/// dedup). `pc` is `price << 8 | category` for a purchase, else 0.
#[derive(Clone, Copy)]
struct EvRec {
    eid: Key,
    user: Key,
    ingest: u32, // seconds, offset so that ordering is preserved
    ts: u32,     // ms since window_start
    pc: u32,
}

/// A valid click. `person` holds the user ID until links are known. `ts` is ms since
/// window_start - 7 days.
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct ClickRec {
    person: Key,
    cid: Key,
    ts: u32,
    camp: u32,
    _pad: u64, // 32 bytes: two records per cache line in the write-combining scatter
}

/// Exact lookup of IDs from a small clean file. IDs of the form `<prefix><n>` (n without leading
/// zeros) are found by array index; any other ID goes through a hash map.
struct IdLookup {
    prefix: Vec<u8>,
    // The prefix as the low bytes of a little-endian word, and the mask of those bytes (for
    // prefixes of at most 8 bytes; pmask = 0 otherwise, which turns the fast path off).
    pword: u64,
    pmask: u64,
    dense: Vec<u32>,
    other: HashMap<Vec<u8>, u32>,
}

const DENSE_LIMIT: usize = 50_000_000;

fn canonical_number(s: &[u8], prefix: &[u8]) -> Option<usize> {
    let rest = s.strip_prefix(prefix)?;
    if rest.is_empty() || rest.len() > 9 || rest[0] == b'0' {
        return None;
    }
    let mut n = 0usize;
    for &c in rest {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        n = n * 10 + d as usize;
    }
    Some(n)
}

impl IdLookup {
    fn build(rows: &[(&[u8], u32)]) -> IdLookup {
        let prefix: Vec<u8> = rows.first().map_or(Vec::new(), |r| r.0.iter().take_while(|c| !c.is_ascii_digit()).copied().collect());
        let (pword, pmask) = if (1..=8).contains(&prefix.len()) {
            let mut w = [0u8; 8];
            w[..prefix.len()].copy_from_slice(&prefix);
            (u64::from_le_bytes(w), u64::MAX >> (64 - 8 * prefix.len()))
        } else {
            (0, 0)
        };
        let mut lookup = IdLookup { prefix, pword, pmask, dense: Vec::new(), other: HashMap::new() };
        for &(id, v) in rows {
            match canonical_number(id, &lookup.prefix).filter(|&n| n < DENSE_LIMIT) {
                Some(n) => {
                    if lookup.dense.len() <= n {
                        lookup.dense.resize(n + 1, NONE);
                    }
                    lookup.dense[n] = v;
                }
                None => {
                    lookup.other.insert(id.to_vec(), v);
                }
            }
        }
        lookup
    }

    /// `get(s)`, where `w` holds the 16 bytes starting at `s`: the prefix is one masked word
    /// compare and the number a branch-free digit loop; anything else goes through `get`.
    #[inline(always)]
    fn get_wide(&self, w: &[u8; 16], s: &[u8]) -> Option<u32> {
        let pl = self.prefix.len();
        let word = u64::from_le_bytes(w[..8].try_into().unwrap());
        if self.pmask != 0 && s.len() > pl && s.len() - pl <= 9 && word & self.pmask == self.pword && s[pl] != b'0' {
            let mut n = 0usize;
            let mut ok = true;
            for &c in &s[pl..] {
                let d = c.wrapping_sub(b'0');
                ok &= d <= 9;
                n = n.wrapping_mul(10).wrapping_add(d as usize);
            }
            if ok && n < self.dense.len() {
                let v = self.dense[n];
                return (v != NONE).then_some(v);
            }
        }
        self.get(s)
    }

    #[inline]
    fn get(&self, s: &[u8]) -> Option<u32> {
        match canonical_number(s, &self.prefix) {
            Some(n) if n < self.dense.len() => Some(self.dense[n]).filter(|&v| v != NONE),
            _ => self.other.get(s).copied(),
        }
    }
}

struct Ctx {
    ws: i64,
    we: i64,
    ingest_base_s: i64,
    click_base: i64,
    products: IdLookup,
    unknown: u32,
    categories: Vec<Vec<u8>>,
    campaigns: IdLookup,
    campaign_ids: Vec<Vec<u8>>,
    channels: Vec<Vec<u8>>,
    day0: i64,
    ndays: usize,
}

fn log(start: Instant, what: &str) {
    eprintln!("[{:>8.3}s] {what}", start.elapsed().as_secs_f64());
}

/// Runs `work(state, task)` for tasks 0..n on `threads` threads, pulling tasks dynamically.
/// Logs the phase's utilization and its longest task, to expose stragglers and barriers.
#[repr(C)]
struct Rusage {
    utime: [i64; 2],
    stime: [i64; 2],
    rest: [i64; 2],
    _idrss_isrss: [i64; 2],
    minflt: i64,
    rest2: [i64; 9],
}

extern "C" {
    fn getrusage(who: i32, usage: *mut Rusage) -> i32;
}

/// (user s, kernel s, minor page faults) of the whole process so far.
fn cpu_now() -> (f64, f64, i64) {
    // SAFETY: getrusage(RUSAGE_SELF) with a valid out pointer to a struct of its size.
    unsafe {
        let mut ru: Rusage = std::mem::zeroed();
        getrusage(0, &mut ru);
        (ru.utime[0] as f64 + ru.utime[1] as f64 / 1e6, ru.stime[0] as f64 + ru.stime[1] as f64 / 1e6, ru.minflt)
    }
}

/// (anonymous, file-backed) resident memory of the process in MB, from /proc/self/status.
fn rss_mb() -> (u64, u64) {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let field = |name: &str| {
        status.lines().find(|l| l.starts_with(name)).and_then(|l| l.split_whitespace().nth(1)).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0) / 1024
    };
    (field("RssAnon:"), field("RssFile:") + field("RssShmem:"))
}

fn parallel<S: Send>(
    name: &str,
    threads: usize,
    n: usize,
    init: impl Fn() -> S + Sync,
    work: impl Fn(&mut S, usize) + Sync,
) -> Vec<S> {
    let next = AtomicUsize::new(0);
    let cpu0 = cpu_now();
    let t0 = Instant::now();
    let results: Vec<(S, f64, f64, usize, f64, f64)> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                scope.spawn(|| {
                    let spawned = t0.elapsed().as_secs_f64();
                    let mut state = init();
                    let started = t0.elapsed().as_secs_f64();
                    let (mut busy, mut longest, mut longest_task) = (0f64, 0f64, 0usize);
                    loop {
                        let task = next.fetch_add(1, Ordering::Relaxed);
                        if task >= n {
                            break;
                        }
                        let t = Instant::now();
                        work(&mut state, task);
                        let d = t.elapsed().as_secs_f64();
                        busy += d;
                        if d > longest {
                            longest = d;
                            longest_task = task;
                        }
                    }
                    (state, busy, longest, longest_task, started, spawned)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().expect("worker panicked")).collect()
    });
    let wall = t0.elapsed().as_secs_f64();
    let busy: f64 = results.iter().map(|r| r.1).sum();
    let (longest, longest_task) = results.iter().map(|r| (r.2, r.3)).fold((0f64, 0), |a, b| if b.0 > a.0 { b } else { a });
    let slowest_start = results.iter().map(|r| r.4).fold(0f64, f64::max);
    let slowest_spawn = results.iter().map(|r| r.5).fold(0f64, f64::max);
    let slowest_init = results.iter().map(|r| r.4 - r.5).fold(0f64, f64::max);
    let cpu1 = cpu_now();
    let (anon, file) = rss_mb();
    eprintln!(
        "  phase {name}: {n} tasks, wall {wall:.3}s, utilization {:.0}%, user {:.3}s, kernel {:.3}s, faults {}k, rss anon {anon} MB file {file} MB, longest task #{longest_task} {longest:.3}s, slowest thread start {slowest_start:.3}s (spawn {:.3}s, init {:.3}s)",
        100.0 * busy / (threads as f64 * wall).max(1e-9),
        cpu1.0 - cpu0.0,
        cpu1.1 - cpu0.1,
        (cpu1.2 - cpu0.2) / 1000,
        slowest_spawn,
        slowest_init
    );
    results.into_iter().map(|r| r.0).collect()
}

#[inline(always)]
fn bucket(k: Key, n: usize) -> usize {
    // Hash bits 40..64 scaled to 0..n by a multiply and a shift (n <= 4096, so no overflow). A
    // `%` by the runtime `n` compiles to a 64-bit DIV: 40-90 cycles on Cascade Lake, at least
    // twice per event.
    debug_assert!(n <= 1 << 16);
    (((key::hash(k) >> 40) * n as u64) >> 24) as usize
}

fn read_small(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// Rows of a small clean two-column file, header skipped.
fn two_columns(data: &[u8]) -> Vec<(&[u8], &[u8])> {
    let mut rows = Vec::new();
    for (i, line) in data.split(|&c| c == b'\n').enumerate() {
        if i == 0 || line.is_empty() {
            continue;
        }
        if let Some([a, b]) = csv::split::<2>(line) {
            rows.push((a, b));
        }
    }
    rows
}

fn manifest_value(text: &str, name: &str) -> i64 {
    let at = text.find(&format!("\"{name}\"")).unwrap_or_else(|| panic!("manifest: no {name}"));
    let rest = &text[at + name.len() + 2..];
    let rest = &rest[rest.find(':').expect("manifest colon") + 1..];
    let open = rest.find('"').expect("manifest quote");
    let close = open + 1 + rest[open + 1..].find('"').expect("manifest quote");
    ts::parse(rest[open + 1..close].as_bytes()).unwrap_or_else(|| panic!("manifest: bad {name}"))
}

const LINK_PARTS: usize = 64;

/// Partition of a key for the links maps: the top hash bits, independent of `bucket` (bits
/// 40..) and of KeyMap slots (low bits).
#[inline(always)]
fn link_part(k: Key) -> usize {
    (key::hash(k) >> 58) as usize
}

/// Person names for linked IDs that are not their person's name: one open-addressing table
/// with key and name in the same slot (one cache miss per hit), behind a bitmap small enough to
/// stay in cache that answers "not linked" for most other IDs without touching the table.
struct Persons {
    // LINK_PARTS (64) sub-tables, partition = the top 6 hash bits (= link_part), linear probing
    // inside a sub-table; the filter has the same layout. A slot's index is the hash's top bits
    // (partition, then the bits below it), so it is one shift: `h >> sshift`; likewise a filter
    // bit's: `h >> fsh`. Partitions are built by separate tasks with no shared writes.
    slots: Vec<[u64; 2]>,
    sshift: u32,
    pmask: usize,
    filter: Vec<u64>,
    fsh: u32,
    len: usize,
}

impl Persons {
    /// Builds the table from each partition's (non-name ID, name) pairs, one task per partition.
    fn build(per: Vec<Vec<(Key, Key)>>, threads: usize) -> Persons {
        assert_eq!(per.len(), LINK_PARTS);
        const { assert!(LINK_PARTS == 64) };
        let len: usize = per.iter().map(|p| p.len()).sum();
        let max = per.iter().map(|p| p.len()).max().unwrap_or(0);
        let pslots = (2 * max).next_power_of_two().max(16);
        let pbits = (16 * max).next_power_of_two().max(64);
        let mut slots: Vec<[u64; 2]> = Vec::with_capacity(LINK_PARTS * pslots);
        let mut filter: Vec<u64> = vec![0; LINK_PARTS * pbits / 64];
        {
            let slot_ptr = SyncPtr(slots.as_mut_ptr());
            let filter_ptr = SyncPtr(filter.as_mut_ptr());
            parallel("links-table", threads, LINK_PARTS, || (), |_, p| {
                // SAFETY: partition p owns slots [p * pslots, (p + 1) * pslots) and filter words
                // [p * pbits / 64, (p + 1) * pbits / 64); the slot vector's capacity covers all
                // partitions, and every slot is written (EMPTY) before any is read.
                unsafe {
                    let part = std::slice::from_raw_parts_mut(slot_ptr.get().add(p * pslots), pslots);
                    part.fill([key::EMPTY, 0]);
                    let bits = std::slice::from_raw_parts_mut(filter_ptr.get().add(p * pbits / 64), pbits / 64);
                    for &(k, name) in &per[p] {
                        let h = key::hash(k);
                        debug_assert_eq!(link_part(k), p);
                        let fb = (h >> (58 - pbits.trailing_zeros())) as usize & (pbits - 1);
                        bits[fb / 64] |= 1 << (fb % 64);
                        let mut at = (h >> (58 - pslots.trailing_zeros())) as usize & (pslots - 1);
                        while part[at][0] != key::EMPTY {
                            at = (at + 1) & (pslots - 1);
                        }
                        part[at] = [k, name];
                    }
                }
            });
        }
        // SAFETY: every partition filled its whole slot range above.
        unsafe { slots.set_len(LINK_PARTS * pslots) };
        Persons { slots, sshift: 58 - pslots.trailing_zeros(), pmask: pslots - 1, filter, fsh: 58 - pbits.trailing_zeros(), len }
    }

    /// Home slot of hash `h` (below slots.len(): 6 + log2(slots per partition) bits).
    #[inline(always)]
    fn home(&self, h: u64) -> usize {
        (h >> self.sshift) as usize
    }

    /// Filter bit of hash `h` (below 64 * filter.len()).
    #[inline(always)]
    fn fbit(&self, h: u64) -> usize {
        (h >> self.fsh) as usize
    }

    /// Whether the filter says `h`'s key may be linked.
    #[inline(always)]
    fn maybe(&self, h: u64) -> bool {
        let fb = self.fbit(h);
        // SAFETY: fb / 64 < filter.len() (see fbit).
        unsafe { *self.filter.get_unchecked(fb / 64) >> (fb % 64) & 1 != 0 }
    }

    #[inline(always)]
    fn get(&self, k: Key) -> Key {
        let h = key::hash(k);
        if !self.maybe(h) {
            return k;
        }
        let mut at = self.home(h);
        loop {
            // SAFETY: at < slots.len() (home, then wrapping inside its partition).
            let s = unsafe { *self.slots.get_unchecked(at) };
            if s[0] == k {
                return s[1];
            }
            if s[0] == key::EMPTY {
                return k;
            }
            // The next slot of the same sub-table (wrapping inside it).
            at = (at & !self.pmask) | ((at + 1) & self.pmask);
        }
    }

    /// First stage of a lookup pipeline: the filter word of `k`.
    #[inline(always)]
    fn prefetch_filter(&self, k: Key) {
        let fb = self.fbit(key::hash(k));
        // SAFETY: fb / 64 < filter.len() (see fbit).
        prefetch(unsafe { self.filter.get_unchecked(fb / 64) });
    }

    /// Second stage: the table slot of `k`, if the filter says it may be linked.
    #[inline(always)]
    fn prefetch_slot(&self, k: Key) {
        let h = key::hash(k);
        if self.maybe(h) {
            // SAFETY: home(h) < slots.len().
            prefetch(unsafe { self.slots.get_unchecked(self.home(h)) });
        }
    }

    fn len(&self) -> usize {
        self.len
    }
}

/// Starts loading the cache line of `x`, for a use a few iterations later.
#[inline(always)]
fn prefetch<T>(x: &T) {
    #[cfg(target_arch = "x86_64")]
    // SAFETY: prefetch is only a hint and `x` is a valid reference.
    unsafe {
        use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
        _mm_prefetch(x as *const T as *const i8, _MM_HINT_T0);
    }
}

struct PartIds {
    keys: Vec<Key>,
    assign: Vec<(u32, u32)>, // (pair slot, local id)
}

/// Links -> person names, on the whole thread pool:
///   1. parse link files in parallel
///   2. scatter (key, pair slot) into LINK_PARTS hash partitions
///   3. per partition, in parallel: dense local IDs from a cache-sized map
///   4. serial union-find (by size, path halving) over plain u32 arrays
///   5. each component's name is its byte-wise smallest key
///   6. per partition, in parallel: the map from non-name IDs to names
fn build_links(data: &Path, threads: usize) -> Persons {
    let t0 = Instant::now();
    let input = csv::Input::open(&csv::list_csv(&data.join("links")));
    let chunks = input.chunks(chunk_size().min(1 << 20));
    struct LinkState {
        buf: Vec<u8>,
        pairs: Vec<(Key, Key)>,
    }
    let parsed = parallel(
        "links-parse",
        threads,
        chunks.len(),
        || LinkState { buf: Vec::new(), pairs: Vec::new() },
        |st, i| {
            let LinkState { buf, pairs } = st;
            let copy = chunks[i].copy;
            input.records::<2>(chunks[i], buf, |r| {
                let [a, b] = r.fields();
                if !a.is_empty() && !b.is_empty() {
                    if cfg!(feature = "scale") && copy != 0 {
                        pairs.push((key::salt(pack(a), copy), key::salt(pack(b), copy)));
                    } else {
                        pairs.push((pack(a), pack(b)));
                    }
                }
            });
        },
    );
    let lists: Vec<Vec<(Key, Key)>> = parsed.into_iter().map(|s| s.pairs).collect();
    let mut offsets = Vec::with_capacity(lists.len());
    let mut total = 0usize;
    for l in &lists {
        offsets.push(total);
        total += l.len();
    }
    assert!(2 * total < u32::MAX as usize, "too many links");

    let scattered = parallel(
        "links-scatter",
        threads,
        lists.len(),
        || (0..LINK_PARTS).map(|_| Vec::<(Key, u32)>::new()).collect::<Vec<_>>(),
        |parts, li| {
            let base = 2 * offsets[li];
            for (i, &(a, b)) in lists[li].iter().enumerate() {
                parts[link_part(a)].push((a, (base + 2 * i) as u32));
                parts[link_part(b)].push((b, (base + 2 * i + 1) as u32));
            }
        },
    );
    drop(lists);
    let part_slots = regroup(scattered, LINK_PARTS);

    let done = parallel("links-ids", threads, LINK_PARTS, Vec::new, |out: &mut Vec<(usize, PartIds)>, p| {
        let entries = take_all(&part_slots[p]);
        let mut map: KeyMap<u32> = KeyMap::with_capacity(entries.len() / 2 + 16);
        let mut keys = Vec::new();
        let mut assign = Vec::with_capacity(entries.len());
        for &(k, slot) in &entries {
            let next = keys.len() as u32;
            let (v, present) = map.entry(k, next);
            if !present {
                keys.push(k);
            }
            assign.push((slot, *v));
        }
        out.push((p, PartIds { keys, assign }));
    });
    let mut parts: Vec<Option<PartIds>> = (0..LINK_PARTS).map(|_| None).collect();
    for list in done {
        for (p, ids) in list {
            parts[p] = Some(ids);
        }
    }
    let parts: Vec<PartIds> = parts.into_iter().map(|p| p.expect("partition")).collect();
    let t_ids = t0.elapsed().as_secs_f64();

    let mut base = vec![0u32; LINK_PARTS + 1];
    for p in 0..LINK_PARTS {
        base[p + 1] = base[p] + parts[p].keys.len() as u32;
    }
    let n_ids = base[LINK_PARTS] as usize;
    // Partitions own disjoint pair slots and disjoint id ranges, so they fill in parallel.
    let mut slot_id = vec![0u32; 2 * total];
    let mut keys: Vec<Key> = vec![0; n_ids];
    {
        let slot_ptr = SyncPtr(slot_id.as_mut_ptr());
        let key_ptr = SyncPtr(keys.as_mut_ptr());
        parallel("links-fill", threads, LINK_PARTS, || (), |_, p| {
            let part = &parts[p];
            let b = base[p];
            for &(slot, local) in &part.assign {
                // SAFETY: each pair slot is assigned by exactly one partition; slot < 2 * total.
                unsafe { *slot_ptr.get().add(slot as usize) = b + local };
            }
            // SAFETY: [base[p], base[p + 1]) is this partition's own range of ids.
            unsafe { std::ptr::copy_nonoverlapping(part.keys.as_ptr(), key_ptr.get().add(b as usize), part.keys.len()) };
        });
    }
    drop(parts);
    let t_fill = t0.elapsed().as_secs_f64();

    // Union-find over all pairs on all threads (lock-free, see uf_union).
    const RANGE: usize = 32 * 1024;
    let parent: Vec<AtomicU32> = (0..n_ids as u32).map(AtomicU32::new).collect();
    let pair_tasks = total.div_ceil(RANGE).max(1);
    parallel("links-union", threads, pair_tasks, || (), |_, t| {
        let (s, e) = (t * RANGE, ((t + 1) * RANGE).min(total));
        for i in s..e {
            uf_union(&parent, slot_id[2 * i], slot_id[2 * i + 1]);
        }
    });
    drop(slot_id);
    let t_union = t0.elapsed().as_secs_f64();

    // Roots, and each component's name: its byte-wise smallest key. Fast-form keys compare as
    // integers, so that is an atomic minimum; interned keys (rare) are compared by bytes after.
    let id_tasks = n_ids.div_ceil(RANGE).max(1);
    let root: Vec<AtomicU32> = (0..n_ids).map(|_| AtomicU32::new(0)).collect();
    let name: Vec<AtomicU64> = (0..n_ids).map(|_| AtomicU64::new(u64::MAX)).collect();
    let slow = parallel("links-roots", threads, id_tasks, Vec::new, |slow: &mut Vec<(u32, Key)>, t| {
        let (s, e) = (t * RANGE, ((t + 1) * RANGE).min(n_ids));
        for i in s..e {
            let r = uf_find(&parent, i as u32);
            root[i].store(r, Ordering::Relaxed);
            let k = keys[i];
            if key::is_slow(k) {
                slow.push((r, k));
            } else {
                name[r as usize].fetch_min(k, Ordering::Relaxed);
            }
        }
    });
    for list in slow {
        for (r, k) in list {
            let cur = name[r as usize].load(Ordering::Relaxed);
            if cur == u64::MAX || key::cmp(k, cur).is_lt() {
                name[r as usize].store(k, Ordering::Relaxed);
            }
        }
    }
    drop(parent);

    // Per partition (its IDs are one contiguous range): the non-name IDs with their names, then
    // the table, one task per partition.
    let lists = parallel("links-names", threads, LINK_PARTS, Vec::new, |out: &mut Vec<(usize, Vec<(Key, Key)>)>, p| {
        let (lo, hi) = (base[p] as usize, base[p + 1] as usize);
        let mut pairs = Vec::new();
        for id in lo..hi {
            let nm = name[root[id].load(Ordering::Relaxed) as usize].load(Ordering::Relaxed);
            if nm != keys[id] {
                pairs.push((keys[id], nm));
            }
        }
        out.push((p, pairs));
    });
    let mut per: Vec<Vec<(Key, Key)>> = (0..LINK_PARTS).map(|_| Vec::new()).collect();
    for list in lists {
        for (p, pairs) in list {
            per[p] = pairs;
        }
    }
    let persons = Persons::build(per, threads);
    eprintln!(
        "  links: {total} pairs, {n_ids} ids; ids {t_ids:.3}s, fill {:.3}s, union-find (parallel) {:.3}s, names + table {:.3}s",
        t_fill - t_ids,
        t_union - t_fill,
        t0.elapsed().as_secs_f64() - t_union
    );
    persons
}

/// Root of `x`, halving the path on the way (a lost compare-and-swap is harmless: another
/// thread moved the pointer up already).
#[inline(always)]
fn uf_find(parent: &[AtomicU32], mut x: u32) -> u32 {
    loop {
        let p = parent[x as usize].load(Ordering::Acquire);
        if p == x {
            return x;
        }
        let gp = parent[p as usize].load(Ordering::Acquire);
        if gp != p {
            let _ = parent[x as usize].compare_exchange_weak(p, gp, Ordering::AcqRel, Ordering::Relaxed);
        }
        x = gp;
    }
}

/// Lock-free union: the root with the larger index is linked under the smaller one with a
/// compare-and-swap, so concurrent unions can never form a cycle; a lost race retries.
#[inline(always)]
fn uf_union(parent: &[AtomicU32], a: u32, b: u32) {
    let (mut a, mut b) = (a, b);
    loop {
        a = uf_find(parent, a);
        b = uf_find(parent, b);
        if a == b {
            return;
        }
        let (lo, hi) = if a < b { (a, b) } else { (b, a) };
        if parent[hi as usize].compare_exchange(hi, lo, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
            return;
        }
    }
}

/// A raw pointer that worker threads may share, for writes to provably disjoint indices.
#[derive(Clone, Copy)]
struct SyncPtr<T>(*mut T);
// SAFETY: used only for writes to disjoint indices (see the call sites).
unsafe impl<T> Send for SyncPtr<T> {}
unsafe impl<T> Sync for SyncPtr<T> {}

impl<T> SyncPtr<T> {
    #[inline(always)]
    fn get(&self) -> *mut T {
        self.0
    }
}

fn load_context(data: &Path, start: Instant) -> Ctx {
    let manifest = String::from_utf8_lossy(&read_small(&data.join("manifest.json"))).into_owned();
    let ws = manifest_value(&manifest, "window_start");
    let we = manifest_value(&manifest, "window_end");
    assert!(we > ws && we - ws + SEVEN_DAYS < u32::MAX as i64, "window too long for u32 offsets");

    let products_raw = read_small(&data.join("products.csv"));
    let product_rows = two_columns(&products_raw);
    // Distinct categories through a set (few of them), not by sorting a copy of every row.
    let distinct: std::collections::HashSet<&[u8]> = product_rows.iter().map(|r| r.1).chain([&b"unknown"[..]]).collect();
    let mut categories: Vec<Vec<u8>> = distinct.into_iter().map(|c| c.to_vec()).collect();
    categories.sort();
    assert!(categories.len() <= 256, "more than 256 categories");
    let cat_index: HashMap<&[u8], u32> = categories.iter().enumerate().map(|(i, c)| (c.as_slice(), i as u32)).collect();
    let unknown = cat_index[&b"unknown"[..]];
    let product_values: Vec<(&[u8], u32)> = product_rows.iter().map(|(id, cat)| (*id, cat_index[cat])).collect();
    let products = IdLookup::build(&product_values);

    let campaigns_raw = read_small(&data.join("campaigns.csv"));
    let campaign_rows = two_columns(&campaigns_raw);
    let campaign_ids: Vec<Vec<u8>> = campaign_rows.iter().map(|r| r.0.to_vec()).collect();
    let channels: Vec<Vec<u8>> = campaign_rows.iter().map(|r| r.1.to_vec()).collect();
    let campaign_values: Vec<(&[u8], u32)> = campaign_rows.iter().enumerate().map(|(i, r)| (r.0, i as u32)).collect();
    let campaigns = IdLookup::build(&campaign_values);
    log(start, &format!("lookups: {} products, {} categories, {} campaigns", product_rows.len(), categories.len(), campaign_ids.len()));

    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1) as usize;
    Ctx {
        ws,
        we,
        ingest_base_s: ws.div_euclid(1000) - (1i64 << 31),
        click_base: ws - SEVEN_DAYS,
        products,
        unknown,
        categories,
        campaigns,
        campaign_ids,
        channels,
        day0,
        ndays,
    }
}

#[inline]
fn parse_price(b: &[u8]) -> Option<u32> {
    if b.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v * 10 + d as u64;
        if v > 10_000_000 {
            return None;
        }
    }
    (v >= 1).then_some(v as u32)
}

#[inline(always)]
fn parse_event(ctx: &Ctx, r: &csv::Rec<'_, 7>) -> Option<EvRec> {
    const VIEW_W: u32 = u32::from_le_bytes(*b"view");
    const CART_W: u32 = u32::from_le_bytes(*b"cart");
    const PURCHASE_W: u64 = u64::from_le_bytes(*b"purchase");
    let eid = r.get(0);
    if eid.is_empty() {
        return None;
    }
    let ingest_both = ts::parse_trusted_f1(r.wide(3), r.get(3));
    let ingest = match ingest_both {
        Some((s, _)) => (s - ctx.ingest_base_s).clamp(0, u32::MAX as i64 - 1) as u32,
        None => u32::MAX,
    };
    let ingest_ms = ingest_both.map(|(_, ms)| ms);
    let mut rec = EvRec { eid: key::pack_wide_anon(r.wide(0), eid), user: 0, ingest, ts: INVALID, pc: 0 };
    // event_type, exactly: one integer compare on the field's window instead of byte slices.
    let ty = u64::from_le_bytes(r.wide(4)[..8].try_into().unwrap());
    let purchase = match r.len(4) {
        4 if ty as u32 == VIEW_W || ty as u32 == CART_W => false,
        8 if ty == PURCHASE_W => true,
        _ => return Some(rec),
    };
    if r.len(1) == 0 || r.len(5) == 0 {
        return Some(rec);
    }
    let Some(t) = ts::parse_wide(r.wide(2), r.get(2)) else { return Some(rec) };
    let Some(ingest_ms) = ingest_ms else { return Some(rec) };
    if t < ctx.ws || t >= ctx.we || ingest_ms - t > DAY {
        return Some(rec);
    }
    if purchase {
        let Some(price) = parse_price(r.get(6)) else { return Some(rec) };
        let cat = ctx.products.get_wide(r.wide(5), r.get(5)).unwrap_or(ctx.unknown);
        rec.pc = price << 8 | cat;
    }
    rec.user = key::pack_wide(r.wide(1), r.get(1));
    rec.ts = (t - ctx.ws) as u32;
    Some(rec)
}

#[inline]
fn parse_click(ctx: &Ctx, r: &csv::Rec<'_, 4>) -> Option<ClickRec> {
    if r.len(0) == 0 || r.len(1) == 0 {
        return None;
    }
    let camp = ctx.campaigns.get_wide(r.wide(2), r.get(2))?;
    let t = ts::parse_wide(r.wide(3), r.get(3))?;
    if t < ctx.click_base || t >= ctx.we {
        return None;
    }
    Some(ClickRec {
        person: key::pack_wide(r.wide(1), r.get(1)),
        cid: key::pack_wide(r.wide(0), r.get(0)),
        ts: (t - ctx.click_base) as u32,
        camp,
        _pad: 0,
    })
}

struct UserRow {
    person: Key,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top: u16,
}

struct Agg {
    rows: Vec<UserRow>,
    camp: Vec<[i64; 4]>, // (campaign or organic) x day: sessions, purchasing, revenue, assisted
    // The click window [start - 7 days, start] as counts per campaign plus the list of campaigns
    // with a non-zero count, so a session's assists cost its distinct campaigns, not its clicks.
    count: Vec<u32>,
    active: Vec<u32>,
    pos: Vec<u32>,
    catsum: Vec<i64>,
    day_purchase: bool, // catsum holds something for the current person-day
    lows: Vec<u64>,     // scratch: one person's events as ts << 32 | price << 8 | category
}

impl Agg {
    #[inline(always)]
    fn enter(&mut self, camp: u32) {
        let c = camp as usize;
        if self.count[c] == 0 {
            self.pos[c] = self.active.len() as u32;
            self.active.push(camp);
        }
        self.count[c] += 1;
    }

    #[inline(always)]
    fn leave(&mut self, camp: u32) {
        let c = camp as usize;
        self.count[c] -= 1;
        if self.count[c] == 0 {
            let at = self.pos[c] as usize;
            let last = *self.active.last().unwrap();
            self.active[at] = last;
            self.pos[last as usize] = at as u32;
            self.active.pop();
        }
    }

    fn clear_window(&mut self) {
        for &c in &self.active {
            self.count[c as usize] = 0;
        }
        self.active.clear();
    }
}

fn flush_day(agg: &mut Agg, person: Key, day: i64, sessions: i64, events: i64, revenue: i64) {
    let mut top = NO_CATEGORY;
    // Most person-days have no purchase: then there is no category to pick and nothing to reset.
    if agg.day_purchase {
        agg.day_purchase = false;
        let mut best = 0;
        for (i, s) in agg.catsum.iter_mut().enumerate() {
            if *s > best {
                best = *s;
                top = i as u16;
            }
            *s = 0;
        }
    }
    agg.rows.push(UserRow { person, day: day as i32, sessions, events, revenue, top });
}

/// The time (ms since window_start) of an event stored as `ts << 32 | price << 8 | category`.
#[inline(always)]
fn ev_ts(x: u64) -> u32 {
    (x >> 32) as u32
}

/// One person's events (sorted `ts << 32 | price << 8 | category`) and clicks (sorted by ts).
fn process_person(ctx: &Ctx, agg: &mut Agg, person: Key, evs: &[u64], clicks: &[ClickRec]) {
    let organic = ctx.campaign_ids.len();
    let ndays = ctx.ndays;
    let (mut l, mut r) = (0usize, 0usize);
    let mut cur_day = i64::MIN;
    let (mut d_sessions, mut d_events, mut d_revenue) = (0i64, 0i64, 0i64);
    let mut s = 0;
    while s < evs.len() {
        let mut e = s + 1;
        while e < evs.len() && (ev_ts(evs[e]) - ev_ts(evs[e - 1])) as i64 <= GAP {
            e += 1;
        }
        let start = ctx.ws + ev_ts(evs[s]) as i64;
        let day = start.div_euclid(DAY);
        if day != cur_day {
            if cur_day != i64::MIN {
                flush_day(agg, person, cur_day, d_sessions, d_events, d_revenue);
            }
            cur_day = day;
            d_sessions = 0;
            d_events = 0;
            d_revenue = 0;
        }
        let mut revenue = 0i64;
        let mut purchased = false;
        for &x in &evs[s..e] {
            let pc = x as u32;
            if pc != 0 {
                let price = (pc >> 8) as i64;
                revenue += price;
                purchased = true;
                agg.day_purchase = true;
                agg.catsum[(pc & 0xFF) as usize] += price;
            }
        }
        d_sessions += 1;
        d_events += (e - s) as i64;
        d_revenue += revenue;

        // Clicks in [start - 7 days, start], in click time (ms since window_start - 7 days).
        let hi = start - ctx.click_base;
        let lo = hi - SEVEN_DAYS;
        while r < clicks.len() && clicks[r].ts as i64 <= hi {
            agg.enter(clicks[r].camp);
            r += 1;
        }
        while l < r && (clicks[l].ts as i64) < lo {
            agg.leave(clicks[l].camp);
            l += 1;
        }
        let di = (day - ctx.day0) as usize;
        let credited = if l < r {
            // The latest click; on a tie in ts, the byte-wise smallest click_id.
            let t = clicks[r - 1].ts;
            let mut best = r - 1;
            let mut k = r - 1;
            while k > l && clicks[k - 1].ts == t {
                k -= 1;
                if key::cmp(clicks[k].cid, clicks[best].cid).is_lt() {
                    best = k;
                }
            }
            clicks[best].camp as usize
        } else {
            organic
        };
        let cell = &mut agg.camp[credited * ndays + di];
        cell[0] += 1;
        cell[1] += purchased as i64;
        cell[2] += revenue;
        // Every campaign with a click in the window assists once.
        for &c in &agg.active {
            agg.camp[c as usize * ndays + di][3] += 1;
        }
        s = e;
    }
    agg.clear_window();
    if cur_day != i64::MIN {
        flush_day(agg, person, cur_day, d_sessions, d_events, d_revenue);
    }
}

fn take_all<T>(slot: &Mutex<Vec<Vec<T>>>) -> Vec<T> {
    let parts = std::mem::take(&mut *slot.lock().unwrap());
    let n = parts.iter().map(|p| p.len()).sum();
    let mut all = Vec::with_capacity(n);
    for p in parts {
        all.extend(p);
    }
    all
}

/// A bucket of records: a Vec or a write-combined line bucket.
trait Bucket {
    fn count(&self) -> usize;
}

impl<T> Bucket for Vec<T> {
    fn count(&self) -> usize {
        self.len()
    }
}

impl<T: Copy, const K: usize> Bucket for scatter::Lines<T, K> {
    fn count(&self) -> usize {
        self.len()
    }
}

/// Moves per-thread bucket lists into per-bucket slots: slots[b] holds every thread's bucket b.
fn regroup<B: Bucket>(per_thread: Vec<Vec<B>>, n: usize) -> Vec<Mutex<Vec<B>>> {
    let mut slots: Vec<Vec<B>> = (0..n).map(|_| Vec::with_capacity(per_thread.len())).collect();
    for buckets in per_thread {
        for (b, v) in buckets.into_iter().enumerate() {
            if v.count() > 0 {
                slots[b].push(v);
            }
        }
    }
    slots.into_iter().map(Mutex::new).collect()
}

/// Counting-sort scatter of several pieces into `nb` bins: returns the binned records and the
/// start of each bin (nb + 1 offsets). Reads each piece in place, so no concatenation copy.
fn bin_scatter<T: Copy>(pieces: &[&[T]], nb: usize, bin: impl Fn(&T) -> usize) -> (Vec<T>, Vec<usize>) {
    let mut at = vec![0usize; nb + 1];
    for piece in pieces {
        for x in piece.iter() {
            at[bin(x) + 1] += 1;
        }
    }
    for b in 0..nb {
        at[b + 1] += at[b];
    }
    // Every slot is written exactly once (the counts are the writes), so no zero-fill first.
    let n = at[nb];
    let mut out: Vec<T> = Vec::with_capacity(n);
    let spare = out.spare_capacity_mut();
    let mut next = at.clone();
    for piece in pieces {
        for x in piece.iter() {
            let b = bin(x);
            spare[next[b]].write(*x);
            next[b] += 1;
        }
    }
    // SAFETY: the counting pass sized each bin exactly, so all n slots were written above.
    unsafe { out.set_len(n) };
    (out, at)
}

fn slot_len<B: Bucket>(slot: &Mutex<Vec<B>>) -> usize {
    slot.lock().unwrap().iter().map(|p| p.count()).sum()
}

/// Single-threaded micro-benchmarks of the event parse path (SS_BENCH=lines|split|event|push).
fn bench(ctx: &Ctx, data: &Path, mode: &str) {
    let input = csv::Input::open(&csv::list_csv(&data.join("events")));
    let chunks = input.chunks(chunk_size());
    let t = Instant::now();
    let mut n = 0u64;
    let mut buf = Vec::new();
    let np = 2048;
    let mut buckets: scatter::Scatter<DedupRec, 4> = scatter::Scatter::new(np);
    let mut payload: Vec<Pay> = Vec::with_capacity(input.bytes() / 56 + 64);
    for &c in &chunks {
        if mode == "lines" {
            input.lines(c, &mut buf, |line| n += line.len() as u64);
            continue;
        }
        input.records::<7>(c, &mut buf, |f| match mode {
            "split" => n += 1,
            "event" => n += parse_event(ctx, f).is_some() as u64,
            _ => {
                if let Some(rec) = parse_event(ctx, f) {
                    push_event(&rec, 0, &mut payload, &mut buckets, np);
                    n += 1;
                }
            }
        });
    }
    let secs = t.elapsed().as_secs_f64();
    eprintln!("bench {mode}: {n} in {secs:.3}s = {:.0} MB/s", input.bytes() as f64 / secs / 1e6);
}

/// What dedup needs of an event copy: its ID, its ingest time, and where its payload is
/// (`NOREF` for a copy that fails validation: it competes in dedup but has nothing to keep).
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct DedupRec {
    eid: Key,
    ingest: u32,
    pref: u32, // thread << REF_BITS | index into that thread's payload
}

/// What a valid event copy contributes if it is its event's kept copy.
#[derive(Clone, Copy)]
#[repr(C)]
struct Pay {
    user: Key,
    ts: u32,
    pc: u32,
}

const NOREF: u32 = u32::MAX;
const REF_BITS: u32 = 26; // up to 64 threads and 67M valid events per thread
const MAX_THREADS: usize = 1 << (32 - REF_BITS);

/// Stores a parsed copy: the payload goes to this thread's sequential array, and a 16-byte
/// record goes to the event-id bucket.
#[inline(always)]
fn push_event(rec: &EvRec, thread: usize, payload: &mut Vec<Pay>, buckets: &mut scatter::Scatter<DedupRec, 4>, np: usize) {
    let pref = if rec.ts == INVALID {
        NOREF
    } else {
        let at = payload.len();
        assert!(at < (1 << REF_BITS) - 1, "too many events on one thread for a 26-bit reference");
        payload.push(Pay { user: rec.user, ts: rec.ts, pc: rec.pc });
        (thread as u32) << REF_BITS | at as u32
    };
    buckets.push(bucket(rec.eid, np), DedupRec { eid: rec.eid, ingest: rec.ingest, pref });
}

struct ParseState {
    thread: usize,
    buf: Vec<u8>,
    events: scatter::Scatter<DedupRec, 4>,
    payload: Vec<Pay>,
    clicks: Vec<ClickRec>,
}

struct DedupState {
    table: Vec<DedupRec>,
    used: Vec<u32>,
}

struct RegroupState {
    sess: scatter::Scatter<u128, 4>,
    clicks: scatter::Scatter<ClickRec, 2>,
}

fn main() {
    let start = Instant::now();
    let args: Vec<String> = std::env::args().collect();
    let data = PathBuf::from(std::env::var("DATA_DIR").ok().or_else(|| args.get(1).cloned()).unwrap_or("/data".into()));
    let output = PathBuf::from(std::env::var("OUTPUT_DIR").ok().or_else(|| args.get(2).cloned()).unwrap_or("/output".into()));
    let threads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(8, usize::from))
        .clamp(1, MAX_THREADS);

    let ctx = load_context(&data, start);
    if let Ok(mode) = std::env::var("SS_BENCH") {
        bench(&ctx, &data, &mode);
        return;
    }
    let events_in = csv::Input::open(&csv::list_csv(&data.join("events")));
    let clicks_in = csv::Input::open(&csv::list_csv(&data.join("clicks")));
    let ev_chunks = events_in.chunks(chunk_size());
    let ck_chunks = clicks_in.chunks(chunk_size());

    // Event-id buckets small enough that a bucket's dedup table stays in L2.
    let np: usize = std::env::var("SS_NP")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or((events_in.bytes() / 76 / 8192).next_power_of_two().min(2048))
        .clamp(64, 4096);
    // Capacities with headroom: a doubling copy of a big array costs far more than slack.
    let pay_estimate = events_in.bytes() / 56 / threads + 64;
    let ck_estimate = clicks_in.bytes() / 30 / threads + 64;
    let n_tasks = ev_chunks.len() + ck_chunks.len();

    // ---- parse events and clicks on the pool; links (which parsing does not need) alongside
    let thread_ids = AtomicUsize::new(0);
    let (parsed, persons) = std::thread::scope(|scope| {
        let links = scope.spawn(|| {
            let persons = build_links(&data, threads.min(2));
            log(start, &format!("links: {} non-name IDs mapped", persons.len()));
            persons
        });
        let parsed = parallel(
            "parse",
            threads,
            n_tasks,
            || ParseState {
                thread: thread_ids.fetch_add(1, Ordering::Relaxed),
                buf: Vec::new(),
                events: scatter::Scatter::new(np),
                payload: Vec::with_capacity(pay_estimate),
                clicks: Vec::with_capacity(ck_estimate),
            },
            |st, task| {
                let ParseState { thread, buf, events, payload, clicks } = st;
                // A replica (SS_REPLICATE benchmarking) shifts its IDs in a loop of its own, so the
                // normal loop is unchanged.
                if task < ev_chunks.len() {
                    let chunk = ev_chunks[task];
                    if !cfg!(feature = "scale") || chunk.copy == 0 {
                        events_in.records::<7>(chunk, buf, |f| {
                            if let Some(rec) = parse_event(&ctx, f) {
                                push_event(&rec, *thread, payload, events, np);
                            }
                        });
                    } else {
                        events_in.records::<7>(chunk, buf, |f| {
                            if let Some(mut rec) = parse_event(&ctx, f) {
                                rec.eid = key::salt(rec.eid, chunk.copy);
                                rec.user = key::salt(rec.user, chunk.copy);
                                push_event(&rec, *thread, payload, events, np);
                            }
                        });
                    }
                } else {
                    let chunk = ck_chunks[task - ev_chunks.len()];
                    let shifted = cfg!(feature = "scale") && chunk.copy != 0;
                    clicks_in.records::<4>(chunk, buf, |f| {
                        if let Some(mut rec) = parse_click(&ctx, f) {
                            if shifted {
                                rec.person = key::salt(rec.person, chunk.copy);
                                rec.cid = key::salt(rec.cid, chunk.copy);
                            }
                            clicks.push(rec);
                        }
                    });
                }
            },
        );
        log(start, &format!("parsed {} MB events, {} MB clicks", events_in.bytes() >> 20, clicks_in.bytes() >> 20));
        (parsed, links.join().expect("links thread panicked"))
    });
    // The long user and click IDs the parse threads noted go to the dictionary before anything
    // reads names (sessions' click tie-breaks, the output).
    key::publish(threads);
    log(start, "long IDs published");
    log(start, "links joined");
    let mut ev_threads = Vec::with_capacity(parsed.len());
    let mut payloads: Vec<Vec<Pay>> = (0..threads).map(|_| Vec::new()).collect();
    let mut ck_lists = Vec::with_capacity(parsed.len());
    for st in parsed {
        ev_threads.push(st.events.finish());
        payloads[st.thread] = st.payload;
        ck_lists.push(st.clicks);
    }
    let ev_slots = regroup(ev_threads, np);
    // One bit per payload entry, set when that copy lost its event's dedup.
    let losers: Vec<Vec<AtomicU64>> =
        payloads.iter().map(|p| (0..p.len() / 64 + 1).map(|_| AtomicU64::new(0)).collect()).collect();

    // ---- dedup on event_id: per bucket, the copy with the smallest ingest_ts is kept and every
    // other copy that has a payload is marked a loser. Duplicates are a few percent of rows, so
    // this phase only reads 16-byte records and probes a small table.
    let deduped = parallel(
        "dedup",
        threads,
        np,
        || DedupState { table: Vec::new(), used: Vec::new() },
        |st, p| {
            let owned = std::mem::take(&mut *ev_slots[p].lock().unwrap());
            let n: usize = owned.iter().map(|l| l.len()).sum();
            let cap = (2 * n).next_power_of_two().max(256);
            if st.table.len() < cap {
                st.table.resize(cap, DedupRec { eid: key::EMPTY, ingest: 0, pref: NOREF });
            }
            let mask = cap - 1;
            for piece in &owned {
                for r in piece.slices().flatten() {
                    let mut slot = key::hash(r.eid) as usize & mask;
                    loop {
                        debug_assert!(slot < st.table.len());
                        // SAFETY: slot <= mask = cap - 1 < table.len() (resized above).
                        let s = unsafe { st.table.get_unchecked_mut(slot) };
                        if s.eid == key::EMPTY {
                            *s = *r;
                            st.used.push(slot as u32);
                            break;
                        }
                        if s.eid == r.eid {
                            // Copies with equal ingest_ts are identical, so either may be kept.
                            let loser = if r.ingest < s.ingest {
                                let old = s.pref;
                                s.ingest = r.ingest;
                                s.pref = r.pref;
                                old
                            } else {
                                r.pref
                            };
                            if loser != NOREF {
                                let at = (loser & ((1 << REF_BITS) - 1)) as usize;
                                losers[(loser >> REF_BITS) as usize][at / 64].fetch_or(1 << (at % 64), Ordering::Relaxed);
                            }
                            break;
                        }
                        slot = (slot + 1) & mask;
                    }
                }
            }
            for &slot in &st.used {
                st.table[slot as usize].eid = key::EMPTY;
            }
            st.used.clear();
            // Freed (purging is off, MIMALLOC_PURGE_DELAY=-1): the next phase's buffers reuse
            // these already-faulted pages instead of new ones the kernel must zero.
            drop(owned);
        },
    );
    drop(deduped);
    drop(ev_slots);
    log(start, "deduplicated events");

    // ---- map users to persons and bucket by person: kept events (sequential over each
    // thread's payload) and clicks, both cut into small ranges so no task is a straggler
    const RANGE: usize = 32 * 1024;
    let mut ranges: Vec<(bool, usize, usize, usize)> = Vec::new();
    for (li, list) in ck_lists.iter().enumerate() {
        for s in (0..list.len()).step_by(RANGE) {
            ranges.push((true, li, s, (s + RANGE).min(list.len())));
        }
    }
    for (t, pay) in payloads.iter().enumerate() {
        for s in (0..pay.len()).step_by(RANGE) {
            ranges.push((false, t, s, (s + RANGE).min(pay.len())));
        }
    }
    // Person buckets small enough that a bucket's events, clicks and their sorted copies stay
    // in L2 during the sessions phase.
    let nq: usize = std::env::var("SS_NQ")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or((events_in.bytes() / 76 / 16384).next_power_of_two().min(1024))
        .clamp(64, 4096);
    let regrouped = parallel(
        "regroup",
        threads,
        ranges.len(),
        || RegroupState {
            sess: scatter::Scatter::new(nq),
            clicks: scatter::Scatter::new(nq),
        },
        |st, task| {
            let (is_click, li, s, e) = ranges[task];
            if is_click {
                let list = &ck_lists[li][s..e];
                let last = list.len().saturating_sub(1);
                for i in 0..list.len() {
                    // Look-ahead clamped to the last record (prefetching it again is harmless), so
                    // the loop has no bounds branches.
                    // SAFETY: every index below is at most last < list.len().
                    unsafe {
                        persons.prefetch_filter(list.get_unchecked((i + 2 * PREFETCH).min(last)).person);
                        persons.prefetch_slot(list.get_unchecked((i + PREFETCH).min(last)).person);
                    }
                    let mut c = list[i];
                    let person = persons.get(c.person);
                    c.person = person;
                    st.clicks.push(bucket(person, nq), c);
                }
                return;
            }
            let pay = &payloads[li][s..e];
            let lost = &losers[li];
            let last = pay.len().saturating_sub(1);
            for i in 0..pay.len() {
                // SAFETY: every index below is at most last < pay.len().
                unsafe {
                    persons.prefetch_filter(pay.get_unchecked((i + 2 * PREFETCH).min(last)).user);
                    persons.prefetch_slot(pay.get_unchecked((i + PREFETCH).min(last)).user);
                }
                let g = s + i;
                if lost[g / 64].load(Ordering::Relaxed) >> (g % 64) & 1 != 0 {
                    continue;
                }
                let p = pay[i];
                let person = persons.get(p.user);
                st.sess.push(bucket(person, nq), (person as u128) << 64 | (p.ts as u128) << 32 | p.pc as u128);
            }
        },
    );
    // Freed, not leaked: with purging off, later phases reuse these already-faulted pages.
    drop(payloads);
    drop(ck_lists);
    let mut sess_threads = Vec::with_capacity(regrouped.len());
    let mut ck_threads = Vec::with_capacity(regrouped.len());
    for st in regrouped {
        sess_threads.push(st.sess.finish());
        ck_threads.push(st.clicks.finish());
    }
    let sess_slots = regroup(sess_threads, nq);
    let ck_slots = regroup(ck_threads, nq);
    log(start, "regrouped by person");

    // ---- sessions, attribution, aggregation per person bucket, largest first
    let mut order: Vec<usize> = (0..nq).collect();
    let sizes: Vec<usize> = (0..nq).map(|q| slot_len(&sess_slots[q]) + slot_len(&ck_slots[q])).collect();
    order.sort_by_key(|&q| std::cmp::Reverse(sizes[q]));
    let ncamp = ctx.campaign_ids.len();
    let aggs = parallel(
        "sessions",
        threads,
        nq,
        || Agg {
            rows: Vec::new(),
            camp: vec![[0; 4]; (ncamp + 1) * ctx.ndays],
            count: vec![0; ncamp + 1],
            active: Vec::new(),
            pos: vec![0; ncamp + 1],
            catsum: vec![0; ctx.categories.len()],
            day_purchase: false,
            lows: Vec::new(),
        },
        |agg, task| {
            let q = order[task];
            let ev_pieces = std::mem::take(&mut *sess_slots[q].lock().unwrap());
            let ck_pieces = std::mem::take(&mut *ck_slots[q].lock().unwrap());
            let n_ev: usize = ev_pieces.iter().map(|p| p.len()).sum();
            // Bin by bits of hash(person) that `bucket` does not use: a person's events and
            // clicks share a bin, and a bin holds only a few persons, so sorting it is cheap.
            let nb = (n_ev / 8).next_power_of_two().clamp(16, 1 << 16);
            let bin = |p: Key| ((key::hash(p) >> 16) as usize) & (nb - 1);
            let ev_slices: Vec<&[u128]> = ev_pieces.iter().flat_map(|p| p.slices()).collect();
            let ck_slices: Vec<&[ClickRec]> = ck_pieces.iter().flat_map(|p| p.slices()).collect();
            let (mut evs, ev_at) = bin_scatter(&ev_slices, nb, |x: &u128| bin((x >> 64) as u64));
            let (mut cks, ck_at) = bin_scatter(&ck_slices, nb, |c: &ClickRec| bin(c.person));
            drop(ev_slices);
            drop(ck_slices);
            drop(ev_pieces);
            drop(ck_pieces);
            for b in 0..nb {
                let evs = &mut evs[ev_at[b]..ev_at[b + 1]];
                if evs.is_empty() {
                    continue;
                }
                let cks = &mut cks[ck_at[b]..ck_at[b + 1]];
                // By (person, ts) only: process_person resolves equal-ts clicks to the smallest
                // click_id itself, so their order does not matter. One u128 compare per step.
                cks.sort_unstable_by_key(|c| (c.person as u128) << 32 | c.ts as u128);
                let mut lows = std::mem::take(&mut agg.lows);
                let first = (evs[0] >> 64) as u64;
                if evs.iter().all(|&x| (x >> 64) as u64 == first) {
                    // The usual bin: one person. Their events sort as 64-bit (ts, payload)
                    // values, which is much cheaper than sorting 128-bit (person, ts, payload).
                    lows.clear();
                    lows.extend(evs.iter().map(|&x| x as u64));
                    lows.sort_unstable();
                    let c0 = cks.partition_point(|c| c.person < first);
                    let c1 = c0 + cks[c0..].partition_point(|c| c.person == first);
                    process_person(&ctx, agg, first, &lows, &cks[c0..c1]);
                } else {
                    evs.sort_unstable();
                    let mut ci = 0;
                    let mut i = 0;
                    while i < evs.len() {
                        let p = (evs[i] >> 64) as u64;
                        let mut j = i + 1;
                        while j < evs.len() && (evs[j] >> 64) as u64 == p {
                            j += 1;
                        }
                        while ci < cks.len() && cks[ci].person < p {
                            ci += 1;
                        }
                        let c0 = ci;
                        while ci < cks.len() && cks[ci].person == p {
                            ci += 1;
                        }
                        lows.clear();
                        lows.extend(evs[i..j].iter().map(|&x| x as u64));
                        process_person(&ctx, agg, p, &lows, &cks[c0..ci]);
                        i = j;
                    }
                }
                agg.lows = lows;
            }
        },
    );
    log(start, "sessionized and attributed");

    // ---- output
    let ud_dir = output.join("user_daily");
    let cd_dir = output.join("campaign_daily");
    std::fs::create_dir_all(&ud_dir).expect("mkdir user_daily");
    std::fs::create_dir_all(&cd_dir).expect("mkdir campaign_daily");

    let mut camp_total = vec![[0i64; 4]; (ncamp + 1) * ctx.ndays];
    let mut row_lists = Vec::new();
    for agg in aggs {
        for (t, a) in camp_total.iter_mut().zip(agg.camp.iter()) {
            for k in 0..4 {
                t[k] += a[k];
            }
        }
        row_lists.push(agg.rows);
    }
    // Small, even pieces (several per thread), so no single file sets the phase's length;
    // campaign_daily is written as one more task alongside them.
    let total_rows: usize = row_lists.iter().map(|r| r.len()).sum();
    let piece_rows = (total_rows / (4 * threads)).max(4096);
    let mut pieces: Vec<(usize, usize, usize)> = Vec::new();
    for (li, list) in row_lists.iter().enumerate() {
        for s in (0..list.len()).step_by(piece_rows) {
            pieces.push((li, s, (s + piece_rows).min(list.len())));
        }
    }
    if pieces.is_empty() {
        pieces.push((0, 0, 0)); // user_daily still needs one (empty) file
    }
    let campaign_rows = AtomicUsize::new(0);
    parallel("write", threads, pieces.len() + 1, || (), |_, task| {
        if task == pieces.len() {
            campaign_rows.store(write_campaigns(&ctx, &camp_total, &cd_dir), Ordering::Relaxed);
            return;
        }
        let (li, s, e) = pieces[task];
        let rows = &row_lists.get(li).map_or(&[][..], |l| &l[s..e]);
        let n = rows.len();
        let mut user_id = out::Col::with_capacity(n * 16);
        let mut day = out::Col::with_capacity(n * 4);
        let mut sessions = out::Col::with_capacity(n * 4);
        let mut events = out::Col::with_capacity(n * 4);
        let mut revenue = out::Col::with_capacity(n * 8);
        let mut top = out::Col::with_capacity(n * 4);
        for (i, r) in rows.iter().enumerate() {
            user_id.push_str_with(|buf| key::append_bytes(r.person, buf));
            day.push_i32(r.day);
            sessions.push_count(r.sessions);
            events.push_count(r.events);
            revenue.push_i64(r.revenue);
            let present = r.top != NO_CATEGORY;
            top.set_present(i, present);
            if present {
                top.push_str(&ctx.categories[r.top as usize]);
            }
        }
        out::write(
            &ud_dir.join(format!("part-{task:05}.parquet")),
            "user_daily",
            &out::USER,
            &[user_id, day, sessions, events, revenue, top],
            n,
        );
    });
    log(
        start,
        &format!("wrote {total_rows} user_daily rows, {} campaign_daily rows", campaign_rows.load(Ordering::Relaxed)),
    );
    // Skip freeing gigabytes of buckets one by one: the process is done.
    std::process::exit(0);
}

/// Writes campaign_daily from the summed (campaign or organic) x day cells; returns its rows.
fn write_campaigns(ctx: &Ctx, camp_total: &[[i64; 4]], cd_dir: &Path) -> usize {
    let ncamp = ctx.campaign_ids.len();
    let mut campaign_id = out::Col::default();
    let mut channel = out::Col::default();
    let mut day = out::Col::default();
    let mut cols: [out::Col; 4] = Default::default();
    let mut n = 0;
    for c in 0..=ncamp {
        for d in 0..ctx.ndays {
            let cell = camp_total[c * ctx.ndays + d];
            if cell[0] == 0 && cell[3] == 0 {
                continue;
            }
            if c == ncamp {
                campaign_id.push_str(b"organic");
                channel.push_str(b"organic");
            } else {
                campaign_id.push_str(&ctx.campaign_ids[c]);
                channel.push_str(&ctx.channels[c]);
            }
            day.push_i32((ctx.day0 + d as i64) as i32);
            // sessions, purchasing_sessions, revenue_cents, assisted_sessions
            for k in 0..4 {
                if k == 2 {
                    cols[k].push_i64(cell[k]);
                } else {
                    cols[k].push_count(cell[k]);
                }
            }
            n += 1;
        }
    }
    let [s, p, r, a] = cols;
    out::write(&cd_dir.join("part-00000.parquet"), "campaign_daily", &out::CAMPAIGN, &[campaign_id, channel, day, s, p, r, a], n);
    n
}
