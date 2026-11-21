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
use parquet::data_type::ByteArray;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
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
        let mut lookup = IdLookup { prefix, dense: Vec::new(), other: HashMap::new() };
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
fn parallel<S: Send>(
    name: &str,
    threads: usize,
    n: usize,
    init: impl Fn() -> S + Sync,
    work: impl Fn(&mut S, usize) + Sync,
) -> Vec<S> {
    let next = AtomicUsize::new(0);
    let t0 = Instant::now();
    let results: Vec<(S, f64, f64, usize, f64)> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                scope.spawn(|| {
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
                    (state, busy, longest, longest_task, started)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().expect("worker panicked")).collect()
    });
    let wall = t0.elapsed().as_secs_f64();
    let busy: f64 = results.iter().map(|r| r.1).sum();
    let (longest, longest_task) = results.iter().map(|r| (r.2, r.3)).fold((0f64, 0), |a, b| if b.0 > a.0 { b } else { a });
    let slowest_start = results.iter().map(|r| r.4).fold(0f64, f64::max);
    eprintln!(
        "  phase {name}: {n} tasks, wall {wall:.3}s, utilization {:.0}%, longest task #{longest_task} {longest:.3}s, slowest thread start {slowest_start:.3}s",
        100.0 * busy / (threads as f64 * wall).max(1e-9)
    );
    results.into_iter().map(|r| r.0).collect()
}

#[inline(always)]
fn bucket(k: Key, n: usize) -> usize {
    ((key::hash(k) >> 40) as usize) % n
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
    slots: Vec<[u64; 2]>,
    mask: usize,
    filter: Vec<u64>,
    fmask: usize,
    len: usize,
}

impl Persons {
    fn build(pairs: &[Vec<(Key, Key)>]) -> Persons {
        let len: usize = pairs.iter().map(|p| p.len()).sum();
        let cap = (2 * len).next_power_of_two().max(16);
        let bits = (16 * len).next_power_of_two().max(1024);
        let mut persons = Persons {
            slots: vec![[key::EMPTY, 0]; cap],
            mask: cap - 1,
            filter: vec![0; bits / 64],
            fmask: bits - 1,
            len,
        };
        for list in pairs {
            for (i, &(k, name)) in list.iter().enumerate() {
                if let Some(&(next, _)) = list.get(i + PREFETCH) {
                    persons.prefetch_slot_always(next);
                }
                let h = key::hash(k);
                let fb = (h >> 20) as usize & persons.fmask;
                persons.filter[fb / 64] |= 1 << (fb % 64);
                let mut at = h as usize & persons.mask;
                while persons.slots[at][0] != key::EMPTY {
                    at = (at + 1) & persons.mask;
                }
                persons.slots[at] = [k, name];
            }
        }
        persons
    }

    #[inline(always)]
    fn get(&self, k: Key) -> Key {
        let h = key::hash(k);
        let fb = (h >> 20) as usize & self.fmask;
        if self.filter[fb / 64] >> (fb % 64) & 1 == 0 {
            return k;
        }
        let mut at = h as usize & self.mask;
        loop {
            let s = self.slots[at];
            if s[0] == k {
                return s[1];
            }
            if s[0] == key::EMPTY {
                return k;
            }
            at = (at + 1) & self.mask;
        }
    }

    /// First stage of a lookup pipeline: the filter word of `k`.
    #[inline(always)]
    fn prefetch_filter(&self, k: Key) {
        let fb = (key::hash(k) >> 20) as usize & self.fmask;
        prefetch(&self.filter[fb / 64]);
    }

    /// Second stage: the table slot of `k`, if the filter says it may be linked.
    #[inline(always)]
    fn prefetch_slot(&self, k: Key) {
        let h = key::hash(k);
        let fb = (h >> 20) as usize & self.fmask;
        if self.filter[fb / 64] >> (fb % 64) & 1 != 0 {
            prefetch(&self.slots[h as usize & self.mask]);
        }
    }

    #[inline(always)]
    fn prefetch_slot_always(&self, k: Key) {
        prefetch(&self.slots[key::hash(k) as usize & self.mask]);
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
            input.records::<2>(chunks[i], buf, |r| {
                let [a, b] = r.fields();
                if !a.is_empty() && !b.is_empty() {
                    pairs.push((pack(a), pack(b)));
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

    // The only serial step: union by size with path halving; parent and size share a cache
    // line, and each root keeps its component's byte-wise smallest key as it goes.
    let mut node: Vec<[u32; 2]> = (0..n_ids as u32).map(|i| [i, 1]).collect();
    let mut name: Vec<Key> = keys.clone();
    #[inline(always)]
    fn find(node: &mut [[u32; 2]], mut x: u32) -> u32 {
        while node[x as usize][0] != x {
            let up = node[node[x as usize][0] as usize][0];
            node[x as usize][0] = up;
            x = up;
        }
        x
    }
    for pair in slot_id.chunks_exact(2) {
        let ra = find(&mut node, pair[0]);
        let rb = find(&mut node, pair[1]);
        if ra != rb {
            let (big, small) = if node[ra as usize][1] >= node[rb as usize][1] { (ra, rb) } else { (rb, ra) };
            node[small as usize][0] = big;
            node[big as usize][1] += node[small as usize][1];
            if key::cmp(name[small as usize], name[big as usize]).is_lt() {
                name[big as usize] = name[small as usize];
            }
        }
    }
    drop(slot_id);
    let t_union = t0.elapsed().as_secs_f64();

    // Roots by read-only walks (trees are shallow after union by size), fused with building
    // each partition's map from non-name IDs to names.
    let pair_lists = parallel("links-names", threads, LINK_PARTS, Vec::new, |out: &mut Vec<(Key, Key)>, p| {
        let (lo, hi) = (base[p] as usize, base[p + 1] as usize);
        for id in lo..hi {
            let mut r = id as u32;
            while node[r as usize][0] != r {
                r = node[r as usize][0];
            }
            let nm = name[r as usize];
            if nm != keys[id] {
                out.push((keys[id], nm));
            }
        }
    });
    let persons = Persons::build(&pair_lists);
    eprintln!(
        "  links: {total} pairs, {n_ids} ids; ids {t_ids:.3}s, fill {:.3}s, union-find (serial) {:.3}s, name maps {:.3}s",
        t_fill - t_ids,
        t_union - t_fill,
        t0.elapsed().as_secs_f64() - t_union
    );
    persons
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
    let ingest_ms = ts::parse_wide(r.wide(3), r.get(3));
    let ingest = match ingest_ms {
        Some(ms) => (ms.div_euclid(1000) - ctx.ingest_base_s).clamp(0, u32::MAX as i64 - 1) as u32,
        None => u32::MAX,
    };
    let mut rec = EvRec { eid: key::pack_wide(r.wide(0), eid), user: 0, ingest, ts: INVALID, pc: 0 };
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
        let cat = ctx.products.get(r.get(5)).unwrap_or(ctx.unknown);
        rec.pc = price << 8 | cat;
    }
    rec.user = key::pack_wide(r.wide(1), r.get(1));
    rec.ts = (t - ctx.ws) as u32;
    Some(rec)
}

#[inline]
fn parse_click(ctx: &Ctx, r: &csv::Rec<'_, 4>) -> Option<ClickRec> {
    let f = r.fields();
    if f[0].is_empty() || f[1].is_empty() {
        return None;
    }
    let camp = ctx.campaigns.get(f[2])?;
    let t = ts::parse_wide(r.wide(3), f[3])?;
    if t < ctx.click_base || t >= ctx.we {
        return None;
    }
    Some(ClickRec { person: key::pack_wide(r.wide(1), f[1]), cid: key::pack_wide(r.wide(0), f[0]), ts: (t - ctx.click_base) as u32, camp, _pad: 0 })
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

/// Counting-sort scatter of several pieces into `nb` bins, keeping the records `keep` accepts
/// and storing `map` of each: returns the binned values and the start of each bin (nb + 1
/// offsets). Reads each piece in place, so no concatenation copy.
fn bin_scatter<T, U>(
    pieces: &[&[T]],
    nb: usize,
    bin: impl Fn(&T) -> usize,
    keep: impl Fn(&T) -> bool,
    map: impl Fn(&T) -> U,
) -> (Vec<U>, Vec<usize>) {
    let mut at = vec![0usize; nb + 1];
    for piece in pieces {
        for x in piece.iter() {
            at[bin(x) + 1] += keep(x) as usize;
        }
    }
    for b in 0..nb {
        at[b + 1] += at[b];
    }
    // Every slot is written exactly once (the counts are the writes), so no zero-fill first.
    let n = at[nb];
    let mut out: Vec<U> = Vec::with_capacity(n);
    let spare = out.spare_capacity_mut();
    let mut next = at.clone();
    for piece in pieces {
        for x in piece.iter() {
            if keep(x) {
                let b = bin(x);
                spare[next[b]].write(map(x));
                next[b] += 1;
            }
        }
    }
    // SAFETY: the counting pass sized each bin exactly with the same `keep` and `bin`, so all n
    // slots were written above.
    unsafe { out.set_len(n) };
    (out, at)
}

fn slot_len<B: Bucket>(slot: &Mutex<Vec<B>>) -> usize {
    slot.lock().unwrap().iter().map(|p| p.count()).sum()
}

/// Single-threaded micro-benchmarks of the event parse path (SS_BENCH=lines|split|event).
fn bench(ctx: &Ctx, data: &Path, mode: &str) {
    let input = csv::Input::open(&csv::list_csv(&data.join("events")));
    let chunks = input.chunks(chunk_size());
    let t = Instant::now();
    let mut n = 0u64;
    let mut buf = Vec::new();
    for &c in &chunks {
        if mode == "lines" {
            input.lines(c, &mut buf, |line| n += line.len() as u64);
            continue;
        }
        input.records::<7>(c, &mut buf, |f| match mode {
            "split" => n += 1,
            _ => n += parse_event(ctx, f).is_some() as u64,
        });
    }
    let secs = t.elapsed().as_secs_f64();
    eprintln!("bench {mode}: {n} in {secs:.3}s = {:.0} MB/s", input.bytes() as f64 / secs / 1e6);
}

/// What dedup needs of an event copy: its ID, its ingest time, and which valid copy it is
/// (`NOREF` for a copy that fails validation: it competes in dedup but has nothing to keep).
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct DedupRec {
    eid: Key,
    ingest: u32,
    pref: u32, // thread << REF_BITS | that thread's count of valid copies before this one
}

/// A valid event copy, in its person's bucket: 16 bytes, four to a cache line. `tseq` is its
/// DedupRec's `pref`, so the sessions phase can drop the copies that lost dedup. A purchase has
/// PURCHASE set in `ts`; its price and category are then in the parsing thread's `pcs[seq]`.
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct PEv {
    person: Key,
    ts: u32,
    tseq: u32,
}

/// Flag in PEv::ts (window offsets are below 2^31 ms, about 24.8 days).
const PURCHASE: u32 = 1 << 31;

const NOREF: u32 = u32::MAX;
const REF_BITS: u32 = 26; // up to 64 threads and 67M valid events per thread
const MAX_THREADS: usize = 1 << (32 - REF_BITS);

/// A parsed valid event waiting for its person lookup.
#[derive(Clone, Copy, Default)]
struct EvPending {
    user: Key,
    ts: u32,
    pc: u32,
    tseq: u32,
}

trait HasUser {
    fn user(&self) -> Key;
}

impl HasUser for EvPending {
    #[inline(always)]
    fn user(&self) -> Key {
        self.user
    }
}

impl HasUser for ClickRec {
    #[inline(always)]
    fn user(&self) -> Key {
        self.person // a parsed click holds its user ID here until the lookup
    }
}

const RING: usize = 8;

/// A software pipeline for person lookups, run on the parsing threads. A record waits RING
/// records between the start of its lookup (prefetch of the membership-filter word, then of the
/// table slot) and its completion, so the lookup's memory latency overlaps the parsing of the
/// records after it instead of stalling a separate phase.
struct Ring<T: Copy + Default + HasUser> {
    items: [T; RING],
    n: usize,
}

impl<T: Copy + Default + HasUser> Ring<T> {
    fn new() -> Self {
        Ring { items: [T::default(); RING], n: 0 }
    }

    #[inline(always)]
    fn push(&mut self, persons: &Persons, x: T, mut done: impl FnMut(T, Key)) {
        persons.prefetch_filter(x.user());
        if self.n >= RING / 2 {
            persons.prefetch_slot(self.items[(self.n - RING / 2) % RING].user());
        }
        if self.n >= RING {
            let old = self.items[self.n % RING];
            done(old, persons.get(old.user()));
        }
        self.items[self.n % RING] = x;
        self.n += 1;
    }

    fn flush(&mut self, persons: &Persons, mut done: impl FnMut(T, Key)) {
        for i in self.n.saturating_sub(RING)..self.n {
            let old = self.items[i % RING];
            done(old, persons.get(old.user()));
        }
        self.n = 0;
    }
}

struct ParseState {
    thread: usize,
    buf: Vec<u8>,
    events: scatter::Scatter<DedupRec, 4>,
    sess: scatter::Scatter<PEv, 4>,
    pcs: Vec<u32>, // price << 8 | category of this thread's valid copies, by sequence number
    clicks: scatter::Scatter<ClickRec, 2>,
    nseq: usize,
    ev_ring: Ring<EvPending>,
    ck_ring: Ring<ClickRec>,
}

struct DedupState {
    table: Vec<DedupRec>,
    used: Vec<u32>,
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

    // ---- links on the whole pool (parsing needs them: events and clicks go straight to their
    // person's bucket), while one more thread loads the lookups and plans the input chunks
    let ((ctx, events_in, clicks_in, ev_chunks, ck_chunks), persons) = std::thread::scope(|scope| {
        let setup = scope.spawn(|| {
            let ctx = load_context(&data, start);
            let events_in = csv::Input::open(&csv::list_csv(&data.join("events")));
            let clicks_in = csv::Input::open(&csv::list_csv(&data.join("clicks")));
            let ev_chunks = events_in.chunks(chunk_size());
            let ck_chunks = clicks_in.chunks(chunk_size());
            (ctx, events_in, clicks_in, ev_chunks, ck_chunks)
        });
        let persons = build_links(&data, threads);
        log(start, &format!("links: {} non-name IDs mapped", persons.len()));
        (setup.join().expect("setup thread panicked"), persons)
    });
    log(start, "setup joined");
    if let Ok(mode) = std::env::var("SS_BENCH") {
        bench(&ctx, &data, &mode);
        return;
    }

    // Bucket counts. Parsing writes into every bucket at once through one staging line per
    // bucket, so the fanout is capped to keep those lines in cache: measured at 8 threads,
    // 4096/2048 buckets ran 19% slower than 1024/512. Within that cap, buckets are sized so a
    // dedup table (event-id) or a sessions task (person) stays small.
    let np: usize = std::env::var("SS_NP")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or((events_in.bytes() / 76 / 8192).next_power_of_two().clamp(64, 1024))
        .clamp(64, 4096);
    let nq: usize = std::env::var("SS_NQ")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or((events_in.bytes() / 76 / 16384).next_power_of_two().clamp(64, 512))
        .clamp(64, 4096);
    // Capacities with headroom: a doubling copy of a big array costs far more than slack.
    let ev_estimate = events_in.bytes() / 56 / np / threads + 64;
    let sess_estimate = events_in.bytes() / 56 / nq / threads + 64;
    let ck_estimate = clicks_in.bytes() / 30 / nq / threads + 64;
    let pcs_estimate = events_in.bytes() / 56 / threads + 64;
    assert!(ctx.we - ctx.ws < PURCHASE as i64, "window too long for 31-bit time offsets");
    let n_tasks = ev_chunks.len() + ck_chunks.len();

    // ---- parse: each event copy goes to its event-id bucket (for dedup) and, if valid, to its
    // person's bucket; each click to its person's bucket. Person lookups run in a prefetch ring.
    let thread_ids = AtomicUsize::new(0);
    let parsed = parallel(
        "parse",
        threads,
        n_tasks,
        || ParseState {
            thread: thread_ids.fetch_add(1, Ordering::Relaxed),
            buf: Vec::new(),
            events: scatter::Scatter::new(np, ev_estimate),
            sess: scatter::Scatter::new(nq, sess_estimate),
            pcs: Vec::with_capacity(pcs_estimate),
            clicks: scatter::Scatter::new(nq, ck_estimate),
            nseq: 0,
            ev_ring: Ring::new(),
            ck_ring: Ring::new(),
        },
        |st, task| {
            let ParseState { thread, buf, events, sess, pcs, clicks, nseq, ev_ring, ck_ring } = st;
            let thread = *thread as u32;
            let mut to_bucket = |x: EvPending, person: Key| {
                let flag = if x.pc != 0 { PURCHASE } else { 0 };
                sess.push(bucket(person, nq), PEv { person, ts: x.ts | flag, tseq: x.tseq });
            };
            if task < ev_chunks.len() {
                events_in.records::<7>(ev_chunks[task], buf, |f| {
                    if let Some(rec) = parse_event(&ctx, f) {
                        let pref = if rec.ts == INVALID {
                            NOREF
                        } else {
                            assert!(*nseq < (1 << REF_BITS) - 1, "too many events on one thread for a 26-bit reference");
                            let pref = thread << REF_BITS | *nseq as u32;
                            *nseq += 1;
                            pcs.push(rec.pc);
                            let pending = EvPending { user: rec.user, ts: rec.ts, pc: rec.pc, tseq: pref };
                            ev_ring.push(&persons, pending, &mut to_bucket);
                            pref
                        };
                        events.push(bucket(rec.eid, np), DedupRec { eid: rec.eid, ingest: rec.ingest, pref });
                    }
                });
                ev_ring.flush(&persons, &mut to_bucket);
            } else {
                let mut click_to_bucket = |mut c: ClickRec, person: Key| {
                    c.person = person;
                    clicks.push(bucket(person, nq), c);
                };
                clicks_in.records::<4>(ck_chunks[task - ev_chunks.len()], buf, |f| {
                    if let Some(rec) = parse_click(&ctx, f) {
                        ck_ring.push(&persons, rec, &mut click_to_bucket);
                    }
                });
                ck_ring.flush(&persons, &mut click_to_bucket);
            }
        },
    );
    log(start, &format!("parsed {} MB events, {} MB clicks", events_in.bytes() >> 20, clicks_in.bytes() >> 20));
    let mut ev_threads = Vec::with_capacity(parsed.len());
    let mut sess_threads = Vec::with_capacity(parsed.len());
    let mut ck_threads = Vec::with_capacity(parsed.len());
    let mut valid = vec![0usize; threads];
    let mut pcs: Vec<Vec<u32>> = (0..threads).map(|_| Vec::new()).collect();
    for st in parsed {
        valid[st.thread] = st.nseq;
        pcs[st.thread] = st.pcs;
        ev_threads.push(st.events.finish());
        sess_threads.push(st.sess.finish());
        ck_threads.push(st.clicks.finish());
    }
    let ev_slots = regroup(ev_threads, np);
    let sess_slots = regroup(sess_threads, nq);
    let ck_slots = regroup(ck_threads, nq);
    // One bit per valid copy, set when that copy lost its event's dedup.
    let losers: Vec<Vec<AtomicU64>> = valid.iter().map(|&n| (0..n / 64 + 1).map(|_| AtomicU64::new(0)).collect()).collect();

    // ---- dedup on event_id: per bucket, the copy with the smallest ingest_ts is kept and every
    // other valid copy is marked a loser. Duplicates are a few percent of rows, so this phase
    // only reads 16-byte records and probes a small table.
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
                for r in piece.as_slice() {
                    let mut slot = key::hash(r.eid) as usize & mask;
                    loop {
                        let s = &mut st.table[slot];
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
            // Freed here, the bucket's memory is reused by the sessions phase (no new pages).
            drop(owned);
        },
    );
    drop(deduped);
    drop(ev_slots);
    log(start, "deduplicated events");
    let lost = |x: &PEv| {
        let at = (x.tseq & ((1 << REF_BITS) - 1)) as usize;
        losers[(x.tseq >> REF_BITS) as usize][at / 64].load(Ordering::Relaxed) >> (at % 64) & 1 != 0
    };

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
            // Copies that lost dedup are dropped on the way in.
            let nb = (n_ev / 8).next_power_of_two().clamp(16, 1 << 16);
            let bin = |p: Key| ((key::hash(p) >> 16) as usize) & (nb - 1);
            let ev_slices: Vec<&[PEv]> = ev_pieces.iter().map(|p| p.as_slice()).collect();
            let ck_slices: Vec<&[ClickRec]> = ck_pieces.iter().map(|p| p.as_slice()).collect();
            let (mut evs, ev_at) = bin_scatter(&ev_slices, nb, |x: &PEv| bin(x.person), |x: &PEv| !lost(x), |x: &PEv| {
                let pc = if x.ts & PURCHASE != 0 {
                    pcs[(x.tseq >> REF_BITS) as usize][(x.tseq & ((1 << REF_BITS) - 1)) as usize]
                } else {
                    0
                };
                (x.person as u128) << 64 | ((x.ts & !PURCHASE) as u128) << 32 | pc as u128
            });
            let (mut cks, ck_at) = bin_scatter(&ck_slices, nb, |c: &ClickRec| bin(c.person), |_| true, |c: &ClickRec| *c);
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
                cks.sort_unstable_by_key(|c| (c.person, c.ts, c.cid));
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
    let cat_bytes: Vec<bytes::Bytes> = ctx.categories.iter().map(|c| bytes::Bytes::from(c.clone())).collect();
    let campaign_rows = AtomicUsize::new(0);
    parallel("write", threads, pieces.len() + 1, || (), |_, task| {
        if task == pieces.len() {
            campaign_rows.store(write_campaigns(&ctx, &camp_total, &cd_dir), Ordering::Relaxed);
            return;
        }
        let (li, s, e) = pieces[task];
        let rows = &row_lists.get(li).map_or(&[][..], |l| &l[s..e]);
        let n = rows.len();
        let mut user_id = Vec::with_capacity(n);
        let mut day = Vec::with_capacity(n);
        let mut sessions = Vec::with_capacity(n);
        let mut events = Vec::with_capacity(n);
        let mut revenue = Vec::with_capacity(n);
        let mut top = Vec::with_capacity(n);
        let mut top_def = Vec::with_capacity(n);
        // All names in one buffer, sliced per row: no allocation per string.
        let mut names = Vec::with_capacity(n * 10);
        let mut ends = Vec::with_capacity(n);
        for r in rows.iter() {
            key::append_bytes(r.person, &mut names);
            ends.push(names.len());
        }
        let names = bytes::Bytes::from(names);
        let mut from = 0;
        for (r, &end) in rows.iter().zip(&ends) {
            user_id.push(ByteArray::from(names.slice(from..end)));
            from = end;
            day.push(r.day);
            sessions.push(r.sessions);
            events.push(r.events);
            revenue.push(r.revenue);
            if r.top == NO_CATEGORY {
                top_def.push(0);
            } else {
                top_def.push(1);
                top.push(ByteArray::from(cat_bytes[r.top as usize].clone()));
            }
        }
        out::write(
            &ud_dir.join(format!("part-{task:05}.parquet")),
            out::USER_SCHEMA,
            vec![
                out::Column::Bytes(user_id),
                out::Column::I32(day),
                out::Column::I64(sessions),
                out::Column::I64(events),
                out::Column::I64(revenue),
                out::Column::OptBytes(top, top_def),
            ],
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
    let mut campaign_id = Vec::new();
    let mut channel = Vec::new();
    let mut day = Vec::new();
    let mut cols: [Vec<i64>; 4] = Default::default();
    for c in 0..=ncamp {
        for d in 0..ctx.ndays {
            let cell = camp_total[c * ctx.ndays + d];
            if cell[0] == 0 && cell[3] == 0 {
                continue;
            }
            if c == ncamp {
                campaign_id.push(ByteArray::from(&b"organic"[..]));
                channel.push(ByteArray::from(&b"organic"[..]));
            } else {
                campaign_id.push(ByteArray::from(ctx.campaign_ids[c].clone()));
                channel.push(ByteArray::from(ctx.channels[c].clone()));
            }
            day.push((ctx.day0 + d as i64) as i32);
            for k in 0..4 {
                cols[k].push(cell[k]);
            }
        }
    }
    let n = day.len();
    let [s, p, r, a] = cols;
    out::write(
        &cd_dir.join("part-00000.parquet"),
        out::CAMPAIGN_SCHEMA,
        vec![
            out::Column::Bytes(campaign_id),
            out::Column::Bytes(channel),
            out::Column::I32(day),
            out::Column::I64(s),
            out::Column::I64(p),
            out::Column::I64(r),
            out::Column::I64(a),
        ],
        n,
    );
    n
}
