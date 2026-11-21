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
mod ts;

use key::{pack, Key, KeyMap};
use parquet::data_type::ByteArray;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Instant;
use ts::DAY;

// mimalloc keeps freed memory for reuse instead of returning large blocks to the OS (glibc
// munmap/mmap churn costs page faults and serializes threads on the mmap lock).
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const GAP: i64 = 1_800_000;
const SEVEN_DAYS: i64 = 7 * DAY;
const NP: usize = 512; // event-id buckets
const NQ: usize = 512; // person buckets
const CHUNK: usize = 4 << 20;
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
struct ClickRec {
    person: Key,
    cid: Key,
    ts: u32,
    camp: u32,
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
    let results: Vec<(S, f64, f64, usize)> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                scope.spawn(|| {
                    let mut state = init();
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
                    (state, busy, longest, longest_task)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().expect("worker panicked")).collect()
    });
    let wall = t0.elapsed().as_secs_f64();
    let busy: f64 = results.iter().map(|r| r.1).sum();
    let (longest, longest_task) = results.iter().map(|r| (r.2, r.3)).fold((0f64, 0), |a, b| if b.0 > a.0 { b } else { a });
    eprintln!(
        "  phase {name}: {n} tasks, wall {wall:.3}s, utilization {:.0}%, longest task #{longest_task} {longest:.3}s",
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

/// Person names for linked IDs that are not their person's name, in hash-partitioned maps that
/// are built in parallel.
struct Persons {
    parts: Vec<KeyMap<Key>>,
}

impl Persons {
    #[inline(always)]
    fn get(&self, k: Key) -> Key {
        self.parts[link_part(k)].get(k).unwrap_or(k)
    }

    #[inline(always)]
    fn prefetch(&self, k: Key) {
        self.parts[link_part(k)].prefetch(k)
    }

    fn len(&self) -> usize {
        self.parts.iter().map(|m| m.len()).sum()
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
    let maps = parallel("links-names", threads, LINK_PARTS, Vec::new, |out: &mut Vec<(usize, KeyMap<Key>)>, p| {
        let (lo, hi) = (base[p] as usize, base[p + 1] as usize);
        let mut m: KeyMap<Key> = KeyMap::with_capacity(hi - lo);
        for id in lo..hi {
            let mut r = id as u32;
            while node[r as usize][0] != r {
                r = node[r as usize][0];
            }
            let nm = name[r as usize];
            if nm != keys[id] {
                m.entry(keys[id], nm);
            }
        }
        out.push((p, m));
    });
    let mut slots: Vec<Option<KeyMap<Key>>> = (0..LINK_PARTS).map(|_| None).collect();
    for list in maps {
        for (p, m) in list {
            slots[p] = Some(m);
        }
    }
    let persons = Persons { parts: slots.into_iter().map(|m| m.expect("partition map")).collect() };
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

#[inline]
fn parse_event(ctx: &Ctx, r: &csv::Rec<'_, 7>) -> Option<EvRec> {
    let f = r.fields();
    if f[0].is_empty() {
        return None;
    }
    let ingest_ms = ts::parse_wide(r.wide(3), f[3]);
    let ingest = match ingest_ms {
        Some(ms) => (ms.div_euclid(1000) - ctx.ingest_base_s).clamp(0, u32::MAX as i64 - 1) as u32,
        None => u32::MAX,
    };
    let mut rec = EvRec { eid: key::pack_wide(r.wide(0), f[0]), user: 0, ingest, ts: INVALID, pc: 0 };
    let purchase = match f[4] {
        b"view" | b"cart" => false,
        b"purchase" => true,
        _ => return Some(rec),
    };
    if f[1].is_empty() || f[5].is_empty() {
        return Some(rec);
    }
    let Some(t) = ts::parse_wide(r.wide(2), f[2]) else { return Some(rec) };
    let Some(ingest_ms) = ingest_ms else { return Some(rec) };
    if t < ctx.ws || t >= ctx.we || ingest_ms - t > DAY {
        return Some(rec);
    }
    if purchase {
        let Some(price) = parse_price(f[6]) else { return Some(rec) };
        let cat = ctx.products.get(f[5]).unwrap_or(ctx.unknown);
        rec.pc = price << 8 | cat;
    }
    rec.user = key::pack_wide(r.wide(1), f[1]);
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
    Some(ClickRec { person: key::pack_wide(r.wide(1), f[1]), cid: key::pack_wide(r.wide(0), f[0]), ts: (t - ctx.click_base) as u32, camp })
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
    stamp: Vec<u32>,
    serial: u32,
    catsum: Vec<i64>,
}

fn flush_day(agg: &mut Agg, person: Key, day: i64, sessions: i64, events: i64, revenue: i64) {
    let mut top = NO_CATEGORY;
    let mut best = 0;
    for (i, s) in agg.catsum.iter_mut().enumerate() {
        if *s > best {
            best = *s;
            top = i as u16;
        }
        *s = 0;
    }
    agg.rows.push(UserRow { person, day: day as i32, sessions, events, revenue, top });
}

#[inline(always)]
fn ev_ts(x: u128) -> u32 {
    (x >> 32) as u32
}

fn process_person(ctx: &Ctx, agg: &mut Agg, person: Key, evs: &[u128], clicks: &[ClickRec]) {
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
            r += 1;
        }
        while l < r && (clicks[l].ts as i64) < lo {
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
        if l < r {
            agg.serial += 1;
            let serial = agg.serial;
            for c in &clicks[l..r] {
                let camp = c.camp as usize;
                if agg.stamp[camp] != serial {
                    agg.stamp[camp] = serial;
                    agg.camp[camp * ndays + di][3] += 1;
                }
            }
        }
        s = e;
    }
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

/// Moves per-thread bucket lists into per-bucket slots: slots[b] holds every thread's bucket b.
fn regroup<T>(per_thread: Vec<Vec<Vec<T>>>, n: usize) -> Vec<Mutex<Vec<Vec<T>>>> {
    let mut slots: Vec<Vec<Vec<T>>> = (0..n).map(|_| Vec::with_capacity(per_thread.len())).collect();
    for buckets in per_thread {
        for (b, v) in buckets.into_iter().enumerate() {
            if !v.is_empty() {
                slots[b].push(v);
            }
        }
    }
    slots.into_iter().map(Mutex::new).collect()
}

/// Counting-sort scatter of several pieces into `nb` bins: returns the binned records and the
/// start of each bin (nb + 1 offsets). Reads each piece in place, so no concatenation copy.
fn bin_scatter<T: Copy>(pieces: &[Vec<T>], nb: usize, bin: impl Fn(&T) -> usize) -> (Vec<T>, Vec<usize>) {
    let mut at = vec![0usize; nb + 1];
    for piece in pieces {
        for x in piece {
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
        for x in piece {
            let b = bin(x);
            spare[next[b]].write(*x);
            next[b] += 1;
        }
    }
    // SAFETY: the counting pass sized each bin exactly, so all n slots were written above.
    unsafe { out.set_len(n) };
    (out, at)
}

fn slot_len<T>(slot: &Mutex<Vec<Vec<T>>>) -> usize {
    slot.lock().unwrap().iter().map(|p| p.len()).sum()
}

/// Single-threaded micro-benchmarks of the event parse path (SS_BENCH=lines|split|event|push).
fn bench(ctx: &Ctx, data: &Path, mode: &str) {
    let input = csv::Input::open(&csv::list_csv(&data.join("events")));
    let chunks = input.chunks(chunk_size());
    let t = Instant::now();
    let mut n = 0u64;
    let mut buf = Vec::new();
    let mut buckets: Vec<Vec<EvRec>> = (0..NP).map(|_| Vec::new()).collect();
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
                    buckets[bucket(rec.eid, NP)].push(rec);
                    n += 1;
                }
            }
        });
    }
    let secs = t.elapsed().as_secs_f64();
    eprintln!("bench {mode}: {n} in {secs:.3}s = {:.0} MB/s", input.bytes() as f64 / secs / 1e6);
}

struct ParseState {
    buf: Vec<u8>,
    events: Vec<Vec<EvRec>>,
    clicks: Vec<ClickRec>,
}

struct RegroupState {
    sess: Vec<Vec<u128>>,
    clicks: Vec<Vec<ClickRec>>,
    users: Vec<(Key, u32, u32)>,
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
        .clamp(1, 256);

    // ---- links on the pool while one more thread loads the lookups and plans the input chunks
    // (links need neither), then events (bucketed by event_id) and clicks
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

    // Generous capacities: untouched capacity costs nothing, a doubling copy costs a lot.
    let ev_estimate = events_in.bytes() / 40 / NP / threads + 64;
    let ck_estimate = clicks_in.bytes() / 20 / threads + 64;
    let n_tasks = ev_chunks.len() + ck_chunks.len();
    let parsed = {
        let parsed = parallel(
            "parse",
            threads,
            n_tasks,
            || ParseState {
                buf: Vec::new(),
                events: (0..NP).map(|_| Vec::<EvRec>::with_capacity(ev_estimate)).collect(),
                clicks: Vec::with_capacity(ck_estimate),
            },
            |st, task| {
                let ParseState { buf, events, clicks } = st;
                if task < ev_chunks.len() {
                    events_in.records::<7>(ev_chunks[task], buf, |f| {
                        if let Some(rec) = parse_event(&ctx, f) {
                            events[bucket(rec.eid, NP)].push(rec);
                        }
                    });
                } else {
                    clicks_in.records::<4>(ck_chunks[task - ev_chunks.len()], buf, |f| {
                        if let Some(rec) = parse_click(&ctx, f) {
                            clicks.push(rec);
                        }
                    });
                }
            },
        );
        log(start, &format!("parsed {} MB events, {} MB clicks", events_in.bytes() >> 20, clicks_in.bytes() >> 20));
        parsed
    };
    let mut ev_threads = Vec::with_capacity(parsed.len());
    let mut ck_lists = Vec::with_capacity(parsed.len());
    for st in parsed {
        ev_threads.push(st.events);
        ck_lists.push(st.clicks);
    }
    let ev_slots = regroup(ev_threads, NP);
    // Click lists are cut into small ranges so no single task becomes a straggler.
    const CLICK_TASK: usize = 32 * 1024;
    let mut ck_ranges: Vec<(usize, usize, usize)> = Vec::new();
    for (li, list) in ck_lists.iter().enumerate() {
        for s in (0..list.len()).step_by(CLICK_TASK) {
            ck_ranges.push((li, s, (s + CLICK_TASK).min(list.len())));
        }
    }

    // ---- dedup events on event_id, keep valid kept copies; map users to persons; bucket by person
    let n_lists = ck_ranges.len();
    let sess_estimate = events_in.bytes() / 40 / NQ / threads + 64;
    let ck_bucket_estimate = clicks_in.bytes() / 20 / NQ / threads + 64;
    let regrouped = parallel(
        "dedup",
        threads,
        n_lists + NP,
        || RegroupState {
            sess: (0..NQ).map(|_| Vec::with_capacity(sess_estimate)).collect(),
            clicks: (0..NQ).map(|_| Vec::with_capacity(ck_bucket_estimate)).collect(),
            users: Vec::new(),
        },
        |st, task| {
            if task < n_lists {
                let (li, s, e) = ck_ranges[task];
                let list = &ck_lists[li][s..e];
                for i in 0..list.len() {
                    if let Some(next) = list.get(i + PREFETCH) {
                        persons.prefetch(next.person);
                    }
                    let mut c = list[i];
                    let person = persons.get(c.person);
                    c.person = person;
                    st.clicks[bucket(person, NQ)].push(c);
                }
                return;
            }
            // Index each thread's piece in place (piece << 24 | position).
            let pieces = std::mem::take(&mut *ev_slots[task - n_lists].lock().unwrap());
            let n: usize = pieces.iter().map(|p| p.len()).sum();
            let at = |v: u32| &pieces[(v >> 24) as usize][(v & 0xFF_FFFF) as usize];
            let mut map: KeyMap<u32> = KeyMap::with_capacity(n);
            for (pi, piece) in pieces.iter().enumerate() {
                assert!(pi < 256 && piece.len() < 1 << 24, "bucket piece too large to index");
                for (i, r) in piece.iter().enumerate() {
                    let v = (pi as u32) << 24 | i as u32;
                    let (slot, present) = map.entry(r.eid, v);
                    if present && r.ingest < at(*slot).ingest {
                        *slot = v;
                    }
                }
            }
            st.users.clear();
            for (_, v) in map.iter() {
                let r = at(v);
                if r.ts != INVALID {
                    st.users.push((r.user, r.ts, r.pc));
                }
            }
            for i in 0..st.users.len() {
                if let Some(next) = st.users.get(i + PREFETCH) {
                    persons.prefetch(next.0);
                }
                let (user, ts, pc) = st.users[i];
                let person = persons.get(user);
                st.sess[bucket(person, NQ)].push((person as u128) << 64 | (ts as u128) << 32 | pc as u128);
            }
        },
    );
    drop(ev_slots);
    let mut sess_threads = Vec::with_capacity(regrouped.len());
    let mut ck_threads = Vec::with_capacity(regrouped.len());
    for st in regrouped {
        sess_threads.push(st.sess);
        ck_threads.push(st.clicks);
    }
    let sess_slots = regroup(sess_threads, NQ);
    let ck_slots = regroup(ck_threads, NQ);
    log(start, "deduplicated events");

    // ---- sessions, attribution, aggregation per person bucket, largest first
    let mut order: Vec<usize> = (0..NQ).collect();
    let sizes: Vec<usize> = (0..NQ).map(|q| slot_len(&sess_slots[q]) + slot_len(&ck_slots[q])).collect();
    order.sort_by_key(|&q| std::cmp::Reverse(sizes[q]));
    let ncamp = ctx.campaign_ids.len();
    let aggs = parallel(
        "sessions",
        threads,
        NQ,
        || Agg {
            rows: Vec::new(),
            camp: vec![[0; 4]; (ncamp + 1) * ctx.ndays],
            stamp: vec![0; ncamp + 1],
            serial: 0,
            catsum: vec![0; ctx.categories.len()],
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
            let (mut evs, ev_at) = bin_scatter(&ev_pieces, nb, |x: &u128| bin((x >> 64) as u64));
            let (mut cks, ck_at) = bin_scatter(&ck_pieces, nb, |c: &ClickRec| bin(c.person));
            drop(ev_pieces);
            drop(ck_pieces);
            for b in 0..nb {
                let evs = &mut evs[ev_at[b]..ev_at[b + 1]];
                if evs.is_empty() {
                    continue;
                }
                let cks = &mut cks[ck_at[b]..ck_at[b + 1]];
                evs.sort_unstable();
                cks.sort_unstable_by_key(|c| (c.person, c.ts, c.cid));
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
                    process_person(&ctx, agg, p, &evs[i..j], &cks[c0..ci]);
                    i = j;
                }
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
