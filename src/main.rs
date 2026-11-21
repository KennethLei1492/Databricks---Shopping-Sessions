//! Shopping Sessions pipeline. See rules.md for the rules and DESIGN.md for the plan.
//!
//! Stages (each data-parallel over NCPU threads, no shared mutable state except taken buckets):
//!   1. lookups + manifest
//!   2. parse events into buckets by hash(event_id) and clicks into per-thread lists, while one
//!      extra thread parses links and runs union-find (off the critical path)
//!   3. per event-id bucket: dedup (min ingest_ts), keep valid survivors, bucket by hash(person);
//!      click lists: map user -> person, bucket by hash(person)
//!   4. per person bucket: sort, sessionize, credit + assists, aggregate
//!   5. write Parquet

mod csv;
mod key;
mod out;
mod ts;

use key::{join, pack, split, Key, KeyMap};
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
const CHUNK: usize = 8 << 20;

/// Chunk size; SS_CHUNK overrides it so tests can force many chunk boundaries.
fn chunk_size() -> usize {
    std::env::var("SS_CHUNK").ok().and_then(|v| v.parse().ok()).unwrap_or(CHUNK)
}
const VIEW: u8 = 1;
const CART: u8 = 2;
const PURCHASE: u8 = 3;
const NO_CATEGORY: u16 = u16::MAX;

#[derive(Clone, Copy)]
struct EvRec {
    eid: [u64; 2],
    user: [u64; 2],
    ingest: i64,
    ts: u32, // ms since window_start
    price: u32,
    cat: u16,
    kind: u8, // 0: invalid kept copy (still competes in dedup)
}

#[derive(Clone, Copy)]
struct SessRec {
    person: [u64; 2],
    ts: u32,
    price: u32,
    cat: u16,
    kind: u8,
}

#[derive(Clone, Copy)]
struct ClickRec {
    person: [u64; 2],
    cid: [u64; 2],
    ts: i64,
    camp: u32,
}

struct Lookup<V: Copy + Default> {
    short: KeyMap<V>,
    long: HashMap<Vec<u8>, V>,
}

impl<V: Copy + Default> Lookup<V> {
    #[inline]
    fn get(&self, s: &[u8]) -> Option<V> {
        match key::pack_short(s) {
            Some(k) => self.short.get(k),
            None => self.long.get(s).copied(),
        }
    }
}

struct Ctx {
    ws: i64,
    we: i64,
    products: Lookup<u16>,
    unknown: u16,
    categories: Vec<Vec<u8>>,
    campaigns: Lookup<u32>,
    campaign_ids: Vec<Vec<u8>>,
    channels: Vec<Vec<u8>>,
    day0: i64,
    ndays: usize,
}

fn log(start: Instant, what: &str) {
    eprintln!("[{:>8.3}s] {what}", start.elapsed().as_secs_f64());
}

/// Runs `work(state, task)` for tasks 0..n on `threads` threads, pulling tasks dynamically.
fn parallel<S: Send>(threads: usize, n: usize, init: impl Fn() -> S + Sync, work: impl Fn(&mut S, usize) + Sync) -> Vec<S> {
    let next = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                scope.spawn(|| {
                    let mut state = init();
                    loop {
                        let task = next.fetch_add(1, Ordering::Relaxed);
                        if task >= n {
                            break;
                        }
                        work(&mut state, task);
                    }
                    state
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().expect("worker panicked")).collect()
    })
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

/// Links -> person names: maps every linked ID that is not its person's name to that name.
/// Runs on one thread, concurrently with event and click parsing.
fn build_links(data: &Path) -> KeyMap<Key> {
    let input = csv::Input::open(&csv::list_csv(&data.join("links")));
    let mut buf = Vec::new();
    let mut pairs: Vec<(Key, Key)> = Vec::with_capacity(input.bytes() / 20);
    for chunk in input.chunks(chunk_size()) {
        input.records::<2>(chunk, &mut buf, |&[a, b]| {
            if !a.is_empty() && !b.is_empty() {
                pairs.push((pack(a), pack(b)));
            }
        });
    }
    let total = pairs.len();
    let t_union = Instant::now();
    // A person's k IDs need at least k-1 links, so distinct IDs are rarely far above the pair
    // count; the map grows if they are.
    let mut ids: KeyMap<u32> = KeyMap::with_capacity(total + total / 4);
    let mut keys: Vec<Key> = Vec::with_capacity(total + total / 4);
    let mut parent: Vec<u32> = Vec::with_capacity(total + total / 4);
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let up = parent[parent[x as usize] as usize];
            parent[x as usize] = up;
            x = up;
        }
        x
    }
    {
        for &(a, b) in &pairs {
            let mut id = |k: Key| -> u32 {
                let next = keys.len() as u32;
                let (slot, present) = ids.entry(k, next);
                if !present {
                    keys.push(k);
                    parent.push(next);
                }
                *slot
            };
            let ia = id(a);
            let ib = id(b);
            let ra = find(&mut parent, ia);
            let rb = find(&mut parent, ib);
            if ra != rb {
                // Union into the root with the smaller key, so each root is its set's name.
                if key::cmp(keys[ra as usize], keys[rb as usize]).is_lt() {
                    parent[rb as usize] = ra;
                } else {
                    parent[ra as usize] = rb;
                }
            }
        }
    }
    eprintln!("links: {total} pairs, {} ids, union-find {:.3}s", keys.len(), t_union.elapsed().as_secs_f64());
    let mut persons: KeyMap<Key> = KeyMap::with_capacity(keys.len());
    for i in 0..keys.len() {
        let root = find(&mut parent, i as u32) as usize;
        if root != i {
            persons.entry(keys[i], keys[root]);
        }
    }
    persons
}

fn load_context(data: &Path, start: Instant) -> Ctx {
    let manifest = String::from_utf8_lossy(&read_small(&data.join("manifest.json"))).into_owned();
    let ws = manifest_value(&manifest, "window_start");
    let we = manifest_value(&manifest, "window_end");
    assert!(we > ws && we - ws < u32::MAX as i64, "window too long for u32 offsets");

    let products_raw = read_small(&data.join("products.csv"));
    let product_rows = two_columns(&products_raw);
    let mut categories: Vec<Vec<u8>> = product_rows.iter().map(|r| r.1.to_vec()).collect();
    categories.push(b"unknown".to_vec());
    categories.sort();
    categories.dedup();
    assert!(categories.len() < NO_CATEGORY as usize);
    let cat_index: HashMap<&[u8], u16> =
        categories.iter().enumerate().map(|(i, c)| (c.as_slice(), i as u16)).collect();
    let unknown = cat_index[&b"unknown"[..]];
    let mut products = Lookup { short: KeyMap::with_capacity(product_rows.len()), long: HashMap::new() };
    for (id, cat) in &product_rows {
        let c = cat_index[cat];
        match key::pack_short(id) {
            Some(k) => *products.short.entry(k, c).0 = c,
            None => {
                products.long.insert(id.to_vec(), c);
            }
        }
    }

    let campaigns_raw = read_small(&data.join("campaigns.csv"));
    let campaign_rows = two_columns(&campaigns_raw);
    let mut campaigns = Lookup { short: KeyMap::with_capacity(campaign_rows.len()), long: HashMap::new() };
    let mut campaign_ids = Vec::new();
    let mut channels = Vec::new();
    for (id, channel) in &campaign_rows {
        let at = campaign_ids.len() as u32;
        campaign_ids.push(id.to_vec());
        channels.push(channel.to_vec());
        match key::pack_short(id) {
            Some(k) => *campaigns.short.entry(k, at).0 = at,
            None => {
                campaigns.long.insert(id.to_vec(), at);
            }
        }
    }
    log(start, &format!("lookups: {} products, {} categories, {} campaigns", product_rows.len(), categories.len(), campaign_ids.len()));

    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1) as usize;
    Ctx { ws, we, products, unknown, categories, campaigns, campaign_ids, channels, day0, ndays }
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
fn parse_event(ctx: &Ctx, f: &[&[u8]; 7]) -> Option<EvRec> {
    if f[0].is_empty() {
        return None;
    }
    let ingest = ts::parse(f[3]).unwrap_or(i64::MAX);
    let mut rec = EvRec { eid: split(pack(f[0])), user: [0; 2], ingest, ts: 0, price: 0, cat: 0, kind: 0 };
    let kind = match f[4] {
        b"view" => VIEW,
        b"cart" => CART,
        b"purchase" => PURCHASE,
        _ => return Some(rec),
    };
    if f[1].is_empty() || f[5].is_empty() {
        return Some(rec);
    }
    let Some(t) = ts::parse(f[2]) else { return Some(rec) };
    if t < ctx.ws || t >= ctx.we || ingest.saturating_sub(t) > DAY {
        return Some(rec);
    }
    if kind == PURCHASE {
        let Some(price) = parse_price(f[6]) else { return Some(rec) };
        rec.price = price;
        rec.cat = ctx.products.get(f[5]).unwrap_or(ctx.unknown);
    }
    rec.user = split(pack(f[1]));
    rec.ts = (t - ctx.ws) as u32;
    rec.kind = kind;
    Some(rec)
}

#[inline]
fn parse_click(ctx: &Ctx, f: &[&[u8]; 4]) -> Option<ClickRec> {
    if f[0].is_empty() || f[1].is_empty() {
        return None;
    }
    let camp = ctx.campaigns.get(f[2])?;
    let t = ts::parse(f[3])?;
    if t < ctx.ws - SEVEN_DAYS || t >= ctx.we {
        return None;
    }
    // `person` holds the user ID until links are known (phase 3 maps it).
    Some(ClickRec { person: split(pack(f[1])), cid: split(pack(f[0])), ts: t, camp })
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

fn process_person(ctx: &Ctx, agg: &mut Agg, person: Key, evs: &[SessRec], clicks: &[ClickRec]) {
    let organic = ctx.campaign_ids.len();
    let ndays = ctx.ndays;
    let (mut l, mut r) = (0usize, 0usize);
    let mut cur_day = i64::MIN;
    let (mut d_sessions, mut d_events, mut d_revenue) = (0i64, 0i64, 0i64);
    let mut s = 0;
    while s < evs.len() {
        let mut e = s + 1;
        while e < evs.len() && (evs[e].ts - evs[e - 1].ts) as i64 <= GAP {
            e += 1;
        }
        let start = ctx.ws + evs[s].ts as i64;
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
        for ev in &evs[s..e] {
            if ev.kind == PURCHASE {
                revenue += ev.price as i64;
                purchased = true;
                agg.catsum[ev.cat as usize] += ev.price as i64;
            }
        }
        d_sessions += 1;
        d_events += (e - s) as i64;
        d_revenue += revenue;

        while r < clicks.len() && clicks[r].ts <= start {
            r += 1;
        }
        while l < r && clicks[l].ts < start - SEVEN_DAYS {
            l += 1;
        }
        let di = (day - ctx.day0) as usize;
        let credited = if l < r {
            let t = clicks[r - 1].ts;
            let mut k = r - 1;
            while k > l && clicks[k - 1].ts == t {
                k -= 1;
            }
            clicks[k].camp as usize
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
                    buckets[bucket(join(rec.eid), NP)].push(rec);
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

    let ctx = load_context(&data, start);

    if let Ok(mode) = std::env::var("SS_BENCH") {
        bench(&ctx, &data, &mode);
        return;
    }

    // ---- parse events (bucketed by event_id) and clicks; links on their own thread meanwhile
    let events_in = csv::Input::open(&csv::list_csv(&data.join("events")));
    let clicks_in = csv::Input::open(&csv::list_csv(&data.join("clicks")));
    let ev_chunks = events_in.chunks(chunk_size());
    let ck_chunks = clicks_in.chunks(chunk_size());
    let ev_estimate = events_in.bytes() / 80 / NP / threads + 16;
    let ck_estimate = clicks_in.bytes() / 40 / threads + 16;
    let n_tasks = ev_chunks.len() + ck_chunks.len();
    let (parsed, persons) = std::thread::scope(|scope| {
        let links = scope.spawn(|| {
            let persons = build_links(&data);
            log(start, &format!("links: {} non-name IDs mapped", persons.len()));
            persons
        });
        let parsed = parallel(
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
                            events[bucket(join(rec.eid), NP)].push(rec);
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
        (parsed, links.join().expect("links thread panicked"))
    });
    let mut ev_threads = Vec::with_capacity(parsed.len());
    let mut ck_lists = Vec::with_capacity(parsed.len());
    for st in parsed {
        ev_threads.push(st.events);
        ck_lists.push(Mutex::new(st.clicks));
    }
    let ev_slots = regroup(ev_threads, NP);

    // ---- dedup events on event_id, keep valid kept copies; map users to persons; bucket by person
    let person_of = |user: Key| persons.get(user).unwrap_or(user);
    let n_lists = ck_lists.len();
    let sess_estimate = events_in.bytes() / 85 / NQ / threads + 16;
    let ck_bucket_estimate = clicks_in.bytes() / 40 / NQ / threads + 16;
    let regrouped = parallel(
        threads,
        NP + n_lists,
        || {
            (
                (0..NQ).map(|_| Vec::<SessRec>::with_capacity(sess_estimate)).collect::<Vec<_>>(),
                (0..NQ).map(|_| Vec::<ClickRec>::with_capacity(ck_bucket_estimate)).collect::<Vec<_>>(),
            )
        },
        |(out, ck_out), task| {
            if task >= NP {
                let list = std::mem::take(&mut *ck_lists[task - NP].lock().unwrap());
                for mut c in list {
                    let person = person_of(join(c.person));
                    c.person = split(person);
                    ck_out[bucket(person, NQ)].push(c);
                }
                return;
            }
            // Index each thread's piece in place (piece << 24 | position) instead of copying
            // the pieces into one vector first.
            let pieces = std::mem::take(&mut *ev_slots[task].lock().unwrap());
            let n: usize = pieces.iter().map(|p| p.len()).sum();
            let at = |v: u32| &pieces[(v >> 24) as usize][(v & 0xFF_FFFF) as usize];
            let mut map: KeyMap<u32> = KeyMap::with_capacity(n);
            for (pi, piece) in pieces.iter().enumerate() {
                assert!(pi < 256 && piece.len() < 1 << 24, "bucket piece too large to index");
                for (i, r) in piece.iter().enumerate() {
                    let v = (pi as u32) << 24 | i as u32;
                    let (slot, present) = map.entry(join(r.eid), v);
                    if present && r.ingest < at(*slot).ingest {
                        *slot = v;
                    }
                }
            }
            for (_, v) in map.iter() {
                let r = at(v);
                if r.kind != 0 {
                    let person = person_of(join(r.user));
                    out[bucket(person, NQ)].push(SessRec { person: split(person), ts: r.ts, price: r.price, cat: r.cat, kind: r.kind });
                }
            }
        },
    );
    drop(ev_slots);
    let (sess_threads, ck_threads): (Vec<_>, Vec<_>) = regrouped.into_iter().unzip();
    let sess_slots = regroup(sess_threads, NQ);
    let ck_slots = regroup(ck_threads, NQ);
    log(start, "deduplicated events");

    // ---- sessions, attribution, aggregation per person bucket
    let ncamp = ctx.campaign_ids.len();
    let aggs = parallel(
        threads,
        NQ,
        || Agg {
            rows: Vec::new(),
            camp: vec![[0; 4]; (ncamp + 1) * ctx.ndays],
            stamp: vec![0; ncamp + 1],
            serial: 0,
            catsum: vec![0; ctx.categories.len()],
        },
        |agg, q| {
            let mut evs = take_all(&sess_slots[q]);
            let mut cks = take_all(&ck_slots[q]);
            evs.sort_unstable_by(|a, b| join(a.person).cmp(&join(b.person)).then(a.ts.cmp(&b.ts)));
            cks.sort_unstable_by(|a, b| {
                join(a.person).cmp(&join(b.person)).then(a.ts.cmp(&b.ts)).then_with(|| key::cmp(join(a.cid), join(b.cid)))
            });
            let mut ci = 0;
            let mut i = 0;
            while i < evs.len() {
                let p = join(evs[i].person);
                let mut j = i + 1;
                while j < evs.len() && join(evs[j].person) == p {
                    j += 1;
                }
                while ci < cks.len() && join(cks[ci].person) < p {
                    ci += 1;
                }
                let c0 = ci;
                while ci < cks.len() && join(cks[ci].person) == p {
                    ci += 1;
                }
                process_person(&ctx, agg, p, &evs[i..j], &cks[c0..ci]);
                i = j;
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
    let row_lists: Vec<Mutex<Vec<UserRow>>> = row_lists.into_iter().map(Mutex::new).collect();
    let written = AtomicUsize::new(0);
    parallel(threads, row_lists.len(), || (), |_, t| {
        let rows = std::mem::take(&mut *row_lists[t].lock().unwrap());
        if rows.is_empty() && t != 0 {
            return;
        }
        written.fetch_add(rows.len(), Ordering::Relaxed);
        let n = rows.len();
        let mut user_id = Vec::with_capacity(n);
        let mut day = Vec::with_capacity(n);
        let mut sessions = Vec::with_capacity(n);
        let mut events = Vec::with_capacity(n);
        let mut revenue = Vec::with_capacity(n);
        let mut top = Vec::with_capacity(n);
        let mut top_def = Vec::with_capacity(n);
        for r in &rows {
            user_id.push(ByteArray::from(key::bytes(r.person)));
            day.push(r.day);
            sessions.push(r.sessions);
            events.push(r.events);
            revenue.push(r.revenue);
            if r.top == NO_CATEGORY {
                top_def.push(0);
            } else {
                top_def.push(1);
                top.push(ByteArray::from(ctx.categories[r.top as usize].clone()));
            }
        }
        out::write(
            &ud_dir.join(format!("part-{t:05}.parquet")),
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
    log(start, &format!("wrote {} user_daily rows, {n} campaign_daily rows", written.load(Ordering::Relaxed)));
}
