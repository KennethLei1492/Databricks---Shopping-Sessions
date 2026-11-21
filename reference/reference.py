#!/usr/bin/env python3
"""Independent, deliberately literal reference for rules.md. Slow by design: it exists only to
verify the fast pipeline (N-version agreement). Usage: reference.py DATA_DIR OUTPUT_DIR"""
import glob
import json
import os
import sys
from collections import defaultdict

import pyarrow as pa
import pyarrow.parquet as pq

DAY = 86_400_000
SEVEN_DAYS = 7 * DAY
STRINGS = {"user_id", "top_category", "campaign_id", "channel"}
GAP = 1_800_000


def is_digits(b):
    return len(b) > 0 and all(48 <= c <= 57 for c in b)


def days_from_civil(y, m, d):
    y -= m <= 2
    era = y // 400
    yoe = y - era * 400
    doy = (153 * ((m + 9) % 12) + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146097 + doe - 719468


def days_in_month(y, m):
    if m == 2:
        return 29 if (y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)) else 28
    return 30 if m in (4, 6, 9, 11) else 31


def parse_ts(b):
    """bytes -> epoch ms, or None when the form is bad. Window checks are separate."""
    if is_digits(b):
        return int(b)
    n = len(b)
    if n not in (20, 25):
        return None
    if b[4:5] != b"-" or b[7:8] != b"-" or b[10:11] != b"T" or b[13:14] != b":" or b[16:17] != b":":
        return None
    parts = [b[0:4], b[5:7], b[8:10], b[11:13], b[14:16], b[17:19]]
    if not all(is_digits(p) for p in parts):
        return None
    y, mo, d, h, mi, s = (int(p) for p in parts)
    if not (1 <= mo <= 12 and 1 <= d <= days_in_month(y, mo) and h <= 23 and mi <= 59 and s <= 59):
        return None
    if n == 20:
        if b[19:20] != b"Z":
            return None
        offset_min = 0
    else:
        sign = b[19:20]
        if sign not in (b"+", b"-") or b[22:23] != b":" or not is_digits(b[20:22]) or not is_digits(b[23:25]):
            return None
        oh, om = int(b[20:22]), int(b[23:25])
        if oh > 23 or om > 59:
            return None
        offset_min = (oh * 60 + om) * (1 if sign == b"+" else -1)
    local = (days_from_civil(y, mo, d) * 86400 + h * 3600 + mi * 60 + s) * 1000
    return local - offset_min * 60_000


def read_rows(folder, nfields):
    for path in sorted(glob.glob(os.path.join(folder, "*.csv"))):
        with open(path, "rb") as f:
            data = f.read()
        lines = data.split(b"\n")
        if data.endswith(b"\n"):
            lines = lines[:-1]
        for line in lines[1:]:  # the first line of every file is its header
            fields = line.split(b",")
            if len(fields) != nfields:
                continue
            out = []
            for field in fields:
                if b'"' in field:
                    if len(field) >= 2 and field[:1] == b'"' and field[-1:] == b'"' and b'"' not in field[1:-1]:
                        field = field[1:-1]
                    else:
                        out = None
                        break
                out.append(field)
            if out is not None:
                yield out


def read_lookup(path):
    rows = {}
    with open(path, "rb") as f:
        lines = f.read().split(b"\n")
    for line in lines[1:]:
        if line:
            k, v = line.split(b",")
            rows[k] = v
    return rows


def main():
    data, out = sys.argv[1], sys.argv[2]
    manifest = json.load(open(os.path.join(data, "manifest.json")))
    ws = parse_ts(manifest["window_start"].encode())
    we = parse_ts(manifest["window_end"].encode())
    products = read_lookup(os.path.join(data, "products.csv"))
    campaigns = read_lookup(os.path.join(data, "campaigns.csv"))

    # ---- links: union-find, person named by the byte-wise smallest ID
    parent = {}

    def find(x):
        root = x
        while parent[root] != root:
            root = parent[root]
        while parent[x] != root:
            parent[x], x = root, parent[x]
        return root

    for a, b in read_rows(os.path.join(data, "links"), 2):
        if not a or not b:
            continue
        for x in (a, b):
            parent.setdefault(x, x)
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[max(ra, rb)] = min(ra, rb)  # the root is always the smallest ID of its set

    def person(uid):
        return find(uid) if uid in parent else uid

    # ---- events: dedup on event_id (min ingest_ts), then validate the kept copy
    kept = {}
    for row in read_rows(os.path.join(data, "events"), 7):
        eid = row[0]
        if not eid:
            continue
        ingest = parse_ts(row[3])
        if eid not in kept or ingest < kept[eid][0]:
            kept[eid] = (ingest, row)
    by_person = defaultdict(list)
    for ingest, (eid, uid, ts_b, _, etype, pid, price_b) in kept.values():
        if not uid or not pid or etype not in (b"view", b"cart", b"purchase"):
            continue
        ts = parse_ts(ts_b)
        if ts is None or not (ws <= ts < we):
            continue
        price = 0
        if etype == b"purchase":
            if not is_digits(price_b):
                continue
            price = int(price_b)
            if not (1 <= price <= 10_000_000):
                continue
        if ingest - ts > DAY:
            continue
        category = products.get(pid, b"unknown")
        by_person[person(uid)].append((ts, etype == b"purchase", price, category))

    # ---- clicks
    clicks_by_id = {}
    for cid, uid, camp, ts_b in read_rows(os.path.join(data, "clicks"), 4):
        if cid:
            clicks_by_id[cid] = (uid, camp, ts_b)
    clicks = defaultdict(list)
    for cid, (uid, camp, ts_b) in clicks_by_id.items():
        if not uid or camp not in campaigns:
            continue
        ts = parse_ts(ts_b)
        if ts is None or not (ws - SEVEN_DAYS <= ts < we):
            continue
        clicks[person(uid)].append((ts, cid, camp))

    # ---- sessions, attribution, aggregation
    user_daily = {}
    camp_daily = defaultdict(lambda: [0, 0, 0, 0])
    for pid_, events in by_person.items():
        events.sort(key=lambda e: e[0])
        sessions = []
        for e in events:
            if not sessions or e[0] - sessions[-1][-1][0] > GAP:
                sessions.append([])
            sessions[-1].append(e)
        pclicks = clicks.get(pid_, [])
        for s in sessions:
            start = s[0][0]
            day = start // DAY
            revenue = sum(e[2] for e in s if e[1])
            purchased = any(e[1] for e in s)
            u = user_daily.setdefault((pid_, day), [0, 0, 0, defaultdict(int)])
            u[0] += 1
            u[1] += len(s)
            u[2] += revenue
            for e in s:
                if e[1]:
                    u[3][e[3]] += e[2]
            window = [c for c in pclicks if start - SEVEN_DAYS <= c[0] <= start]
            if window:
                best = min(window, key=lambda c: (-c[0], c[1]))
                credited = best[2]
            else:
                credited = b"organic"
            c = camp_daily[(credited, day)]
            c[0] += 1
            c[1] += purchased
            c[2] += revenue
            for camp in {c[2] for c in window}:
                camp_daily[(camp, day)][3] += 1

    epoch = __import__("datetime").date(1970, 1, 1)
    to_date = lambda d: epoch + __import__("datetime").timedelta(days=d)
    rows = sorted(user_daily.items())
    ud = {
        "user_id": [k[0].decode("utf-8", "surrogateescape") for k, _ in rows],
        "day": [to_date(k[1]) for k, _ in rows],
        "sessions": [v[0] for _, v in rows],
        "events": [v[1] for _, v in rows],
        "revenue_cents": [v[2] for _, v in rows],
        "top_category": [
            (min(v[3].items(), key=lambda kv: (-kv[1], kv[0]))[0].decode() if v[3] else None) for _, v in rows
        ],
    }
    crow = sorted(camp_daily.items())
    cd = {
        "campaign_id": [k[0].decode() for k, _ in crow],
        "channel": [(b"organic" if k[0] == b"organic" else campaigns[k[0]]).decode() for k, _ in crow],
        "day": [to_date(k[1]) for k, _ in crow],
        "sessions": [v[0] for _, v in crow],
        "purchasing_sessions": [v[1] for _, v in crow],
        "revenue_cents": [v[2] for _, v in crow],
        "assisted_sessions": [v[3] for _, v in crow],
    }
    for name, table in (("user_daily", ud), ("campaign_daily", cd)):
        types = {k: (pa.date32() if k == "day" else pa.string() if k in STRINGS else pa.int64()) for k in table}
        os.makedirs(os.path.join(out, name), exist_ok=True)
        arrays = {k: pa.array(v, type=types.get(k)) for k, v in table.items()}
        pq.write_table(pa.table(arrays), os.path.join(out, name, "part-0.parquet"))


if __name__ == "__main__":
    main()
