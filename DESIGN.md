# Shopping Sessions: design and verification plan

The approach borrows three methods from the `prebuilt-ai-technique-models` repo. They are applied to
how the pipeline is built, not run inside the container, because the scorer has no network:

- **Task decomposition**: the work is a validated DAG of sub-tasks. Each node has explicit inputs,
  outputs and a pass criterion, and a node is "done" only when its criterion passes.
- **Answer verification**: only *external* signals count, never self-review. In order of authority:
  1. the 36 hand-written fixtures (`fixtures/`, official harness `scripts/run_fixtures.py`)
  2. the official dev answer (`vendor/dev-answer`, `scripts/compare.py`)
  3. N-version agreement: a deliberately simple, independent Python reference
     (`reference/reference.py`) and the fast Rust pipeline (`src/`) must produce identical tables
     on fresh generator seeds and sizes the organizers never published.
- **Parallelization**: data-parallel stages with no shared mutable state. The DAG's critical path
  decides the stage order (Amdahl: the serial link-graph step is kept small).

## DAG

```
manifest, products, campaigns ─┐
links ──► union-find ──► person name map ─┐
                                          ├─► events: parse+validate (parallel, per chunk)
                                          │       └─► shuffle by hash(event_id) ─► dedup (min ingest_ts)
                                          │               └─► shuffle by hash(person)
                                          ├─► clicks: parse+validate (parallel) ─► shuffle by hash(person)
                                          ▼
                      per person bucket (parallel): sort by ts ─► sessions ─► credit + assists
                                          ▼
                      user_daily rows  +  campaign×day accumulators ─► Parquet
```

| Node | Rules | Pass criterion |
| --- | --- | --- |
| CSV dialect | `\n` only, whole-field quotes, exact field count, header is the first line of each file | fixtures E13, E15, E19, E20, L3 |
| Timestamps | F1/F2/F3, calendar and offset ranges, ASCII digits, windows | fixtures E2, E7, E12, E17, E18, A10 |
| Links | drop empty sides, transitive, named by the byte-wise min | fixtures L1-L4 |
| Events | dedup before validation, late > 24 h | fixtures E4-E6, E10 |
| Sessions | gap > 30 min between distinct ts, day = start date | fixtures E1-E3, E14, E16 |
| Attribution | latest click in [start-7d, start], tie by smallest click_id; assists | fixtures A1-A12 |
| Whole pipeline | everything | dev answer exact; reference == Rust on seeds 2..N |

Exactness: IDs are compared as exact byte strings. Strings up to 15 bytes are packed into a `u128`
whose integer order equals byte-wise order (bytes, zero padding, then length). Longer strings go
through a small global interner.
