# Shopping Sessions

A native Rust pipeline for the Shopping Sessions track. It cleans the event, click and link CSVs,
joins each person's IDs, sessionizes, credits and assists campaigns, and writes
`/output/user_daily/` and `/output/campaign_daily/` as Parquet. The rules are in [rules.md](rules.md);
the design and the verification plan are in [DESIGN.md](DESIGN.md).

## Layout

| Path | What |
| --- | --- |
| `src/` | The pipeline: `main.rs` (stages), `csv.rs` (dialect, chunked `pread`), `ts.rs` (timestamps), `key.rs` (exact packed string keys), `out.rs` (Parquet) |
| `Dockerfile` | Builds the binary with Rust 1.100 and runs it on `debian:bookworm-slim` as uid 1000 |
| `reference/reference.py` | A deliberately simple, independent Python implementation, used only to verify |
| `fixtures/` | The 36 official edge-case fixtures |
| `scripts/` | The official fixture runner and comparer, plus `nversion.sh` (reference vs pipeline on fresh seeds) |

## Verify

```sh
cargo build --release
python3 scripts/run_fixtures.py fixtures -- target/release/pipeline      # 36/36
target/release/pipeline dev out && python3 scripts/compare.py out dev-answer   # MATCH
scripts/nversion.sh 300000 2 3 4                                          # reference == pipeline
```

`SS_CHUNK=<bytes>` forces small read chunks to stress chunk boundaries; `NCPU` sets the thread count.
