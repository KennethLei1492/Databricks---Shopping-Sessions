#!/bin/bash
# Full-scale A/B on one machine: each binary runs on the dev set replicated R times (SS_REPLICATE,
# about 21M events for R=20, the practice set's size), interleaved, N rounds. Prints each
# binary's best and median total and per-phase median walls. Usage: scale_bench.sh DEV N BIN...
# The binaries must be built with `--features scale`.
set -u
DEV=$1; N=$2; shift 2
R=${R:-20}
OUT=${OUT:-$HOME/ss/scale_out}
LOG=$(mktemp -d)
for i in $(seq 1 "$N"); do
  for bin in "$@"; do
    name=$(basename "$bin")
    rm -rf "$OUT"
    SS_REPLICATE=$R MIMALLOC_PURGE_DELAY=-1 NCPU=${NCPU:-8} /usr/bin/time -o "$LOG/t" -f "%e" "$bin" "$DEV" "$OUT" > "$LOG/r" 2>&1
    cat "$LOG/t" >> "$LOG/$name.t"
    grep -o "phase [a-z-]*: [0-9]* tasks, wall [0-9.]*" "$LOG/r" | awk '{print $2, $6}' | tr -d ':' >> "$LOG/$name.p"
  done
done
for bin in "$@"; do
  name=$(basename "$bin")
  echo "$name: best $(sort -n "$LOG/$name.t" | head -1) s  median $(sort -n "$LOG/$name.t" | sed -n "$(( (N + 1) / 2 ))p") s  phases(median): $(awk '{v[$1] = v[$1] " " $2} END {for (k in v) {n = split(v[k], a, " "); asort(a); printf "%s %.3f  ", k, a[int((n + 1) / 2)]}}' "$LOG/$name.p")"
done
rm -rf "$LOG"
