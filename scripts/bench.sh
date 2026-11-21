#!/bin/bash
# Interleaved best-of-N wall-clock comparison on a noisy machine. For each binary prints the
# minimum total and the minimum of each phase. Usage: bench.sh DATA N BIN...
set -u
DATA=$1; N=$2; shift 2
export MIMALLOC_PURGE_DELAY=-1
OUT=${OUT:-$HOME/ss/b_out}
LOG=${LOG:-$HOME/ss/b_logs}; mkdir -p "$LOG"; rm -f "$LOG"/*.t "$LOG"/*.p
for i in $(seq 1 "$N"); do
  for bin in "$@"; do
    name=$(basename "$bin")
    rm -rf "$OUT"
    /usr/bin/time -o "$LOG/t" -f "%e" env NCPU=${NCPU:-8} "$bin" "$DATA" "$OUT" > "$LOG/r" 2>&1
    cat "$LOG/t" >> "$LOG/$name.t"
    grep -o "phase [a-z-]*: [0-9]* tasks, wall [0-9.]*" "$LOG/r" | awk '{print $2, $6}' | tr -d ':' >> "$LOG/$name.p"
  done
done
for bin in "$@"; do
  name=$(basename "$bin")
  echo "$name: min $(sort -n "$LOG/$name.t" | head -1) s  median $(sort -n "$LOG/$name.t" | sed -n "$(( (N + 1) / 2 ))p") s   phases(min): $(awk '{ if (!($1 in m) || $2 < m[$1]) m[$1] = $2 } END { for (k in m) printf "%s %.3f  ", k, m[k] }' "$LOG/$name.p")"
done
