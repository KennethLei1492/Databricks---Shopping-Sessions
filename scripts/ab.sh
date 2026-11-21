#!/bin/bash
# Interleaved A/B timing on a noisy machine: alternates two binaries N times and prints each run's
# wall time and minor page faults, then the minimum wall per binary (the least-disturbed run).
# Usage: ab.sh DATA N BIN_A BIN_B
set -u
DATA=$1; N=$2; A=$3; B=$4
OUT=${OUT:-$HOME/ss/ab_out}
declare -A best
for i in $(seq 1 "$N"); do
  for bin in "$A" "$B"; do
    rm -rf "$OUT"
    line=$( { /usr/bin/time -f "TIME %e %R" env NCPU=${NCPU:-8} "$bin" "$DATA" "$OUT" >/dev/null; } 2>&1 | grep '^TIME')
    wall=$(echo "$line" | awk '{print $2}'); flt=$(echo "$line" | awk '{print $3}')
    echo "$(basename "$bin") wall $wall minflt $flt"
    cur=${best[$bin]:-999999}
    if awk "BEGIN{exit !($wall < $cur)}"; then best[$bin]=$wall; fi
  done
done
for bin in "$A" "$B"; do echo "MIN $(basename "$bin") ${best[$bin]}"; done
