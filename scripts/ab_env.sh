#!/bin/bash
# Interleaved timing of one binary under several SS_CHUNK values (noisy machines: compare minima).
# Usage: ab_env.sh DATA N BIN CHUNK...
set -u
DATA=$1; N=$2; BIN=$3; shift 3
OUT=${OUT:-$HOME/ss/ab_env_out}
declare -A best
for i in $(seq 1 "$N"); do
  for c in "$@"; do
    rm -rf "$OUT"
    line=$( { /usr/bin/time -f "TIME %e" env SS_CHUNK="$c" NCPU=${NCPU:-8} "$BIN" "$DATA" "$OUT" >/dev/null; } 2>&1 | grep '^TIME')
    wall=$(echo "$line" | awk '{print $2}')
    echo "chunk $c wall $wall"
    cur=${best[$c]:-999999}
    if awk "BEGIN{exit !($wall < $cur)}"; then best[$c]=$wall; fi
  done
done
for c in "$@"; do echo "MIN chunk $c ${best[$c]}"; done
