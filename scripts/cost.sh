#!/bin/bash
# Deterministic cost of one binary on the dev set (single thread, cachegrind): instructions,
# branch mispredictions, L1 data misses and misses of a 1 MB second level (a Cascade Lake L2),
# combined into a rough cycle estimate. Independent of machine load. Usage: cost.sh BIN...
set -u
export VALGRIND_LIB=~/ss/vg/root/usr/libexec/valgrind MIMALLOC_PURGE_DELAY=-1
for bin in "$@"; do
  rm -rf ~/ss/cost_out
  out=$(NCPU=1 ~/ss/vg/root/usr/bin/valgrind --tool=cachegrind --cache-sim=yes --branch-sim=yes \
        --D1=32768,8,64 --LL=1048576,16,64 --cachegrind-out-file=$HOME/ss/cost.$(basename "$bin") \
        "$bin" ${COST_DATA:-~/ss/dev} ~/ss/cost_out 2>&1)
  ir=$(echo "$out" | grep "I *refs:" | awk '{print $4}' | tr -d ,)
  bm=$(echo "$out" | grep "Mispredicts:" | awk '{print $3}' | tr -d ,)
  d1=$(echo "$out" | grep "D1  misses:" | awk '{print $4}' | tr -d ,)
  l2=$(echo "$out" | grep "LLd misses:" | awk '{print $4}' | tr -d ,)
  awk -v n="$(basename "$bin")" -v ir="$ir" -v bm="$bm" -v d1="$d1" -v l2="$l2" 'BEGIN {
    cost = ir * 0.4 + bm * 18 + (d1 - l2) * 10 + l2 * 40
    printf "%s: Ir %.0fM  mispredict %.2fM  L1miss %.2fM  L2miss %.2fM  => est %.0fM cycles\n", n, ir/1e6, bm/1e6, d1/1e6, l2/1e6, cost/1e6 }'
done
