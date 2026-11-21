#!/bin/bash
# N-version check: the independent Python reference and the Rust pipeline must agree exactly on
# fresh generator seeds. Usage: nversion.sh EVENTS SEED...   (env: GEN, PY, BIN, WORK)
set -u
EVENTS=$1; shift
GEN=${GEN:-~/ss/gen/target/release/generate}; PY=${PY:-~/ss/venv/bin/python}
BIN=${BIN:-~/ss/target/release/pipeline}; WORK=${WORK:-~/ss/nv}
HERE=$(cd "$(dirname "$0")/.." && pwd)
fail=0
for seed in "$@"; do
  d=$WORK/s$seed; rm -rf "$d"; mkdir -p "$d"
  $GEN --seed "$seed" --events "$EVENTS" --clicks $((EVENTS / 4)) --output "$d/data" >/dev/null 2>&1
  $PY "$HERE/reference/reference.py" "$d/data" "$d/ref" || { echo "seed $seed: reference failed"; fail=1; continue; }
  chunk=$(( (RANDOM % 50000) + 200 ))
  SS_CHUNK=$chunk NCPU=$(( (RANDOM % 8) + 1 )) $BIN "$d/data" "$d/rust" 2>/dev/null || { echo "seed $seed: pipeline failed"; fail=1; continue; }
  res=$($PY "$HERE/scripts/compare.py" "$d/ref" "$d/rust" | tail -1)
  echo "seed $seed events $EVENTS chunk $chunk window $(grep -o '"window_start": "[^"]*' $d/data/manifest.json | cut -d'"' -f4): $res"
  [ "$res" = MATCH ] || fail=1
  [ "$res" = MATCH ] && rm -rf "$d"
done
exit $fail
