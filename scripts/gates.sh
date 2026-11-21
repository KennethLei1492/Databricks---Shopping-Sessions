#!/bin/bash
# Every correctness gate for one pipeline binary. Prints one line per gate and "GATES PASS" or
# "GATES FAIL". Usage: gates.sh BIN [SEED...]   (env: BASE = a trusted binary to compare at scale)
set -u
BIN=$1; shift
SEEDS=${*:-"$((RANDOM % 900 + 300)) $((RANDOM % 900 + 300)) 15"}
HERE=$(cd "$(dirname "$0")/.." && pwd)
PY=${PY:-~/ss/venv/bin/python}
export MIMALLOC_PURGE_DELAY=-1
fail=0
check() { echo "$1: $2"; [ "$2" = "$3" ] || fail=1; }

check fixtures "$($PY "$HERE/scripts/run_fixtures.py" "$HERE/fixtures" -- "$BIN" 2>&1 | tail -1)" "36/36 fixtures passed"
check fixtures-pread "$(SS_MMAP=0 $PY "$HERE/scripts/run_fixtures.py" "$HERE/fixtures" -- "$BIN" 2>&1 | tail -1)" "36/36 fixtures passed"
for c in 262144 333 70; do
  rm -rf ~/ss/g_dev
  SS_CHUNK=$c "$BIN" ~/ss/dev ~/ss/g_dev 2>/dev/null
  check "dev chunk=$c" "$($PY "$HERE/scripts/compare.py" ~/ss/g_dev "$HERE/vendor/dev-answer/dev-answer" | tail -1)" MATCH
done
rm -rf ~/ss/g_dev
SS_MMAP=0 SS_NP=64 SS_NQ=64 NCPU=3 "$BIN" ~/ss/dev ~/ss/g_dev 2>/dev/null
check "dev pread np=nq=64 ncpu=3" "$($PY "$HERE/scripts/compare.py" ~/ss/g_dev "$HERE/vendor/dev-answer/dev-answer" | tail -1)" MATCH
out=$(BIN=$BIN WORK=~/ss/g_nv bash "$HERE/scripts/nversion.sh" 300000 $SEEDS 2>&1)
echo "$out"
[ "$(echo "$out" | grep -c ': MATCH$')" = "$(echo $SEEDS | wc -w)" ] || fail=1
# At scale: identical tables to a trusted build (SCALE defaults to the 5M-event set, which fits
# in memory next to other work on this machine; use SCALE=~/ss/big20 when it is quiet).
if [ -n "${BASE:-}" ]; then
  SCALE=${SCALE:-~/ss/big5}
  tag=$(basename "$SCALE")
  [ -d ~/ss/g_base_$tag ] || NCPU=8 "$BASE" "$SCALE" ~/ss/g_base_$tag 2>/dev/null
  rm -rf ~/ss/g_new_$tag
  NCPU=8 "$BIN" "$SCALE" ~/ss/g_new_$tag 2>/dev/null
  check "$tag vs base" "$($PY "$HERE/scripts/compare.py" ~/ss/g_base_$tag ~/ss/g_new_$tag | tail -1)" MATCH
fi
[ $fail = 0 ] && echo "GATES PASS" || echo "GATES FAIL"
exit $fail
