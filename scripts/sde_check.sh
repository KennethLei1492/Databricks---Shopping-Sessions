#!/bin/bash
# Builds the pipeline for Cascade Lake (as the Dockerfile does) and runs every correctness gate on
# it under Intel SDE's Cascade Lake emulation, so AVX-512 code is checked on a machine without
# AVX-512. Usage: SDE=/path/to/sde64 sde_check.sh [SEED...]   (env: BASE = trusted binary)
set -eu
HERE=$(cd "$(dirname "$0")/.." && pwd)
SDE=${SDE:-$HOME/ss/sde/sde-external-10.13.1-2026-07-28-lin/sde64}
OUT=${OUT:-$HOME/ss}
(cd "$HERE" && CARGO_TARGET_DIR=$OUT/target_clx RUSTFLAGS="-C target-cpu=cascadelake" \
    cargo build --release --locked --target x86_64-unknown-linux-gnu)
cp "$OUT/target_clx/x86_64-unknown-linux-gnu/release/pipeline" "$OUT/pipeline_clx"
cat > "$OUT/pipeline_clx_sde" <<EOF
#!/bin/bash
exec "$SDE" -clx -- "$OUT/pipeline_clx" "\$@"
EOF
chmod +x "$OUT/pipeline_clx_sde"
bash "$HERE/scripts/gates.sh" "$OUT/pipeline_clx_sde" "$@"
