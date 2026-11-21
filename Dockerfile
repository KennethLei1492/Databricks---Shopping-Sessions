# Shopping Sessions: a native Rust pipeline. Multi-stage: compile, then a slim runtime image.
FROM rust:1.100-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# Built for the scorer's CPU (Cascade Lake: AVX-512, and its scheduling model). The explicit
# --target keeps RUSTFLAGS off build scripts, which run on the build machine and may not have
# AVX-512. Checked locally under Intel SDE's Cascade Lake emulation (scripts/sde_check.sh).
ENV RUSTFLAGS="-C target-cpu=cascadelake"
RUN cargo build --release --locked --target x86_64-unknown-linux-gnu \
    && strip target/x86_64-unknown-linux-gnu/release/pipeline

FROM debian:bookworm-slim
COPY --from=build /src/target/x86_64-unknown-linux-gnu/release/pipeline /app/pipeline
# The scorer runs as uid 1000 with a read-only root; the binary needs no temp files.
USER 1000:1000
# The run lasts about a second: returning freed memory to the kernel only costs page faults.
ENV DATA_DIR=/data OUTPUT_DIR=/output MIMALLOC_PURGE_DELAY=-1
CMD ["/app/pipeline"]
