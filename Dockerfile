# Shopping Sessions: a native Rust pipeline. Multi-stage: compile, then a slim runtime image.
FROM rust:1.100-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# x86-64-v3 (AVX2/BMI2) runs on every Cascade Lake-or-newer scorer and on ordinary dev machines.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN cargo build --release --locked && strip target/release/pipeline

FROM debian:bookworm-slim
COPY --from=build /src/target/release/pipeline /app/pipeline
# The scorer runs as uid 1000 with a read-only root; the binary needs no temp files.
USER 1000:1000
# The run lasts about a second: returning freed memory to the kernel only costs page faults.
ENV DATA_DIR=/data OUTPUT_DIR=/output MIMALLOC_PURGE_DELAY=-1
CMD ["/app/pipeline"]
