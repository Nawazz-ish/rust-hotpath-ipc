# Linux build/run image for rust-hotpath-ipc.
#
# Iceoryx2 uses shared memory (Linux) and pulls in a C/C++ build step, so we
# need clang, cmake, and libclang available at compile time.
FROM rust:1.93-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
        clang \
        cmake \
        libclang-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

# Build the library and all process binaries in release mode.
RUN cargo build --release --bins

# By default, print how to run the two-process transport benchmark. Real-time
# scheduling and reliable core pinning need --privileged (or CAP_SYS_NICE) and
# --ipc=host so both processes share the same shared-memory namespace.
CMD echo "rust-hotpath-ipc demo:" && \
    echo "  run the subscriber in one shell: CPU_CORE=3 cargo run --release --bin bench-subscriber" && \
    echo "  run the publisher in another:    CPU_CORE=2 cargo run --release --bin bench-publisher" && \
    echo "  or the full pipeline:            sudo make pipeline" && \
    echo "  (start the container with --privileged --ipc=host for RT scheduling + shared memory)"
