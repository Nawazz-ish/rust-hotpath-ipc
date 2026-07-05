# Linux build/run image for rust-hotpath-ipc.
#
# Iceoryx2 uses shared memory (Linux) and pulls in a C/C++ build step, so we
# need clang, cmake, and libclang available at compile time.
FROM rust:1-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
        clang \
        cmake \
        libclang-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

# Build the library, its tests, and the demo examples in release mode.
RUN cargo build --release --examples

# By default, print how to run the two-process demo. Real-time scheduling and
# reliable core pinning need --privileged (or CAP_SYS_NICE) and --ipc=host so
# both processes share the same shared-memory namespace.
CMD echo "rust-hotpath-ipc demo:" && \
    echo "  run the subscriber in one shell: CPU_CORE=3 cargo run --release --example subscriber" && \
    echo "  run the publisher in another:    CPU_CORE=2 cargo run --release --example publisher" && \
    echo "  (start the container with --privileged --ipc=host for RT scheduling + shared memory)"
