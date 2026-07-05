# rust-hotpath-ipc — build, test, and demo targets.
# Note: the demo needs Linux (Iceoryx2 shared memory). Real-time scheduling and
# reliable CPU pinning need root / CAP_SYS_NICE.

.PHONY: build test demo bench docker-build docker-run clean fmt clippy

build:
	cargo build --release --examples

test:
	cargo test

# Two-process demo: subscriber in the background, publisher in the foreground.
# Pins them to separate cores; override with PUB_CORE / SUB_CORE.
PUB_CORE ?= 2
SUB_CORE ?= 3
demo: build
	@echo "starting subscriber on core $(SUB_CORE)..."
	CPU_CORE=$(SUB_CORE) cargo run --release --example subscriber & \
	SUB_PID=$$!; \
	sleep 1; \
	echo "starting publisher on core $(PUB_CORE)..."; \
	CPU_CORE=$(PUB_CORE) cargo run --release --example publisher; \
	kill $$SUB_PID 2>/dev/null || true

bench:
	cargo build --release --examples
	@echo "run 'make demo' and read the subscriber's latency percentile table"

docker-build:
	docker build -t rust-hotpath-ipc .

# --privileged + --ipc=host so RT scheduling works and both demo processes
# share the same shared-memory namespace.
docker-run:
	docker run --rm -it --privileged --ipc=host rust-hotpath-ipc

fmt:
	cargo fmt

clippy:
	cargo clippy --all-targets

clean:
	cargo clean
