# rust-hotpath-ipc — build, test, and demo targets.
# Note: the demo needs Linux (Iceoryx2 shared memory). Real-time scheduling and
# reliable CPU pinning need root / CAP_SYS_NICE.

.PHONY: build test demo bench docker-build docker-run clean fmt clippy

build:
	cargo build --release --examples

test:
	cargo test

# Two-process demo: subscriber in the background, publisher in the foreground.
# Pins publisher, subscriber, and the subscriber's reporter thread to three
# separate cores; override with PUB_CORE / SUB_CORE / REPORTER_CORE. The reporter
# MUST be on a different core than the subscriber: the receive loop takes
# SCHED_FIFO and busy-spins, so a reporter sharing its core would never run.
# SCHED_FIFO needs privilege — run `sudo make demo` (or grant CAP_SYS_NICE).
PUB_CORE ?= 2
SUB_CORE ?= 3
REPORTER_CORE ?= 0
demo: build
	@echo "starting subscriber on core $(SUB_CORE) (reporter on core $(REPORTER_CORE))..."
	CPU_CORE=$(SUB_CORE) REPORTER_CORE=$(REPORTER_CORE) ./target/release/examples/subscriber & \
	SUB_PID=$$!; \
	sleep 3; \
	echo "starting publisher on core $(PUB_CORE)..."; \
	CPU_CORE=$(PUB_CORE) ./target/release/examples/publisher; \
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
