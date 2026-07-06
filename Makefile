# rust-hotpath-ipc — build, test, and demo targets.
# Note: the demo needs Linux (Iceoryx2 shared memory). Real-time scheduling and
# reliable CPU pinning need root / CAP_SYS_NICE.

.PHONY: build test demo pipeline studio bench docker-build docker-run clean fmt clippy

build:
	cargo build --release --bins

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
	@echo "starting bench-subscriber on core $(SUB_CORE) (reporter on core $(REPORTER_CORE))..."
	CPU_CORE=$(SUB_CORE) REPORTER_CORE=$(REPORTER_CORE) ./target/release/bench-subscriber & \
	SUB_PID=$$!; \
	sleep 3; \
	echo "starting bench-publisher on core $(PUB_CORE)..."; \
	CPU_CORE=$(PUB_CORE) ./target/release/bench-publisher; \
	kill $$SUB_PID 2>/dev/null || true

# Full three-stage custom-strategy pipeline with end-to-end latency windows.
# feed -> strategy -> execution, pinned to cores 1/2/3; latency reporters on
# core 0 (a core no hot loop owns). Each stage prints its `LAT` percentile lines:
# strategy owns decision-only + tick-to-order, execution owns tick-to-fill.
# SCHED_FIFO on the stages needs privilege — run `sudo make pipeline`.
FEED_CORE ?= 1
STRAT_CORE ?= 2
EXEC_CORE ?= 3
TICK_US ?= 40
THRESHOLD ?= 0.25
MAX_POSITION ?= 3
pipeline: build
	@rm -rf /dev/shm/iox2* /tmp/iceoryx2 2>/dev/null || true
	@echo "starting execution (core $(EXEC_CORE)), strategy (core $(STRAT_CORE)), feed (core $(FEED_CORE)); reporters on core $(REPORTER_CORE)"
	CPU_CORE=$(EXEC_CORE) REPORTER_CORE=$(REPORTER_CORE) ./target/release/execution & E=$$!; \
	sleep 1; \
	CPU_CORE=$(STRAT_CORE) REPORTER_CORE=$(REPORTER_CORE) THRESHOLD=$(THRESHOLD) MAX_POSITION=$(MAX_POSITION) ./target/release/strategy & S=$$!; \
	sleep 1; \
	CPU_CORE=$(FEED_CORE) TICK_US=$(TICK_US) ./target/release/feed; \
	kill $$S $$E 2>/dev/null || true

# Visual strategy builder: control server + web UI on :8080, driving the real
# pipeline with a live latency panel. `sudo make studio` for RT scheduling.
studio: build
	@echo "open http://localhost:8080  (or tunnel: ssh -L 8080:localhost:8080 ...)"
	./target/release/control-server

bench:
	cargo build --release --bins
	@echo "run 'make demo' (transport benchmark) or 'sudo make pipeline' (custom-strategy latency)"

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
