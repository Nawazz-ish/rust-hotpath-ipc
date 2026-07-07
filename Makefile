# rust-hotpath-ipc — build, test, and demo targets.
# Note: the demo needs Linux (Iceoryx2 shared memory). Real-time scheduling and
# reliable CPU pinning need root / CAP_SYS_NICE.

.PHONY: build test demo demo-execution demo-latency pipeline studio require-bins bench docker-build docker-run clean fmt clippy

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

# Full custom-strategy pipeline with a real matching engine and end-to-end
# latency windows. exchange -> strategy -> execution, pinned to cores 1/2/3;
# latency reporters on core 0 (a core no hot loop owns). The exchange runs the
# order book + synthetic market and is the market-data source; the strategy
# decides and sends orders; execution accounts the fills. Each stage prints its
# `LAT` percentile lines: strategy owns decision-only + tick-to-order, execution
# owns tick-to-fill. SCHED_FIFO on the strategy needs privilege — run `sudo make
# pipeline`. Set PASSIVE=1 to have the strategy post resting limit orders.
EXCH_CORE ?= 1
STRAT_CORE ?= 2
EXEC_CORE ?= 3
TICK_US ?= 40
THRESHOLD ?= 0.25
MAX_POSITION ?= 3
PASSIVE ?= 0
ORDER_UNITS ?= 1
pipeline: require-bins
	@rm -rf /dev/shm/iox2* /tmp/iceoryx2 2>/dev/null || true
	@echo "starting execution (core $(EXEC_CORE)), strategy (core $(STRAT_CORE)), exchange (core $(EXCH_CORE)); reporters on core $(REPORTER_CORE)"
	CPU_CORE=$(EXEC_CORE) REPORTER_CORE=$(REPORTER_CORE) ./target/release/execution & E=$$!; \
	sleep 1; \
	CPU_CORE=$(STRAT_CORE) REPORTER_CORE=$(REPORTER_CORE) THRESHOLD=$(THRESHOLD) MAX_POSITION=$(MAX_POSITION) PASSIVE=$(PASSIVE) ORDER_UNITS=$(ORDER_UNITS) ./target/release/strategy & S=$$!; \
	sleep 1; \
	CPU_CORE=$(EXCH_CORE) TICK_US=$(TICK_US) ./target/release/exchange; \
	kill $$S $$E 2>/dev/null || true

# --- Focused presentation targets (one command, one thing proven) ---
# Both run the same exchange -> strategy -> execution pipeline as `pipeline`;
# they only differ in the config and the banner telling you what to watch for.
# Run under sudo for SCHED_FIFO. Let it run ~15s, then Ctrl-C and read the output.
#
# IMPORTANT: these run targets do NOT depend on `build`. When a Rust toolchain is
# installed per-user (rustup in $HOME/.cargo), `cargo` is not on root's PATH, so a
# `cargo build` invoked under `sudo` fails with "cargo: No such file or directory".
# So the flow is two steps: `make build` (as your user, once) then `sudo make
# demo-execution`. `require-bins` gives a clear error instead of a confusing one if
# you forget the build step.

# Fail early with a helpful message if the binaries aren't built yet, rather than
# trying to `cargo build` under sudo (which can't find a per-user cargo).
require-bins:
	@for b in exchange strategy execution; do \
	  if [ ! -x target/release/$$b ]; then \
	    echo "error: target/release/$$b not found."; \
	    echo "  Run 'make build' as your normal user first, THEN 'sudo make <demo>'."; \
	    exit 1; \
	  fi; \
	done

# ORDER EXECUTION: multi-unit orders (ORDER_UNITS=5) sweep several book levels, so
# a single order fills in pieces at worsening prices — a real partial fill against
# a real order book. Watch the execution lines: `fill # N ... (partial)` where the
# same order id fills twice at different px, and `pnl=` (mark-to-market P&L).
# COOLDOWN=8 keeps orders flowing; MAX_POSITION is in units so it scales with size.
demo-execution: require-bins
	@rm -rf /dev/shm/iox2* /tmp/iceoryx2 2>/dev/null || true
	@echo "======================================================================"
	@echo " ORDER-EXECUTION DEMO — watch the [exec] lines for PARTIAL FILLS:"
	@echo "   fill # N  SELL  px=..  qty=3.00 .. (partial)   <- same order id,"
	@echo "   fill # N  SELL  px=..  qty=2.00 ..             <- two prices = one"
	@echo "   order swept two levels of the book. pnl= is mark-to-market P&L."
	@echo "   (pos= is the *realized* net; the strategy caps *intended* exposure,"
	@echo "    so realized can lag it across flips — a real reconciliation gap.)"
	@echo "   Ctrl-C after ~15s."
	@echo "======================================================================"
	CPU_CORE=$(EXEC_CORE) REPORTER_CORE=$(REPORTER_CORE) ./target/release/execution & E=$$!; \
	sleep 1; \
	CPU_CORE=$(STRAT_CORE) REPORTER_CORE=$(REPORTER_CORE) THRESHOLD=0.10 MAX_POSITION=50 ORDER_UNITS=5 COOLDOWN=8 ./target/release/strategy & S=$$!; \
	sleep 1; \
	CPU_CORE=$(EXCH_CORE) TICK_US=$(TICK_US) ./target/release/exchange; \
	kill $$S $$E 2>/dev/null || true

# TICK-TO-TRADE LATENCY: the three RDTSC windows. Watch the `LAT` lines:
#   decision-only ~65ns  (the strategy math — my code)
#   tick-to-order ~900ns (decision + one shared-memory hop)
#   tick-to-fill  ~80us  (full round-trip to the matcher; the match is 450ns, the
#                         rest is cross-core cache-line visibility under sparse flow)
demo-latency: require-bins
	@rm -rf /dev/shm/iox2* /tmp/iceoryx2 2>/dev/null || true
	@echo "======================================================================"
	@echo " TICK-TO-TRADE LATENCY DEMO — watch the three LAT windows:"
	@echo "   LAT decision-only  ~65 ns   (strategy math, off the hot path)"
	@echo "   LAT tick-to-order  ~900 ns  (decision + one iceoryx2 hop)"
	@echo "   LAT tick-to-fill   ~80 us   (round-trip to the real matcher)"
	@echo "   Ctrl-C after ~15s; they also print on shutdown."
	@echo "======================================================================"
	CPU_CORE=$(EXEC_CORE) REPORTER_CORE=$(REPORTER_CORE) ./target/release/execution & E=$$!; \
	sleep 1; \
	CPU_CORE=$(STRAT_CORE) REPORTER_CORE=$(REPORTER_CORE) THRESHOLD=0.12 MAX_POSITION=3 ./target/release/strategy & S=$$!; \
	sleep 1; \
	CPU_CORE=$(EXCH_CORE) TICK_US=$(TICK_US) ./target/release/exchange; \
	kill $$S $$E 2>/dev/null || true

# Visual strategy builder: control server + web UI on :8080, driving the real
# pipeline with a live latency panel. `sudo make studio` for RT scheduling.
# The UI is a plain static file — nothing to compile. To view it from a laptop
# when the server is remote, forward the port over SSH (no public port needed).
studio: require-bins
	@echo "======================================================================"
	@echo " Strategy builder serving on http://localhost:8080"
	@echo ""
	@echo " Remote box? From YOUR laptop, tunnel the port, then open the URL:"
	@echo "   ssh -i <key.pem> -L 8080:localhost:8080 <user>@<host>"
	@echo "   # then browse to  http://localhost:8080"
	@echo "======================================================================"
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
