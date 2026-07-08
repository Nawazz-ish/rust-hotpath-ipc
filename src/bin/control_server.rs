//! Control server for the strategy console.
//!
//! This is the *control plane* — it is deliberately NOT on the hot path. It:
//!   1. serves the single-page strategy console (`webui/index.html`),
//!   2. accepts a set of strategy parameters (signal weights, threshold, position
//!      limit, market volatility) on `POST /run`,
//!   3. launches the exchange -> strategy -> execution pipeline as three processes
//!      with those knobs applied,
//!   4. streams their combined output back to the browser as Server-Sent Events
//!      so you watch orders, fills and mark-to-market P&L live, and
//!   5. tears the pipeline down cleanly on `POST /stop`.
//!
//! The point is that the knobs you set in the browser actually configure and run
//! the low-latency engine — no separate simulation.
//!
//! (The separate graph -> bytecode compiler lives in `src/compiler.rs` and is
//! exercised from the CLI via the `disasm` binary; it is not driven from here.)
//!
//! Run:  cargo run --release --bin control-server
//! Then open http://localhost:8080

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;

use tiny_http::{Header, Method, Response, Server};

const PORT: u16 = 8080;

/// A studio run is finite: the strategy stops after this many orders (and the UI
/// tears the pipeline down when it sees the strategy's summary line). Keeps a live
/// demo bounded and deterministic instead of streaming forever.
const STUDIO_ORDERS: u64 = 300;

fn main() {
    let bind = format!("0.0.0.0:{PORT}");
    let server = Server::http(&bind).expect("failed to bind control server");
    println!("strategy-builder control server on http://localhost:{PORT}");
    println!("open that URL in a browser");

    // A single active pipeline at a time; its live output lines fan out to any
    // connected SSE clients.
    let hub: Arc<Mutex<Hub>> = Arc::new(Mutex::new(Hub::default()));

    for request in server.incoming_requests() {
        let hub = hub.clone();
        // Each request handled on its own thread so an SSE stream doesn't block
        // new requests.
        thread::spawn(move || handle(request, hub));
    }
}

/// Fan-out hub: the running pipeline pushes lines here; SSE clients subscribe.
#[derive(Default)]
struct Hub {
    running: Option<RunningPipeline>,
    subscribers: Vec<Sender<String>>,
}

struct RunningPipeline {
    children: Vec<Child>,
}

impl RunningPipeline {
    fn kill(&mut self) {
        // Kill the children we spawned...
        for c in &mut self.children {
            let _ = c.kill();
            let _ = c.wait();
        }
        // ...then a belt-and-braces sweep for any stage that outlived its parent
        // handle (e.g. a re-niced SCHED_FIFO strategy), and clear the shared-memory
        // segments so the next Run starts clean. Without this a "Stop" can leave a
        // stage running and the UI keeps streaming orders.
        let _ = std::process::Command::new("sh")
            .arg("-c")
            .arg("pkill -9 -f 'target/release/(exchange|strategy|execution)' 2>/dev/null; rm -rf /dev/shm/iox2* /tmp/iceoryx2 2>/dev/null")
            .status();
    }
}

fn handle(request: tiny_http::Request, hub: Arc<Mutex<Hub>>) {
    let method = request.method().clone();
    let url = request.url().to_string();
    let path = url.split('?').next().unwrap_or("/");

    match (&method, path) {
        (Method::Get, "/") | (Method::Get, "/index.html") => serve_index(request),
        (Method::Get, "/events") => serve_events(request, hub),
        (Method::Post, "/run") => run_pipeline(request, hub),
        (Method::Post, "/stop") => stop_pipeline(request, hub),
        _ => {
            let _ = request.respond(Response::from_string("not found").with_status_code(404));
        }
    }
}

fn serve_index(request: tiny_http::Request) {
    // Try a few locations so it works whether run from the repo root or elsewhere.
    let candidates = [
        "webui/index.html",
        "./webui/index.html",
        concat!(env!("CARGO_MANIFEST_DIR"), "/webui/index.html"),
    ];
    let mut body = None;
    for c in candidates {
        if let Ok(s) = std::fs::read_to_string(c) {
            body = Some(s);
            break;
        }
    }
    let body = body.unwrap_or_else(|| "<h1>webui/index.html not found</h1>".to_string());
    let header =
        Header::from_bytes(&b"Content-Type"[..], &b"text/html; charset=utf-8"[..]).unwrap();
    let _ = request.respond(Response::from_string(body).with_header(header));
}

/// SSE endpoint: register a subscriber and stream lines until the client leaves.
fn serve_events(request: tiny_http::Request, hub: Arc<Mutex<Hub>>) {
    let (tx, rx): (Sender<String>, Receiver<String>) = mpsc::channel();
    hub.lock().unwrap().subscribers.push(tx);

    // We must write the SSE stream manually to keep the connection open.
    let mut writer = request.into_writer();
    let head = "HTTP/1.1 200 OK\r\n\
                Content-Type: text/event-stream\r\n\
                Cache-Control: no-cache\r\n\
                Connection: keep-alive\r\n\
                Access-Control-Allow-Origin: *\r\n\r\n";
    if writer.write_all(head.as_bytes()).is_err() {
        return;
    }
    let _ = writer.flush();

    // Relay lines as they arrive. If the socket write fails, the client is gone.
    for line in rx {
        let msg = format!("data: {}\n\n", line.replace('\n', " "));
        if writer.write_all(msg.as_bytes()).is_err() {
            break;
        }
        let _ = writer.flush();
    }
}

/// Read the JSON body, extract the strategy knobs, (re)launch the pipeline.
fn run_pipeline(mut request: tiny_http::Request, hub: Arc<Mutex<Hub>>) {
    let mut body = String::new();
    let _ = request.as_reader().read_to_string(&mut body);

    let params = StrategyParams::from_params_json(&body);
    log_line(
        &hub,
        format!("[control] launching pipeline: {}", params.describe()),
    );

    // Stop any prior run first.
    {
        let mut h = hub.lock().unwrap();
        if let Some(mut p) = h.running.take() {
            p.kill();
        }
    }

    match launch(&params, hub.clone()) {
        Ok(pipeline) => {
            hub.lock().unwrap().running = Some(pipeline);
            respond_json(request, r#"{"ok":true}"#);
        }
        Err(e) => {
            log_line(&hub, format!("[control] launch failed: {e}"));
            respond_json(request, &format!(r#"{{"ok":false,"error":"{e}"}}"#));
        }
    }
}

fn stop_pipeline(request: tiny_http::Request, hub: Arc<Mutex<Hub>>) {
    let mut h = hub.lock().unwrap();
    if let Some(mut p) = h.running.take() {
        p.kill();
    }
    drop(h);
    log_line(&hub, "[control] pipeline stopped".to_string());
    respond_json(request, r#"{"ok":true}"#);
}

/// The knobs the console sets. The browser sends a small JSON `params` object;
/// here we reduce it to the parameters the composite strategy engine understands
/// (signal weights, decision threshold, position cap, market volatility).
struct StrategyParams {
    weight_trend: f64,
    weight_momentum: f64,
    weight_reversion: f64,
    threshold: f64,
    max_position: i64,
    vol: f64,
    tick_us: u64,
    // When true, run the boolean AND/OR *gate* graph (examples/gates.json) via the
    // bytecode VM instead of the composite blend. The sliders above only affect the
    // composite; the gate strategy is a fixed rule set compiled from the graph.
    gates: bool,
}

impl Default for StrategyParams {
    fn default() -> Self {
        Self {
            weight_trend: 0.5,
            weight_momentum: 0.3,
            weight_reversion: 0.2,
            threshold: 0.25,
            max_position: 3,
            vol: 8.0,
            tick_us: 40,
            gates: false,
        }
    }
}

impl StrategyParams {
    /// Extract parameters from the console's JSON. A light, dependency-free scan:
    /// the browser sends a flat `params` object, so we pull the numbers we need
    /// out of it directly.
    fn from_params_json(body: &str) -> Self {
        let mut p = Self::default();
        let get = |key: &str| -> Option<f64> {
            let pat = format!("\"{key}\"");
            let idx = body.find(&pat)?;
            let rest = &body[idx + pat.len()..];
            let colon = rest.find(':')?;
            let after = &rest[colon + 1..];
            let end = after.find([',', '}']).unwrap_or(after.len());
            after[..end].trim().trim_matches('"').parse::<f64>().ok()
        };
        if let Some(v) = get("weight_trend") {
            p.weight_trend = v;
        }
        if let Some(v) = get("weight_momentum") {
            p.weight_momentum = v;
        }
        if let Some(v) = get("weight_reversion") {
            p.weight_reversion = v;
        }
        if let Some(v) = get("threshold") {
            p.threshold = v;
        }
        if let Some(v) = get("max_position") {
            p.max_position = v as i64;
        }
        if let Some(v) = get("vol") {
            p.vol = v;
        }
        if let Some(v) = get("tick_us") {
            p.tick_us = v as u64;
        }
        // Gate mode: the browser sends "strategy":"gates" to run the AND/OR graph.
        p.gates = body.contains("\"gates\"");
        p
    }

    fn describe(&self) -> String {
        if self.gates {
            format!(
                "gate strategy (AND/OR bytecode) maxpos={} vol={:.1} tick={}us",
                self.max_position, self.vol, self.tick_us
            )
        } else {
            format!(
                "composite trend={:.2} mom={:.2} rev={:.2} thr={:.2} maxpos={} vol={:.1} tick={}us",
                self.weight_trend,
                self.weight_momentum,
                self.weight_reversion,
                self.threshold,
                self.max_position,
                self.vol,
                self.tick_us
            )
        }
    }
}

/// Launch exchange + strategy + execution, each with stdout piped into the hub.
fn launch(p: &StrategyParams, hub: Arc<Mutex<Hub>>) -> Result<RunningPipeline, String> {
    // Clean any stale iceoryx2 state so a fresh run starts clean.
    let _ = std::process::Command::new("sh")
        .arg("-c")
        .arg("rm -rf /dev/shm/iox2* /tmp/iceoryx2 2>/dev/null; pkill -9 -f 'target/release/(exchange|strategy|execution)' 2>/dev/null")
        .status();

    let exe = |name: &str| -> String {
        // Resolve the release binary whether the server was started from the repo
        // root or elsewhere (e.g. a launcher with a different cwd). The pipeline
        // stages are first-class binaries, so they live directly in
        // target/release/ (not target/release/examples/).
        let manifest = env!("CARGO_MANIFEST_DIR");
        let candidates = [
            format!("target/release/{name}"),
            format!("./target/release/{name}"),
            format!("{manifest}/target/release/{name}"),
        ];
        candidates
            .into_iter()
            .find(|c| std::path::Path::new(c).exists())
            .unwrap_or_else(|| format!("{manifest}/target/release/{name}"))
    };

    // Consumers first so they're attached before the exchange starts producing:
    // execution (core 3), then strategy (core 2), then the exchange (core 1),
    // which matches orders and is the market-data source.
    let execution = spawn_stage(
        &exe("execution"),
        &[
            ("CPU_CORE", "3".into()),
            // Self-exit after the strategy's bounded order count is filled, so the
            // whole finite run winds down on its own.
            ("MAX_FILLS", format!("{STUDIO_ORDERS}")),
        ],
        "exec",
        &hub,
    )?;
    thread::sleep(std::time::Duration::from_millis(400));

    let mut strat_env: Vec<(&str, String)> = vec![
        ("CPU_CORE", "2".into()),
        ("THRESHOLD", format!("{}", p.threshold)),
        ("WEIGHT_TREND", format!("{}", p.weight_trend)),
        ("WEIGHT_MOMENTUM", format!("{}", p.weight_momentum)),
        ("WEIGHT_REVERSION", format!("{}", p.weight_reversion)),
        ("MAX_POSITION", format!("{}", p.max_position)),
        // A studio run is finite — stop after STUDIO_ORDERS orders.
        ("MAX_ORDERS", format!("{STUDIO_ORDERS}")),
    ];
    // Gate mode: point the strategy at the AND/OR graph so it compiles it to
    // bytecode and runs THAT (via STRATEGY_JSON) instead of the composite blend.
    if p.gates {
        let manifest = env!("CARGO_MANIFEST_DIR");
        let candidates = [
            "examples/gates.json".to_string(),
            "./examples/gates.json".to_string(),
            format!("{manifest}/examples/gates.json"),
        ];
        let graph = candidates
            .into_iter()
            .find(|c| std::path::Path::new(c).exists())
            .unwrap_or_else(|| format!("{manifest}/examples/gates.json"));
        strat_env.push(("STRATEGY_JSON", graph));
    }
    let strategy = spawn_stage(&exe("strategy"), &strat_env, "strat", &hub)?;
    thread::sleep(std::time::Duration::from_millis(400));

    let exchange = spawn_stage(
        &exe("exchange"),
        &[
            ("CPU_CORE", "1".into()),
            ("VOL", format!("{}", p.vol)),
            ("TICK_US", format!("{}", p.tick_us)),
            ("SEED", "42".into()),
        ],
        "exch",
        &hub,
    )?;

    Ok(RunningPipeline {
        children: vec![execution, strategy, exchange],
    })
}

/// Spawn one stage, tagging and forwarding its stdout lines to the hub.
fn spawn_stage(
    path: &str,
    envs: &[(&str, String)],
    tag: &'static str,
    hub: &Arc<Mutex<Hub>>,
) -> Result<Child, String> {
    let mut cmd = Command::new(path);
    for (k, v) in envs {
        cmd.env(k, v);
    }
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd.spawn().map_err(|e| format!("spawn {path}: {e}"))?;

    if let Some(out) = child.stdout.take() {
        let hub = hub.clone();
        thread::spawn(move || {
            let reader = BufReader::new(out);
            for line in reader.lines().map_while(Result::ok) {
                log_line(&hub, format!("[{tag}] {line}"));
            }
        });
    }
    Ok(child)
}

/// Broadcast a line to every SSE subscriber (dropping dead ones).
fn log_line(hub: &Arc<Mutex<Hub>>, line: String) {
    let mut h = hub.lock().unwrap();
    h.subscribers.retain(|s| s.send(line.clone()).is_ok());
    // Also echo to the server's own stdout for debugging.
    println!("{line}");
}

fn respond_json(request: tiny_http::Request, body: &str) {
    let header = Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();
    let _ = request.respond(Response::from_string(body).with_header(header));
}
