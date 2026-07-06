//! Control server for the visual strategy builder.
//!
//! This is the *control plane* — it is deliberately NOT on the hot path. It:
//!   1. serves the single-page strategy builder (`webui/index.html`),
//!   2. accepts a strategy graph (the node/connection spec the builder exports)
//!      on `POST /run`,
//!   3. translates that graph into the `StrategyConfig` knobs the real Rust
//!      strategy engine reads (signal weights, threshold, position limit),
//!   4. launches the feed -> strategy -> execution pipeline as three processes,
//!   5. streams their combined output back to the browser as Server-Sent Events
//!      so you watch orders and mark-to-market P&L live.
//!
//! The point is that the picture you draw in the browser actually configures and
//! runs the low-latency engine — no separate simulation.
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
        for c in &mut self.children {
            let _ = c.kill();
            let _ = c.wait();
        }
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
    let mut writer = match request.into_writer() {
        w => w,
    };
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

    let params = StrategyParams::from_graph_json(&body);
    log_line(
        &hub,
        format!("[control] launching pipeline: {}", params.describe()),
    );

    // Persist the drawn graph so the strategy stage can compile it to bytecode
    // and run THAT (rather than only the parametric fallback). Written to a fixed
    // temp path the strategy reads via STRATEGY_JSON.
    let graph_path = std::env::temp_dir().join("rust_hotpath_strategy.json");
    let has_graph = std::fs::write(&graph_path, &body).is_ok() && body.contains("\"nodes\"");
    if has_graph {
        log_line(
            &hub,
            format!("[control] strategy graph -> {}", graph_path.display()),
        );
    }

    // Stop any prior run first.
    {
        let mut h = hub.lock().unwrap();
        if let Some(mut p) = h.running.take() {
            p.kill();
        }
    }

    let graph_arg = if has_graph {
        Some(graph_path.to_string_lossy().to_string())
    } else {
        None
    };

    match launch(&params, graph_arg.as_deref(), hub.clone()) {
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

/// The knobs we extract from a strategy graph. The visual builder emits a
/// node/connection graph; here we reduce it to the parameters the composite
/// strategy engine understands. A richer mapping would walk the DAG, but for the
/// demo we read the presence/weight of indicator + signal nodes.
struct StrategyParams {
    weight_trend: f64,
    weight_momentum: f64,
    weight_reversion: f64,
    threshold: f64,
    max_position: i64,
    vol: f64,
    tick_us: u64,
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
        }
    }
}

impl StrategyParams {
    /// Extract parameters from the builder's JSON. We do a light, dependency-free
    /// scan: the builder sends a flat `params` object alongside the graph, so we
    /// pull the numbers we need out of it. (Full DAG interpretation lives in the
    /// Rust strategy-studio engine; this control plane only needs the knobs.)
    fn from_graph_json(body: &str) -> Self {
        let mut p = Self::default();
        let get = |key: &str| -> Option<f64> {
            let pat = format!("\"{key}\"");
            let idx = body.find(&pat)?;
            let rest = &body[idx + pat.len()..];
            let colon = rest.find(':')?;
            let after = &rest[colon + 1..];
            let end = after
                .find(|c: char| c == ',' || c == '}')
                .unwrap_or(after.len());
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
        p
    }

    fn describe(&self) -> String {
        format!(
            "trend={:.2} mom={:.2} rev={:.2} thr={:.2} maxpos={} vol={:.1} tick={}us",
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

/// Launch feed + strategy + execution, each with stdout piped into the hub.
fn launch(
    p: &StrategyParams,
    graph_json_path: Option<&str>,
    hub: Arc<Mutex<Hub>>,
) -> Result<RunningPipeline, String> {
    // Clean any stale iceoryx2 state so a fresh run starts clean.
    let _ = std::process::Command::new("sh")
        .arg("-c")
        .arg("rm -rf /dev/shm/iox2* /tmp/iceoryx2 2>/dev/null; pkill -9 -f 'release/examples/(feed|strategy|execution)' 2>/dev/null")
        .status();

    let exe = |name: &str| -> String {
        // Resolve the release example binary whether the server was started from
        // the repo root or elsewhere (e.g. a launcher with a different cwd).
        let manifest = env!("CARGO_MANIFEST_DIR");
        let candidates = [
            format!("target/release/examples/{name}"),
            format!("./target/release/examples/{name}"),
            format!("{manifest}/target/release/examples/{name}"),
        ];
        candidates
            .into_iter()
            .find(|c| std::path::Path::new(c).exists())
            .unwrap_or_else(|| format!("{manifest}/target/release/examples/{name}"))
    };

    // Execution (core 3) first, then strategy (core 2), then feed (core 1).
    let execution = spawn_stage(&exe("execution"), &[("CPU_CORE", "3".into())], "exec", &hub)?;
    thread::sleep(std::time::Duration::from_millis(400));

    let mut strat_env: Vec<(&str, String)> = vec![
        ("CPU_CORE", "2".into()),
        ("THRESHOLD", format!("{}", p.threshold)),
        ("WEIGHT_TREND", format!("{}", p.weight_trend)),
        ("WEIGHT_MOMENTUM", format!("{}", p.weight_momentum)),
        ("WEIGHT_REVERSION", format!("{}", p.weight_reversion)),
        ("MAX_POSITION", format!("{}", p.max_position)),
    ];
    // Hand the drawn graph to the strategy so it compiles + runs the bytecode.
    if let Some(path) = graph_json_path {
        strat_env.push(("STRATEGY_JSON", path.to_string()));
    }
    let strategy = spawn_stage(&exe("strategy"), &strat_env, "strat", &hub)?;
    thread::sleep(std::time::Duration::from_millis(400));

    let feed = spawn_stage(
        &exe("feed"),
        &[
            ("CPU_CORE", "1".into()),
            ("VOL", format!("{}", p.vol)),
            ("TICK_US", format!("{}", p.tick_us)),
            ("SEED", "42".into()),
        ],
        "feed",
        &hub,
    )?;

    Ok(RunningPipeline {
        children: vec![execution, strategy, feed],
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
