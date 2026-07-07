//! Compile a strategy graph (a node/connection spec) into a flat bytecode program
//! for [`crate::bytecode::Vm`].
//!
//! The graph JSON looks like:
//! ```json
//! { "canvas": { "nodes": [ {"id","type","properties":{...}}, ... ],
//!               "connections": [ {"from","to"}, ... ] } }
//! ```
//! We parse it (with a small dependency-free scanner — the repo avoids `serde`),
//! then lower it. Because the graph is a DAG, we emit each terminal signal node's
//! input sub-graph in dependency order (a node's inputs are emitted before the
//! node itself), which for a stack machine means operands land on the stack
//! before the operator that consumes them.
//!
//! This is what makes the graph *actually run*: change the wiring and the emitted
//! program changes, rather than the graph only tuning fixed parameters. Exercised
//! from the CLI via the `disasm` binary (see `examples/graph.json`).

use crate::bytecode::Op;
use std::collections::HashMap;

/// A parsed graph node.
#[derive(Debug, Clone)]
pub struct GNode {
    pub id: String,
    pub ntype: String,
    /// Numeric properties (period, lookback, window, value).
    pub nums: HashMap<String, f64>,
    /// String properties (op).
    pub strs: HashMap<String, String>,
}

/// A parsed graph: nodes plus directed `from -> to` connections.
#[derive(Debug, Clone, Default)]
pub struct GraphSpec {
    pub nodes: Vec<GNode>,
    pub connections: Vec<(String, String)>,
}

#[derive(Debug)]
pub enum CompileError {
    Cycle,
    UnknownNode(String),
    UnknownType(String),
    /// A node that produces a single value has more than one input wired — the
    /// graph is ambiguous. (e.g. two indicators into one condition.) Combine them
    /// through a logic gate instead. We reject rather than silently drop inputs.
    TooManyInputs {
        node: String,
        ntype: String,
        got: usize,
    },
}

impl std::fmt::Display for CompileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompileError::Cycle => write!(f, "strategy graph has a cycle"),
            CompileError::UnknownNode(id) => write!(f, "connection references unknown node {id}"),
            CompileError::UnknownType(t) => write!(f, "unknown node type {t}"),
            CompileError::TooManyInputs { node, ntype, got } => write!(
                f,
                "{ntype} node '{node}' has {got} inputs but takes one; combine inputs with a gate"
            ),
        }
    }
}
impl std::error::Error for CompileError {}

// ============================================================================
// Compile: GraphSpec -> Vec<Op>
// ============================================================================

/// Lowering context threaded through the recursive emit.
struct Lowerer<'a> {
    by_id: HashMap<&'a str, &'a GNode>,
    // incoming edges: node id -> list of source node ids, in connection order.
    incoming: HashMap<&'a str, Vec<&'a str>>,
    prog: Vec<Op>,
    next_slot: usize,
    // guards against cycles during the recursive emit.
    on_stack: Vec<&'a str>,
}

pub fn compile(spec: &GraphSpec) -> Result<Vec<Op>, CompileError> {
    let by_id: HashMap<&str, &GNode> = spec.nodes.iter().map(|n| (n.id.as_str(), n)).collect();

    // Validate connections reference known nodes and build the incoming map.
    let mut incoming: HashMap<&str, Vec<&str>> = HashMap::new();
    for (from, to) in &spec.connections {
        if !by_id.contains_key(from.as_str()) {
            return Err(CompileError::UnknownNode(from.clone()));
        }
        if !by_id.contains_key(to.as_str()) {
            return Err(CompileError::UnknownNode(to.clone()));
        }
        incoming.entry(to.as_str()).or_default().push(from.as_str());
    }

    let mut lo = Lowerer {
        by_id,
        incoming,
        prog: Vec::new(),
        next_slot: 0,
        on_stack: Vec::new(),
    };

    // Emit one terminal per signal node, in the graph's declared node order so
    // the output is deterministic. A buy/sell node emits its input sub-tree then
    // the Buy/Sell op.
    for node in &spec.nodes {
        match node.ntype.as_str() {
            "buy_signal" | "sell_signal" => {
                lo.emit_unary_input(&node.id)?;
                lo.prog.push(if node.ntype == "buy_signal" {
                    Op::Buy
                } else {
                    Op::Sell
                });
            }
            _ => {}
        }
    }

    Ok(lo.prog)
}

impl<'a> Lowerer<'a> {
    /// Emit the single input feeding a *unary* consumer (buy/sell/condition),
    /// leaving one value on the stack. More than one input is ambiguous and is a
    /// hard error — we never silently drop a wired input. Zero inputs pushes a
    /// neutral 0 so the consumer simply doesn't fire.
    fn emit_unary_input(&mut self, id: &'a str) -> Result<(), CompileError> {
        let srcs = self.incoming.get(id).cloned().unwrap_or_default();
        if srcs.len() > 1 {
            let node = self.by_id[id];
            return Err(CompileError::TooManyInputs {
                node: id.to_string(),
                ntype: node.ntype.clone(),
                got: srcs.len(),
            });
        }
        if let Some(&src) = srcs.first() {
            self.emit_node(src)?;
        } else {
            self.prog.push(Op::PushConst(0.0));
        }
        Ok(())
    }

    /// Emit `id` so that it leaves exactly one value on the stack.
    fn emit_node(&mut self, id: &'a str) -> Result<(), CompileError> {
        if self.on_stack.contains(&id) {
            return Err(CompileError::Cycle);
        }
        self.on_stack.push(id);
        let node = *self
            .by_id
            .get(id)
            .ok_or_else(|| CompileError::UnknownNode(id.to_string()))?;

        match node.ntype.as_str() {
            // Price source: the value is the current price.
            "data_source" => self.prog.push(Op::PushPrice),

            // Indicators consume the price; they emit PushPrice implicitly by
            // reading it inside the op, so we just push the op (its input is the
            // price, provided by the VM, not the stack).
            "ema" => {
                let period = node.nums.get("period").copied().unwrap_or(12.0) as usize;
                let slot = self.slot();
                self.prog.push(Op::Ema { slot, period });
            }
            "momentum" => {
                let lookback = node.nums.get("lookback").copied().unwrap_or(16.0) as usize;
                let slot = self.slot();
                self.prog.push(Op::Momentum { slot, lookback });
            }
            "reversion" => {
                let window = node.nums.get("window").copied().unwrap_or(64.0) as usize;
                let slot = self.slot();
                self.prog.push(Op::Reversion { slot, window });
            }

            // Condition: emit its input, push the threshold, compare.
            "condition" => {
                self.emit_unary_input(id)?;
                let value = node.nums.get("value").copied().unwrap_or(0.0);
                self.prog.push(Op::PushConst(value));
                let op = node
                    .strs
                    .get("op")
                    .map(String::as_str)
                    .unwrap_or("greater_than");
                self.prog.push(match op {
                    "less_than" => Op::Lt,
                    "greater_equal" => Op::Ge,
                    "less_equal" => Op::Le,
                    "equal" => Op::Eq,
                    _ => Op::Gt,
                });
            }

            // Logic gates: emit all incoming sub-trees, then fold with And/Or.
            "and_gate" | "or_gate" => {
                let srcs = self.incoming.get(id).cloned().unwrap_or_default();
                let fold = if node.ntype == "and_gate" {
                    Op::And
                } else {
                    Op::Or
                };
                if srcs.is_empty() {
                    self.prog.push(Op::PushConst(0.0));
                } else {
                    self.emit_node(srcs[0])?;
                    for &src in &srcs[1..] {
                        self.emit_node(src)?;
                        self.prog.push(fold);
                    }
                }
            }

            // A signal node used as an intermediate input (rare) — emit its input.
            "buy_signal" | "sell_signal" => {
                self.emit_unary_input(id)?;
            }

            other => {
                self.on_stack.pop();
                return Err(CompileError::UnknownType(other.to_string()));
            }
        }

        self.on_stack.pop();
        Ok(())
    }

    fn slot(&mut self) -> usize {
        let s = self.next_slot;
        self.next_slot += 1;
        s
    }
}

// ============================================================================
// A tiny, dependency-free JSON reader for the graph spec
// ============================================================================

/// Parse the builder's graph JSON into a [`GraphSpec`]. Deliberately minimal —
/// it understands only the shape the builder emits, not arbitrary JSON — so the
/// crate needs no serialization dependency.
pub fn parse(json: &str) -> Result<GraphSpec, String> {
    let v = mini_json::parse(json)?;
    let canvas = v.get("canvas").ok_or("missing canvas")?;

    let mut spec = GraphSpec::default();

    if let Some(nodes) = canvas.get("nodes").and_then(|n| n.as_array()) {
        for n in nodes {
            let id = n
                .get("id")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string();
            let ntype = n
                .get("type")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string();
            let mut nums = HashMap::new();
            let mut strs = HashMap::new();
            if let Some(props) = n.get("properties").and_then(|p| p.as_object()) {
                for (k, val) in props {
                    if let Some(f) = val.as_f64() {
                        nums.insert(k.clone(), f);
                    } else if let Some(s) = val.as_str() {
                        strs.insert(k.clone(), s.to_string());
                    }
                }
            }
            spec.nodes.push(GNode {
                id,
                ntype,
                nums,
                strs,
            });
        }
    }

    if let Some(conns) = canvas.get("connections").and_then(|c| c.as_array()) {
        for c in conns {
            let from = c
                .get("from")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string();
            let to = c
                .get("to")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string();
            if !from.is_empty() && !to.is_empty() {
                spec.connections.push((from, to));
            }
        }
    }

    Ok(spec)
}

/// A minimal JSON value + recursive-descent parser. Handles objects, arrays,
/// strings, numbers, booleans, and null — enough for the graph spec.
mod mini_json {
    use std::collections::HashMap;

    #[derive(Debug, Clone)]
    #[allow(dead_code)] // Null/Bool are parsed for completeness; the graph spec doesn't read them.
    pub enum Json {
        Null,
        Bool(bool),
        Num(f64),
        Str(String),
        Arr(Vec<Json>),
        Obj(HashMap<String, Json>),
    }

    impl Json {
        pub fn get(&self, key: &str) -> Option<&Json> {
            match self {
                Json::Obj(m) => m.get(key),
                _ => None,
            }
        }
        pub fn as_array(&self) -> Option<&Vec<Json>> {
            match self {
                Json::Arr(a) => Some(a),
                _ => None,
            }
        }
        pub fn as_object(&self) -> Option<&HashMap<String, Json>> {
            match self {
                Json::Obj(m) => Some(m),
                _ => None,
            }
        }
        pub fn as_str(&self) -> Option<&str> {
            match self {
                Json::Str(s) => Some(s),
                _ => None,
            }
        }
        pub fn as_f64(&self) -> Option<f64> {
            match self {
                Json::Num(n) => Some(*n),
                _ => None,
            }
        }
    }

    pub fn parse(s: &str) -> Result<Json, String> {
        let b = s.as_bytes();
        let mut i = 0;
        let v = parse_value(b, &mut i)?;
        Ok(v)
    }

    fn skip_ws(b: &[u8], i: &mut usize) {
        while *i < b.len() && (b[*i] as char).is_whitespace() {
            *i += 1;
        }
    }

    fn parse_value(b: &[u8], i: &mut usize) -> Result<Json, String> {
        skip_ws(b, i);
        if *i >= b.len() {
            return Err("unexpected end".into());
        }
        match b[*i] {
            b'{' => parse_obj(b, i),
            b'[' => parse_arr(b, i),
            b'"' => parse_str(b, i).map(Json::Str),
            b't' | b'f' => parse_bool(b, i),
            b'n' => {
                *i += 4;
                Ok(Json::Null)
            }
            _ => parse_num(b, i),
        }
    }

    fn parse_obj(b: &[u8], i: &mut usize) -> Result<Json, String> {
        let mut m = HashMap::new();
        *i += 1; // {
        loop {
            skip_ws(b, i);
            if *i < b.len() && b[*i] == b'}' {
                *i += 1;
                break;
            }
            let key = parse_str(b, i)?;
            skip_ws(b, i);
            if *i >= b.len() || b[*i] != b':' {
                return Err("expected :".into());
            }
            *i += 1;
            let val = parse_value(b, i)?;
            m.insert(key, val);
            skip_ws(b, i);
            if *i < b.len() && b[*i] == b',' {
                *i += 1;
            }
        }
        Ok(Json::Obj(m))
    }

    fn parse_arr(b: &[u8], i: &mut usize) -> Result<Json, String> {
        let mut a = Vec::new();
        *i += 1; // [
        loop {
            skip_ws(b, i);
            if *i < b.len() && b[*i] == b']' {
                *i += 1;
                break;
            }
            a.push(parse_value(b, i)?);
            skip_ws(b, i);
            if *i < b.len() && b[*i] == b',' {
                *i += 1;
            }
        }
        Ok(Json::Arr(a))
    }

    fn parse_str(b: &[u8], i: &mut usize) -> Result<String, String> {
        skip_ws(b, i);
        if *i >= b.len() || b[*i] != b'"' {
            return Err("expected string".into());
        }
        *i += 1;
        let mut s = String::new();
        while *i < b.len() && b[*i] != b'"' {
            if b[*i] == b'\\' && *i + 1 < b.len() {
                *i += 1;
                s.push(match b[*i] {
                    b'n' => '\n',
                    b't' => '\t',
                    c => c as char,
                });
            } else {
                s.push(b[*i] as char);
            }
            *i += 1;
        }
        *i += 1; // closing "
        Ok(s)
    }

    fn parse_bool(b: &[u8], i: &mut usize) -> Result<Json, String> {
        if b[*i] == b't' {
            *i += 4;
            Ok(Json::Bool(true))
        } else {
            *i += 5;
            Ok(Json::Bool(false))
        }
    }

    fn parse_num(b: &[u8], i: &mut usize) -> Result<Json, String> {
        let start = *i;
        while *i < b.len()
            && (b[*i].is_ascii_digit()
                || b[*i] == b'-'
                || b[*i] == b'+'
                || b[*i] == b'.'
                || b[*i] == b'e'
                || b[*i] == b'E')
        {
            *i += 1;
        }
        let s = std::str::from_utf8(&b[start..*i]).map_err(|_| "bad number")?;
        s.parse::<f64>()
            .map(Json::Num)
            .map_err(|_| "bad number".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
    { "canvas": {
        "nodes": [
          {"id":"d","type":"data_source","properties":{"symbol":"BTC/USDT"}},
          {"id":"e","type":"ema","properties":{"period":12}},
          {"id":"c","type":"condition","properties":{"op":"greater_than","value":50000}},
          {"id":"b","type":"buy_signal","properties":{}}
        ],
        "connections": [
          {"from":"d","to":"e"},
          {"from":"e","to":"c"},
          {"from":"c","to":"b"}
        ]
    }}"#;

    #[test]
    fn parses_and_compiles_sample() {
        let spec = parse(SAMPLE).unwrap();
        assert_eq!(spec.nodes.len(), 4);
        assert_eq!(spec.connections.len(), 3);
        let prog = compile(&spec).unwrap();
        // ema(price) -> push 50000 -> Gt -> Buy
        assert_eq!(
            prog,
            vec![
                Op::Ema {
                    slot: 0,
                    period: 12
                },
                Op::PushConst(50000.0),
                Op::Gt,
                Op::Buy,
            ]
        );
    }

    #[test]
    fn deterministic() {
        let a = compile(&parse(SAMPLE).unwrap()).unwrap();
        let b = compile(&parse(SAMPLE).unwrap()).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn cycle_is_rejected() {
        // Two conditions feeding each other (each with exactly one input, so it's
        // a genuine cycle, not an ambiguous-input error), reachable from a buy.
        let json = r#"{"canvas":{
            "nodes":[{"id":"a","type":"condition","properties":{"value":1}},
                     {"id":"b","type":"condition","properties":{"value":1}},
                     {"id":"s","type":"buy_signal","properties":{}}],
            "connections":[{"from":"a","to":"b"},{"from":"b","to":"a"},{"from":"b","to":"s"}]
        }}"#;
        let spec = parse(json).unwrap();
        // emit from s -> b -> a -> b: revisits b -> cycle.
        assert!(matches!(compile(&spec), Err(CompileError::Cycle)));
    }

    #[test]
    fn empty_graph_empty_program() {
        let prog = compile(&GraphSpec::default()).unwrap();
        assert!(prog.is_empty());
    }

    #[test]
    fn multi_input_and_gate_compiles_all_inputs() {
        // (momentum > 0.1) AND (reversion < -0.1) -> buy. Both indicators must
        // appear in the program, folded by AND.
        let json = r#"{"canvas":{"nodes":[
            {"id":"d","type":"data_source","properties":{}},
            {"id":"m","type":"momentum","properties":{"lookback":16}},
            {"id":"cm","type":"condition","properties":{"op":"greater_than","value":0.1}},
            {"id":"r","type":"reversion","properties":{"window":64}},
            {"id":"cr","type":"condition","properties":{"op":"less_than","value":-0.1}},
            {"id":"g","type":"and_gate","properties":{}},
            {"id":"b","type":"buy_signal","properties":{}}
        ],"connections":[
            {"from":"d","to":"m"},{"from":"m","to":"cm"},
            {"from":"d","to":"r"},{"from":"r","to":"cr"},
            {"from":"cm","to":"g"},{"from":"cr","to":"g"},
            {"from":"g","to":"b"}
        ]}}"#;
        let prog = compile(&parse(json).unwrap()).unwrap();
        assert_eq!(
            prog,
            vec![
                Op::Momentum {
                    slot: 0,
                    lookback: 16
                },
                Op::PushConst(0.1),
                Op::Gt,
                Op::Reversion {
                    slot: 1,
                    window: 64
                },
                Op::PushConst(-0.1),
                Op::Lt,
                Op::And,
                Op::Buy,
            ]
        );
    }

    #[test]
    fn ambiguous_condition_is_rejected_not_dropped() {
        // Two indicators wired straight into one condition is ambiguous — this
        // must error, NOT silently drop one input.
        let json = r#"{"canvas":{"nodes":[
            {"id":"d","type":"data_source","properties":{}},
            {"id":"m","type":"momentum","properties":{"lookback":16}},
            {"id":"r","type":"reversion","properties":{"window":64}},
            {"id":"c","type":"condition","properties":{"op":"greater_than","value":0.1}},
            {"id":"b","type":"buy_signal","properties":{}}
        ],"connections":[
            {"from":"d","to":"m"},{"from":"d","to":"r"},
            {"from":"m","to":"c"},{"from":"r","to":"c"},
            {"from":"c","to":"b"}
        ]}}"#;
        let spec = parse(json).unwrap();
        assert!(matches!(
            compile(&spec),
            Err(CompileError::TooManyInputs { .. })
        ));
    }
}
