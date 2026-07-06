//! A tiny stack-machine bytecode and interpreter for graph-defined strategies.
//!
//! A strategy the user draws in the visual builder is compiled (see
//! [`crate::compiler`]) into a flat program of [`Op`]s and executed by [`Vm`]
//! once per market tick. This mirrors the platform's strategy VM: a stack of
//! `f64`, booleans as `1.0`/`0.0`, operands pushed before their operator, and a
//! terminal `Buy`/`Sell` opcode that pops a truthy condition.
//!
//! One deliberate difference from a batch backtester: this runs on a *live tick
//! stream*, so indicators (EMA, momentum, mean-reversion) are **stateful ops** —
//! each carries an index (`slot`) into a parallel state vector that persists
//! across ticks. That state, the program, and the stack are all allocated once
//! when the [`Vm`] is built, so `run` does no per-tick allocation.
//!
//! The interpreter dispatches on the typed [`Op`] enum for speed and safety;
//! [`Vm::to_bytes`]/[`disassemble`] give the equivalent flat byte program (in the
//! platform's opcode space — `BUY = 0x30`, `GT = 0x50`, …) for inspection and
//! wire serialization.

use crate::strategy::Side;

const STACK_CAP: usize = 32;

/// Momentum reading of this many basis points maps to a full-scale (±1) signal.
/// Matches the fixed strategy's default so the VM's momentum op agrees with it.
const MOMENTUM_BPS_FULL_SCALE: f64 = 25.0;

// ============================================================================
// Opcodes
// ============================================================================

/// One stack-machine instruction.
///
/// Parameters of a *stateful* indicator (period, lookback, window) are frozen
/// into the op at compile time; the running state lives in a parallel vector
/// indexed by `slot`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Op {
    // --- data in ---
    PushPrice,      // push the current tick price
    PushConst(f64), // push an immediate (a threshold, etc.)

    // --- stateful indicators (each carries its state slot) ---
    Ema { slot: usize, period: usize },        // push EMA(price)
    Momentum { slot: usize, lookback: usize }, // push return-bps signal in [-1, 1]
    Reversion { slot: usize, window: usize },  // push -zscore signal in [-1, 1]

    // --- comparisons (pop 2, push bool) ---
    Gt,
    Lt,
    Ge,
    Le,
    Eq,

    // --- logic on booleans (>0.0 is truthy) ---
    And,
    Or,
    Not,

    // --- terminal decision (pop condition; fire if truthy) ---
    Buy,
    Sell,
}

impl Op {
    /// The single-byte opcode, in the platform's opcode space. Operands (for
    /// `PushConst` and the indicators) follow in the byte stream; see
    /// [`Vm::to_bytes`].
    pub fn opcode(&self) -> u8 {
        match self {
            Op::PushPrice => 0x41,
            Op::PushConst(_) => 0x40,
            Op::Ema { .. } => 0x60,
            Op::Momentum { .. } => 0x61,
            Op::Reversion { .. } => 0x62,
            Op::Gt => 0x50,
            Op::Lt => 0x51,
            Op::Ge => 0x52,
            Op::Le => 0x53,
            Op::Eq => 0x54,
            Op::And => 0x10,
            Op::Or => 0x11,
            Op::Not => 0x12,
            Op::Buy => 0x30,
            Op::Sell => 0x31,
        }
    }

    fn mnemonic(&self) -> &'static str {
        match self {
            Op::PushPrice => "PUSH_PRICE",
            Op::PushConst(_) => "PUSH_CONST",
            Op::Ema { .. } => "EMA",
            Op::Momentum { .. } => "MOMENTUM",
            Op::Reversion { .. } => "REVERSION",
            Op::Gt => "GT",
            Op::Lt => "LT",
            Op::Ge => "GE",
            Op::Le => "LE",
            Op::Eq => "EQ",
            Op::And => "AND",
            Op::Or => "OR",
            Op::Not => "NOT",
            Op::Buy => "BUY",
            Op::Sell => "SELL",
        }
    }

    fn is_stateful(&self) -> bool {
        matches!(
            self,
            Op::Ema { .. } | Op::Momentum { .. } | Op::Reversion { .. }
        )
    }
}

// ============================================================================
// Indicator state (one entry per stateful op, in slot order)
// ============================================================================

/// Per-instruction indicator state, carried across ticks. Each stateful op owns
/// one of these; the ring buffers are pre-sized so `run` never allocates.
enum IndState {
    Ema {
        alpha: f64,
        value: f64,
        init: bool,
    },
    // Momentum over `lookback`: a ring of the last `lookback + 1` prices.
    Momentum {
        buf: Vec<f64>,
        filled: usize,
        head: usize,
        lookback: usize,
    },
    // Mean-reversion z-score over `window`: a ring plus running sum and sum of
    // squares so mean/variance are O(1) per tick.
    Reversion {
        buf: Vec<f64>,
        filled: usize,
        head: usize,
        window: usize,
        sum: f64,
        sum_sq: f64,
    },
}

impl IndState {
    fn for_op(op: &Op) -> Option<Self> {
        match *op {
            Op::Ema { period, .. } => Some(IndState::Ema {
                alpha: 2.0 / (period as f64 + 1.0),
                value: 0.0,
                init: false,
            }),
            Op::Momentum { lookback, .. } => Some(IndState::Momentum {
                buf: vec![0.0; lookback + 1],
                filled: 0,
                head: 0,
                lookback,
            }),
            Op::Reversion { window, .. } => Some(IndState::Reversion {
                buf: vec![0.0; window],
                filled: 0,
                head: 0,
                window,
                sum: 0.0,
                sum_sq: 0.0,
            }),
            _ => None,
        }
    }
}

/// EMA update — mirrors `Ema::update` in the fixed strategy.
fn ema_update(s: &mut IndState, price: f64) -> f64 {
    if let IndState::Ema { alpha, value, init } = s {
        if *init {
            *value += *alpha * (price - *value);
        } else {
            *value = price;
            *init = true;
        }
        *value
    } else {
        0.0
    }
}

/// Momentum signal — mirrors `Strategy::momentum_signal`.
fn momentum_update(s: &mut IndState, price: f64) -> f64 {
    if let IndState::Momentum {
        buf,
        filled,
        head,
        lookback,
    } = s
    {
        // push price into the ring
        buf[*head] = price;
        *head = (*head + 1) % buf.len();
        if *filled < buf.len() {
            *filled += 1;
        }
        if *filled <= *lookback {
            return 0.0;
        }
        // oldest = element at head (the slot about to be overwritten next)
        let oldest = buf[*head];
        let now = price;
        if oldest == 0.0 {
            return 0.0;
        }
        let bps = (now - oldest) / oldest * 10_000.0;
        (bps / MOMENTUM_BPS_FULL_SCALE).clamp(-1.0, 1.0)
    } else {
        0.0
    }
}

/// Mean-reversion signal — mirrors `Strategy::reversion_signal`.
fn reversion_update(s: &mut IndState, price: f64) -> f64 {
    if let IndState::Reversion {
        buf,
        filled,
        head,
        window,
        sum,
        sum_sq,
    } = s
    {
        // evict the value we're about to overwrite once the window is full
        if *filled == *window {
            let old = buf[*head];
            *sum -= old;
            *sum_sq -= old * old;
        }
        buf[*head] = price;
        *sum += price;
        *sum_sq += price * price;
        *head = (*head + 1) % *window;
        if *filled < *window {
            *filled += 1;
        }
        if *filled < *window {
            return 0.0;
        }
        let n = *window as f64;
        let mean = *sum / n;
        let var = (*sum_sq / n) - mean * mean;
        if var <= 1e-12 {
            return 0.0;
        }
        let std = var.sqrt();
        let z = (price - mean) / std;
        (-z / 2.0).clamp(-1.0, 1.0)
    } else {
        0.0
    }
}

// ============================================================================
// The VM
// ============================================================================

/// The decision a program produces for one tick.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum VmDecision {
    Buy(f64),
    Sell(f64),
    Hold,
}

/// A compiled strategy program plus its per-tick execution state.
pub struct Vm {
    prog: Vec<Op>,
    state: Vec<IndState>, // parallel to the stateful ops; `slot` indexes here
    stack: [f64; STACK_CAP],
    sp: usize,
}

impl Vm {
    /// Build a VM from a compiled program. Allocates the indicator state (and its
    /// ring buffers) once, in slot order — so nothing is allocated per tick.
    pub fn new(prog: Vec<Op>) -> Self {
        let state: Vec<IndState> = prog.iter().filter_map(IndState::for_op).collect();
        debug_assert_eq!(
            state.len(),
            prog.iter().filter(|o| o.is_stateful()).count(),
            "one state cell per stateful op"
        );
        Self {
            prog,
            state,
            stack: [0.0; STACK_CAP],
            sp: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.prog.is_empty()
    }

    pub fn program(&self) -> &[Op] {
        &self.prog
    }

    /// Execute the whole straight-line program for one price. No branches or
    /// jumps (the graph is a DAG lowered in topological order), so this is a
    /// single linear pass over `prog`.
    #[inline]
    pub fn run(&mut self, price: f64) -> VmDecision {
        self.sp = 0;
        let mut decision = VmDecision::Hold;
        let n = self.prog.len();
        let mut i = 0;
        while i < n {
            match self.prog[i] {
                Op::PushPrice => self.push(price),
                Op::PushConst(c) => self.push(c),
                Op::Ema { slot, .. } => {
                    let v = ema_update(&mut self.state[slot], price);
                    self.push(v);
                }
                Op::Momentum { slot, .. } => {
                    let v = momentum_update(&mut self.state[slot], price);
                    self.push(v);
                }
                Op::Reversion { slot, .. } => {
                    let v = reversion_update(&mut self.state[slot], price);
                    self.push(v);
                }
                Op::Gt => {
                    let b = self.pop();
                    let a = self.pop();
                    self.push(bool_f64(a > b));
                }
                Op::Lt => {
                    let b = self.pop();
                    let a = self.pop();
                    self.push(bool_f64(a < b));
                }
                Op::Ge => {
                    let b = self.pop();
                    let a = self.pop();
                    self.push(bool_f64(a >= b));
                }
                Op::Le => {
                    let b = self.pop();
                    let a = self.pop();
                    self.push(bool_f64(a <= b));
                }
                Op::Eq => {
                    let b = self.pop();
                    let a = self.pop();
                    self.push(bool_f64((a - b).abs() < 1e-10));
                }
                Op::And => {
                    let b = self.pop();
                    let a = self.pop();
                    self.push(bool_f64(a > 0.0 && b > 0.0));
                }
                Op::Or => {
                    let b = self.pop();
                    let a = self.pop();
                    self.push(bool_f64(a > 0.0 || b > 0.0));
                }
                Op::Not => {
                    let a = self.pop();
                    self.push(bool_f64(a <= 0.0));
                }
                Op::Buy => {
                    let c = self.pop();
                    if c > 0.0 {
                        decision = VmDecision::Buy(c);
                    }
                }
                Op::Sell => {
                    let c = self.pop();
                    if c > 0.0 {
                        decision = VmDecision::Sell(c);
                    }
                }
            }
            i += 1;
        }
        decision
    }

    #[inline]
    fn push(&mut self, v: f64) {
        if self.sp < STACK_CAP {
            self.stack[self.sp] = v;
            self.sp += 1;
        }
    }

    #[inline]
    fn pop(&mut self) -> f64 {
        if self.sp > 0 {
            self.sp -= 1;
            self.stack[self.sp]
        } else {
            0.0
        }
    }

    /// Flat byte program in the platform's opcode space, for inspection or wire
    /// transport. Immediates are little-endian f64; indicator periods are u16.
    pub fn to_bytes(&self) -> Vec<u8> {
        program_to_bytes(&self.prog)
    }

    /// Human-readable disassembly of the program.
    pub fn disassemble(&self) -> String {
        disassemble(&self.prog)
    }
}

#[inline]
fn bool_f64(b: bool) -> f64 {
    if b {
        1.0
    } else {
        0.0
    }
}

/// Convert a [`VmDecision`] to the fixed strategy's [`crate::strategy::Decision`]
/// so the runner can treat both paths identically.
pub fn to_side(d: VmDecision) -> Option<(Side, f64)> {
    match d {
        VmDecision::Buy(s) => Some((Side::Buy, s)),
        VmDecision::Sell(s) => Some((Side::Sell, s)),
        VmDecision::Hold => None,
    }
}

// ============================================================================
// Serialization / disassembly
// ============================================================================

fn program_to_bytes(prog: &[Op]) -> Vec<u8> {
    let mut out = Vec::with_capacity(prog.len() * 2);
    for op in prog {
        out.push(op.opcode());
        match *op {
            Op::PushConst(c) => out.extend_from_slice(&c.to_le_bytes()),
            Op::Ema { period, .. } => out.extend_from_slice(&(period as u16).to_le_bytes()),
            Op::Momentum { lookback, .. } => {
                out.extend_from_slice(&(lookback as u16).to_le_bytes())
            }
            Op::Reversion { window, .. } => out.extend_from_slice(&(window as u16).to_le_bytes()),
            _ => {}
        }
    }
    out
}

/// Render a program as assembly-style text, one instruction per line.
pub fn disassemble(prog: &[Op]) -> String {
    let mut s = String::new();
    for (i, op) in prog.iter().enumerate() {
        let operand = match *op {
            Op::PushConst(c) => format!(" {c}"),
            Op::Ema { period, slot } => format!(" period={period} slot={slot}"),
            Op::Momentum { lookback, slot } => format!(" lookback={lookback} slot={slot}"),
            Op::Reversion { window, slot } => format!(" window={window} slot={slot}"),
            _ => String::new(),
        };
        s.push_str(&format!(
            "{:>3}  0x{:02X}  {}{}\n",
            i,
            op.opcode(),
            op.mnemonic(),
            operand
        ));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_const_gt_buy_fires_only_when_price_exceeds() {
        // program: if price > 100 -> Buy
        let mut vm = Vm::new(vec![Op::PushPrice, Op::PushConst(100.0), Op::Gt, Op::Buy]);
        assert_eq!(vm.run(99.0), VmDecision::Hold);
        assert_eq!(vm.run(101.0), VmDecision::Buy(1.0));
    }

    #[test]
    fn lt_sell() {
        let mut vm = Vm::new(vec![Op::PushPrice, Op::PushConst(50.0), Op::Lt, Op::Sell]);
        assert_eq!(vm.run(40.0), VmDecision::Sell(1.0));
        assert_eq!(vm.run(60.0), VmDecision::Hold);
    }

    #[test]
    fn and_or_not_truth_table() {
        // (price > 10) AND (price < 20) -> Buy
        let mut vm = Vm::new(vec![
            Op::PushPrice,
            Op::PushConst(10.0),
            Op::Gt,
            Op::PushPrice,
            Op::PushConst(20.0),
            Op::Lt,
            Op::And,
            Op::Buy,
        ]);
        assert_eq!(vm.run(15.0), VmDecision::Buy(1.0)); // in band
        assert_eq!(vm.run(5.0), VmDecision::Hold); // below
        assert_eq!(vm.run(25.0), VmDecision::Hold); // above
    }

    #[test]
    fn ema_converges_to_constant() {
        let mut vm = Vm::new(vec![
            Op::Ema {
                slot: 0,
                period: 10,
            },
            Op::PushConst(0.0),
            Op::Gt,
        ]);
        for _ in 0..200 {
            vm.run(50.0);
        }
        // stack after run isn't observable, but state should have converged;
        // check via a program that emits the EMA compared to 49.9 -> true.
        let mut vm2 = Vm::new(vec![
            Op::Ema {
                slot: 0,
                period: 10,
            },
            Op::PushConst(49.9),
            Op::Gt,
            Op::Buy,
        ]);
        let mut last = VmDecision::Hold;
        for _ in 0..200 {
            last = vm2.run(50.0);
        }
        assert_eq!(last, VmDecision::Buy(1.0));
    }

    #[test]
    fn reversion_negative_after_spike() {
        // Emit reversion, compare < 0 -> Sell. Fill window at 100, then spike.
        let mut vm = Vm::new(vec![
            Op::Reversion {
                slot: 0,
                window: 64,
            },
            Op::PushConst(0.0),
            Op::Lt,
            Op::Sell,
        ]);
        for _ in 0..64 {
            vm.run(100.0);
        }
        let d = vm.run(110.0); // spike above mean -> reversion negative -> Sell
        assert_eq!(d, VmDecision::Sell(1.0));
    }

    #[test]
    fn to_bytes_and_disassemble_roundtrip_shapes() {
        let prog = vec![Op::PushPrice, Op::PushConst(0.25), Op::Gt, Op::Buy];
        let bytes = program_to_bytes(&prog);
        // PushPrice(1) + PushConst(1+8) + Gt(1) + Buy(1) = 12 bytes
        assert_eq!(bytes.len(), 1 + 9 + 1 + 1);
        assert_eq!(bytes[0], 0x41); // PUSH_PRICE
        assert!(disassemble(&prog).contains("BUY"));
    }

    #[test]
    fn empty_program_holds() {
        let mut vm = Vm::new(vec![]);
        assert!(vm.is_empty());
        assert_eq!(vm.run(123.0), VmDecision::Hold);
    }
}
