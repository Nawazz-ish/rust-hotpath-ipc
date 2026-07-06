//! Compile a strategy graph JSON and print its bytecode disassembly.
//! Usage: cargo run --example disasm -- path/to/graph.json
use rust_hotpath_ipc::{bytecode::Vm, compiler};
fn main() {
    let path = std::env::args().nth(1).expect("usage: disasm <graph.json>");
    let json = std::fs::read_to_string(&path).expect("read graph");
    let spec = compiler::parse(&json).expect("parse");
    match compiler::compile(&spec) {
        Ok(prog) => {
            let vm = Vm::new(prog);
            println!("{} ops:", vm.program().len());
            print!("{}", vm.disassemble());
        }
        Err(e) => eprintln!("compile error: {e}"),
    }
}
