//! Command-line interface: typed commands shared by the one-shot CLI and the REPL.
//! SKELETON — the CLI agent implements it.

pub mod commands;
pub mod repl;

/// Entry point of the `babeldb` binary. Returns the process exit code.
pub fn main_entry(args: Vec<String>) -> i32 {
    let _ = args;
    eprintln!("babeldb CLI not implemented yet");
    2
}
