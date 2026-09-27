//! `babeldb` command-line tool: see `babeldb help` and `babeldb::cli`.

fn main() {
    let mut args = Vec::new();
    for (i, arg) in std::env::args_os().enumerate() {
        match arg.into_string() {
            Ok(s) => args.push(s),
            Err(raw) => {
                // Keys and paths must round-trip exactly: never guess an encoding.
                eprintln!(
                    "error: argument {i} is not valid Unicode: {}",
                    raw.to_string_lossy()
                );
                std::process::exit(babeldb::cli::EXIT_USAGE);
            }
        }
    }
    std::process::exit(babeldb::cli::main_entry(args));
}
