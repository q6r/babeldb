fn main() {
    let code = babeldb::cli::main_entry(std::env::args().collect());
    std::process::exit(code);
}
