//! Command-line interface of `babeldb`: typed commands shared by the one-shot
//! CLI and the REPL, plus the minimal TCP server of stage 10.
//!
//! ```text
//! babeldb --db <path> [--mode adaptive|babel-pure] [--block-size N] [--inline-max N]
//!         [--history] <command> [args]
//! babeldb help [command]
//! ```
//!
//! - The whole command line is parsed, and the inputs of `put --file` and of
//!   `put` from standard input are read, before the database is opened: a
//!   usage error never creates or modifies a database. Only `put`, `import`,
//!   `gen`, `repl` and `serve` may create a missing database file.
//! - `--mode`, `--block-size` and `--inline-max` are creation parameters:
//!   persisted when the database is created; afterwards the persisted values
//!   win and a note is printed when the flags differ.
//! - Exit status: 0 success; 1 runtime error (engine or I/O error, key not
//!   found, failed verification); 2 usage error.
//!
//! Modules: [`commands`] (typed commands: parsing, execution, reports),
//! [`repl`] (interactive session), [`text`] (tokenizer, quoting, number and
//! size formats), [`protocol`] and [`server`] (binary TCP protocol, server
//! and the blocking client used to measure interface overhead).

pub mod commands;
pub mod protocol;
pub mod repl;
pub mod server;
pub mod text;

use std::any::Any;
use std::fmt;
use std::io::{self, IsTerminal, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::config::{Config, Mode};
use crate::engine::Db;
use crate::store::Store;
use crate::store::redb::RedbStore;

pub use commands::{Command, Flow, HelpContext, execute, parse};

pub const EXIT_OK: i32 = 0;
pub const EXIT_RUNTIME: i32 = 1;
pub const EXIT_USAGE: i32 = 2;

/// Options accepted before the command name.
pub const GLOBAL_OPTIONS: [&str; 5] = [
    "--db",
    "--mode",
    "--block-size",
    "--inline-max",
    "--history",
];

/// A bad command line; always detected before the database is opened.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UsageError {
    /// Canonical name of the command being parsed, when known.
    pub command: Option<&'static str>,
    pub message: String,
}

impl UsageError {
    pub fn new(command: Option<&'static str>, message: impl Into<String>) -> UsageError {
        UsageError {
            command,
            message: message.into(),
        }
    }

    pub fn general(message: impl Into<String>) -> UsageError {
        UsageError::new(None, message)
    }
}

impl fmt::Display for UsageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for UsageError {}

/// Failure of a command. Usage errors exit with status 2, everything else 1.
#[derive(Debug)]
pub enum CliError {
    Usage(UsageError),
    /// Error reported by the engine.
    Db(crate::Error),
    /// Reading an input or writing an output failed.
    Io(io::Error),
    /// The key (or revision) does not exist.
    NotFound(String),
    /// The command ran but reports a failure (e.g. `verify` found problems).
    Failed(String),
}

impl CliError {
    pub fn exit_code(&self) -> i32 {
        match self {
            CliError::Usage(_) => EXIT_USAGE,
            _ => EXIT_RUNTIME,
        }
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CliError::Usage(e) => write!(f, "{e}"),
            CliError::Db(e) => write!(f, "{e}"),
            CliError::Io(e) => write!(f, "{e}"),
            CliError::NotFound(what) => write!(f, "not found: {what}"),
            CliError::Failed(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for CliError {}

impl From<UsageError> for CliError {
    fn from(e: UsageError) -> Self {
        CliError::Usage(e)
    }
}

impl From<crate::Error> for CliError {
    fn from(e: crate::Error) -> Self {
        CliError::Db(e)
    }
}

impl From<io::Error> for CliError {
    fn from(e: io::Error) -> Self {
        CliError::Io(e)
    }
}

pub type CliResult<T> = std::result::Result<T, CliError>;

/// Prefix an I/O error with what was being done (keeps its kind).
pub(crate) fn io_context(e: io::Error, context: impl fmt::Display) -> io::Error {
    io::Error::new(e.kind(), format!("{context}: {e}"))
}

/// Text of a panic payload.
pub(crate) fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "panic with a non-text payload".to_string()
    }
}

// ---------------------------------------------------------------------------
// Global options
// ---------------------------------------------------------------------------

/// Options given before the command name.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GlobalOptions {
    pub db: Option<PathBuf>,
    pub mode: Option<Mode>,
    pub block_size: Option<u32>,
    pub inline_max: Option<u32>,
    /// Retain previous revisions (runtime option, not persisted).
    pub history: bool,
}

/// A fully parsed command line.
#[derive(Clone, Debug, PartialEq)]
pub struct Invocation {
    pub globals: GlobalOptions,
    pub command: Command,
}

fn parse_mode(s: &str) -> Result<Mode, UsageError> {
    match s {
        "adaptive" => Ok(Mode::Adaptive),
        "babel-pure" => Ok(Mode::BabelPure),
        other => Err(UsageError::general(format!(
            "unknown mode {} (expected adaptive or babel-pure)",
            text::quote(other.as_bytes())
        ))),
    }
}

fn parse_u32_size(option: &str, s: &str) -> Result<u32, UsageError> {
    let n = text::parse_size(s).map_err(|e| UsageError::general(format!("{option}: {e}")))?;
    u32::try_from(n).map_err(|_| UsageError::general(format!("{option}: {n} is too large")))
}

/// Parse the arguments after the program name: global options, then the
/// command and its arguments.
pub fn parse_invocation(args: &[String]) -> Result<Invocation, UsageError> {
    let mut globals = GlobalOptions::default();
    let mut seen: Vec<&'static str> = Vec::new();
    let mut i = 0;
    while let Some(arg) = args.get(i) {
        if !arg.starts_with('-') || arg == "-" {
            break;
        }
        i += 1;
        if arg == "--" {
            break;
        }
        let (name, inline) = match arg.split_once('=') {
            Some((n, v)) if arg.starts_with("--") => (n, Some(v)),
            _ => (arg.as_str(), None),
        };
        match name {
            "-h" | "--help" => {
                return Ok(Invocation {
                    globals,
                    command: Command::Help { topic: None },
                });
            }
            "-V" | "--version" => {
                return Ok(Invocation {
                    globals,
                    command: Command::Version,
                });
            }
            _ => {}
        }
        let Some(option) = GLOBAL_OPTIONS.iter().copied().find(|o| *o == name) else {
            return Err(UsageError::general(format!(
                "unknown global option {} (global options: {}; command options go after the command name)",
                text::quote(name.as_bytes()),
                GLOBAL_OPTIONS.join(", ")
            )));
        };
        if seen.contains(&option) {
            return Err(UsageError::general(format!(
                "option {option} given more than once"
            )));
        }
        seen.push(option);
        if option == "--history" {
            if inline.is_some() {
                return Err(UsageError::general(
                    "option --history does not take a value",
                ));
            }
            globals.history = true;
            continue;
        }
        let value = match inline {
            Some(v) => v,
            None => {
                let v = args.get(i).ok_or_else(|| {
                    UsageError::general(format!("option {option} requires a value"))
                })?;
                i += 1;
                v.as_str()
            }
        };
        match option {
            "--db" => {
                if value.is_empty() {
                    return Err(UsageError::general("--db requires a non-empty path"));
                }
                globals.db = Some(PathBuf::from(value));
            }
            "--mode" => globals.mode = Some(parse_mode(value)?),
            "--block-size" => globals.block_size = Some(parse_u32_size(option, value)?),
            _ => globals.inline_max = Some(parse_u32_size(option, value)?),
        }
    }
    let command = commands::parse(&args[i..])?;
    Ok(Invocation { globals, command })
}

/// Engine configuration for the global options (validated: an invalid
/// combination is a usage error).
pub fn config_for(globals: &GlobalOptions) -> Result<Config, UsageError> {
    let mut cfg = Config::default();
    if let Some(mode) = globals.mode {
        cfg.mode = mode;
    }
    if let Some(block_size) = globals.block_size {
        cfg.block_size = block_size;
    }
    match globals.inline_max {
        Some(inline_max) => cfg.inline_max = inline_max,
        // The default follows a smaller --block-size.
        None => cfg.inline_max = cfg.inline_max.min(cfg.block_size),
    }
    cfg.keep_history = globals.history;
    cfg.validate().map_err(|e| {
        let detail = match e {
            crate::Error::InvalidArgument(m) => m,
            other => other.to_string(),
        };
        UsageError::general(format!("invalid creation parameters: {detail}"))
    })?;
    Ok(cfg)
}

/// Notes for creation flags that an existing database overrides.
pub fn creation_notes<S: Store>(globals: &GlobalOptions, db: &Db<S>) -> Vec<String> {
    let tail = "creation parameters are persisted when the database is created";
    let mut notes = Vec::new();
    if let Some(mode) = globals.mode.filter(|&m| m != db.mode()) {
        notes.push(format!(
            "--mode {} ignored: this database was created with mode {} ({tail})",
            mode.as_str(),
            db.mode().as_str()
        ));
    }
    if let Some(bs) = globals.block_size.filter(|&b| b != db.block_size()) {
        notes.push(format!(
            "--block-size {bs} ignored: this database was created with block size {} ({tail})",
            db.block_size()
        ));
    }
    if let Some(im) = globals.inline_max.filter(|&i| i != db.inline_max()) {
        notes.push(format!(
            "--inline-max {im} ignored: this database was created with inline max {} ({tail})",
            db.inline_max()
        ));
    }
    notes
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Entry point of the `babeldb` binary (`args[0]` is the program name).
/// Returns the process exit status.
pub fn main_entry(args: Vec<String>) -> i32 {
    let rest = args.get(1..).unwrap_or(&[]);
    let invocation = match parse_invocation(rest) {
        Ok(invocation) => invocation,
        Err(e) => return report_usage(&e),
    };
    match run(invocation) {
        Ok(()) => EXIT_OK,
        Err(CliError::Usage(e)) => report_usage(&e),
        // The reader of the output went away (e.g. `| head`): not a failure.
        Err(CliError::Io(e)) if e.kind() == io::ErrorKind::BrokenPipe => EXIT_OK,
        Err(e) => {
            let hint = match &e {
                CliError::Io(io_err)
                    if io_err.kind() == io::ErrorKind::InvalidData
                        && io::stdout().is_terminal() =>
                {
                    " (this console cannot display these bytes: use --hex or --out <path>, or redirect stdout)"
                }
                _ => "",
            };
            let prefix = if matches!(e, CliError::NotFound(_)) {
                ""
            } else {
                "error: "
            };
            eprintln!("{prefix}{e}{hint}");
            e.exit_code()
        }
    }
}

fn report_usage(e: &UsageError) -> i32 {
    let mut err = io::stderr().lock();
    let _ = writeln!(err, "error: {}", e.message);
    match e.command.and_then(commands::command_help) {
        Some(help) => {
            let _ = writeln!(
                err,
                "usage: {}",
                commands::usage_line(help, HelpContext::Cli)
            );
            let _ = writeln!(err, "run 'babeldb help {}' for details", help.name);
        }
        None => {
            let _ = writeln!(
                err,
                "usage: babeldb --db <path> [global options] <command> [args]"
            );
            let _ = writeln!(err, "run 'babeldb help' for the list of commands");
        }
    }
    EXIT_USAGE
}

fn print_stdout(f: impl FnOnce(&mut dyn Write) -> io::Result<()>) -> CliResult<()> {
    let stdout = io::stdout();
    let mut out = stdout.lock();
    f(&mut out)?;
    out.flush()?;
    Ok(())
}

fn run(invocation: Invocation) -> CliResult<()> {
    let Invocation { globals, command } = invocation;
    match &command {
        Command::Help { topic } => {
            let topic = *topic;
            return print_stdout(|out| commands::write_help(out, topic, HelpContext::Cli));
        }
        Command::Version => {
            return print_stdout(|out| writeln!(out, "{}", commands::version_line()));
        }
        Command::Quit => {
            return Err(UsageError::new(
                Some("quit"),
                "'quit' and 'exit' only work inside the REPL",
            )
            .into());
        }
        _ => {}
    }
    let path = globals.db.clone().ok_or_else(|| {
        UsageError::new(
            Some(command.name()),
            "missing --db <path> (global options go before the command name)",
        )
    })?;
    let cfg = config_for(&globals)?;
    if matches!(
        command,
        Command::Put {
            value: commands::ValueSource::Stdin,
            ..
        }
    ) && io::stdin().is_terminal()
    {
        return Err(UsageError::new(
            Some("put"),
            "no value given: use --value, --hex or --file, or pipe the value on standard input",
        )
        .into());
    }
    let existed = path.exists();
    if !existed && !command.may_create_db() {
        return Err(CliError::Failed(format!(
            "database {} does not exist ('{}' never creates one; put, import, gen, repl and serve do)",
            path.display(),
            command.name()
        )));
    }
    // Inputs are read (and the listen address bound) before the database is
    // opened: a failure there leaves the database untouched.
    let command = commands::prepare_inputs(command)?;
    match command {
        Command::Serve { addr, threads } => {
            let listener = server::bind(addr.as_str())
                .map_err(|e| io_context(e, format!("cannot listen on {addr}")))?;
            let db = open_db(&path, cfg, &globals, existed)?;
            serve_forever(db, listener, threads, &path)
        }
        Command::Repl => {
            let mut db = open_db(&path, cfg, &globals, existed)?;
            run_repl(&mut db, &path)
        }
        command => {
            let mut db = open_db(&path, cfg, &globals, existed)?;
            let stdout = io::stdout();
            let mut out = io::BufWriter::with_capacity(64 * 1024, stdout.lock());
            let result = commands::execute(&mut db, &command, &mut out);
            let flushed = out.flush();
            result?;
            flushed?;
            Ok(())
        }
    }
}

fn open_db(
    path: &Path,
    cfg: Config,
    globals: &GlobalOptions,
    existed: bool,
) -> CliResult<Db<RedbStore>> {
    let db = Db::open(path, cfg)
        .map_err(|e| CliError::Failed(format!("cannot open {}: {e}", path.display())))?;
    if existed {
        for note in creation_notes(globals, &db) {
            eprintln!("note: {note}");
        }
    }
    Ok(db)
}

fn run_repl<S: Store>(db: &mut Db<S>, path: &Path) -> CliResult<()> {
    let interactive = io::stdin().is_terminal();
    let banner = format!(
        "babeldb {} - {} (mode {}, block size {} B, inline max {} B); 'help' lists the commands, 'quit' leaves",
        env!("CARGO_PKG_VERSION"),
        path.display(),
        db.mode().as_str(),
        db.block_size(),
        db.inline_max()
    );
    let opts = repl::ReplOptions {
        prompt: interactive.then(|| "babeldb> ".to_string()),
        banner: interactive.then_some(banner),
    };
    let stdin = io::stdin();
    let mut input = stdin.lock();
    let stdout = io::stdout();
    let mut out = stdout.lock();
    let mut err = io::stderr();
    repl::run(db, &mut input, &mut out, &mut err, &opts)?;
    Ok(())
}

fn serve_forever(
    db: Db<RedbStore>,
    listener: TcpListener,
    threads: Option<usize>,
    path: &Path,
) -> CliResult<()> {
    let threads = threads.unwrap_or_else(server::default_threads);
    let mode = db.mode();
    let handle = server::serve_listener(Arc::new(db), listener, threads)?;
    print_stdout(|out| {
        writeln!(
            out,
            "babeldb {}: serving {} (mode {}) on {} with {threads} worker threads; wire protocol v{}; stop with Ctrl+C",
            env!("CARGO_PKG_VERSION"),
            path.display(),
            mode.as_str(),
            handle.local_addr(),
            protocol::PROTOCOL_VERSION
        )
    })?;
    handle.wait();
    Ok(())
}
