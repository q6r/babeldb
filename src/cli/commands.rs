//! Typed commands shared by the one-shot CLI and the REPL.
//!
//! Parsing ([`parse`], [`parse_tokens`]) is separate from execution
//! ([`execute`]), so every usage error is detected before the database is
//! opened. Keys and values are bytes: on the command line they are the UTF-8
//! bytes of the arguments; in the REPL the tokenizer can produce any byte
//! (see [`crate::cli::text::tokenize`]).
//!
//! Output conventions:
//! - `get`, `range` and `get-at` write the exact stored bytes and nothing else
//!   (`--hex` prints lowercase hex plus a newline, `--out` writes a file).
//! - Record lines are tab-separated: `<key>\trev=<R>\tlen=<N>` (`scan`, `head`)
//!   and `rev=<R>\tlen=<N>` (`put`, `import`, `gen`). Keys and values are
//!   printed with [`quote`], whose output the REPL tokenizer reads back exactly.
//! - Reports (`inspect`, `stats`, `verify`, ...) are `label  value` lines;
//!   sizes show the exact byte count plus a human-readable form. A
//!   stored/logical ratio is only printed next to the components it is made
//!   of, metadata included; no report prints a bare savings percentage.

use std::borrow::Cow;
use std::fmt;
use std::io::{self, Read, Write};
use std::path::PathBuf;

use super::text::{
    fmt_bytes, fmt_ratio, fmt_signed_bytes, format_unix_ms, hex_decode, hex_encode, parse_size,
    parse_u64, quote,
};
use super::{CliError, CliResult, GLOBAL_OPTIONS, UsageError, io_context};
use crate::engine::{Db, Expect, HistoryEntry, Inspection, ScanOptions};
use crate::format::{ENVELOPE_HEADER_LEN, FORMAT_VERSION, SourceDescriptor, source_kind};
use crate::generator::{self, ids};
use crate::ingest::ImportOptions;
use crate::maintenance::{CompactReport, GcReport, VerifyReport};
use crate::planner::{TrainOptions, TrainReport};
use crate::stats::Stats;
use crate::store::Store;

/// What the caller does after a command.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Flow {
    Continue,
    /// `quit` / `exit`: leave the REPL.
    Quit,
}

/// Version of the built-in generators invoked by `gen`.
pub const GENERATOR_VERSION: u16 = 1;
/// Default number of samples read by `train-dict` / `train-template`.
pub const DEFAULT_TRAIN_LIMIT: usize = 10_000;
/// Default listen address of `serve`.
pub const DEFAULT_ADDR: &str = "127.0.0.1:7878";
/// Upper bound of `serve --threads`.
pub const MAX_THREADS: usize = 1024;

/// Where `put` takes its value from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValueSource {
    /// `--value` / `--hex`, or input already read by [`prepare_inputs`].
    Bytes(Vec<u8>),
    /// `--file`: the file's exact bytes.
    File(PathBuf),
    /// Standard input until EOF (one-shot mode only).
    Stdin,
}

/// Where `get`, `range` and `get-at` write the bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Output {
    /// Exact bytes to the output stream, nothing added.
    Raw,
    /// Lowercase hex followed by a newline.
    Hex,
    /// Exact bytes to a file.
    File(PathBuf),
}

/// Record described by a built-in generator (`gen`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GenSpec {
    Arith { start: u64, step: u64, count: u64 },
    Xof { key: [u8; 32], len: u64 },
    Repeat { total: u64, motif: Vec<u8> },
}

impl GenSpec {
    pub fn generator_id(&self) -> u16 {
        match self {
            GenSpec::Arith { .. } => ids::ARITH_U64,
            GenSpec::Xof { .. } => ids::BLAKE3_XOF,
            GenSpec::Repeat { .. } => ids::REPEAT,
        }
    }

    pub fn params(&self) -> Vec<u8> {
        match self {
            GenSpec::Arith { start, step, count } => generator::arith_params(*start, *step, *count),
            GenSpec::Xof { key, len } => generator::blake3_xof_params(key, *len),
            GenSpec::Repeat { total, motif } => generator::repeat_params(*total, motif),
        }
    }
}

/// Options of `train-dict` / `train-template`.
#[derive(Clone, Debug, PartialEq)]
pub struct TrainArgs {
    pub prefix: Vec<u8>,
    /// Maximum number of sample values read under the prefix.
    pub limit: usize,
    pub expected_uses: u64,
    pub validation_fraction: f64,
    /// Maximum dictionary/template size (`TrainOptions::max_dict_bytes`).
    pub max_bytes: usize,
    /// Install even without a projected net gain.
    pub force: bool,
}

impl TrainArgs {
    pub fn options(&self) -> TrainOptions {
        TrainOptions {
            expected_uses: self.expected_uses,
            validation_fraction: self.validation_fraction,
            max_dict_bytes: self.max_bytes,
            require_gain: !self.force,
        }
    }
}

/// A fully parsed command. See `babeldb help <command>` for each one.
#[derive(Clone, Debug, PartialEq)]
pub enum Command {
    Put {
        key: Vec<u8>,
        value: ValueSource,
        expect: Expect,
    },
    Get {
        key: Vec<u8>,
        output: Output,
    },
    Range {
        key: Vec<u8>,
        offset: u64,
        len: u64,
        output: Output,
    },
    Head {
        key: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
        expect: Expect,
    },
    Import {
        key: Vec<u8>,
        path: PathBuf,
        expect: Expect,
    },
    Gen {
        key: Vec<u8>,
        spec: GenSpec,
        expect: Expect,
    },
    Inspect {
        key: Vec<u8>,
    },
    /// `limit == 0` means no limit.
    Scan {
        prefix: Option<Vec<u8>>,
        limit: usize,
        reverse: bool,
        values: bool,
    },
    History {
        key: Vec<u8>,
    },
    GetAt {
        key: Vec<u8>,
        revision: u64,
        output: Output,
    },
    PruneHistory {
        key: Option<Vec<u8>>,
        keep: usize,
    },
    Sources,
    Stats,
    Verify {
        deep: bool,
    },
    Gc,
    Compact,
    TrainDict(TrainArgs),
    TrainTemplate(TrainArgs),
    Repl,
    /// `threads == None`: [`crate::cli::server::default_threads`].
    Serve {
        addr: String,
        threads: Option<usize>,
    },
    /// `topic` is a canonical command name.
    Help {
        topic: Option<&'static str>,
    },
    Version,
    Quit,
}

impl Command {
    /// Canonical command name.
    pub fn name(&self) -> &'static str {
        match self {
            Command::Put { .. } => "put",
            Command::Get { .. } => "get",
            Command::Range { .. } => "range",
            Command::Head { .. } => "head",
            Command::Delete { .. } => "delete",
            Command::Import { .. } => "import",
            Command::Gen { .. } => "gen",
            Command::Inspect { .. } => "inspect",
            Command::Scan { .. } => "scan",
            Command::History { .. } => "history",
            Command::GetAt { .. } => "get-at",
            Command::PruneHistory { .. } => "prune-history",
            Command::Sources => "sources",
            Command::Stats => "stats",
            Command::Verify { .. } => "verify",
            Command::Gc => "gc",
            Command::Compact => "compact",
            Command::TrainDict(_) => "train-dict",
            Command::TrainTemplate(_) => "train-template",
            Command::Repl => "repl",
            Command::Serve { .. } => "serve",
            Command::Help { .. } => "help",
            Command::Version => "version",
            Command::Quit => "quit",
        }
    }

    /// Commands allowed to create a missing database file. Every other
    /// command requires an existing database, so a typo in `--db` never
    /// leaves an empty database behind.
    pub fn may_create_db(&self) -> bool {
        matches!(
            self,
            Command::Put { .. }
                | Command::Import { .. }
                | Command::Gen { .. }
                | Command::Repl
                | Command::Serve { .. }
        )
    }
}

// ---------------------------------------------------------------------------
// Help
// ---------------------------------------------------------------------------

/// Help entry of one command.
#[derive(Clone, Copy, Debug)]
pub struct CommandHelp {
    pub name: &'static str,
    /// Synopsis without the `babeldb --db <path>` prefix.
    pub usage: &'static str,
    pub summary: &'static str,
    pub details: &'static [&'static str],
}

/// Every command, in help order.
pub const COMMANDS: &[CommandHelp] = &[
    CommandHelp {
        name: "put",
        usage: "put <key> [--value <text> | --hex <hex> | --file <path>] [--if-absent | --if-revision <N>]",
        summary: "store a value",
        details: &[
            "The value comes from exactly one of:",
            "  --value <text>   the argument's bytes (UTF-8 on the command line; REPL escapes such as \\xHH give any byte)",
            "  --hex <hex>      hex-encoded bytes, e.g. 00ff10 (an empty string stores an empty value)",
            "  --file <path>    the file's exact bytes, read into memory (use `import` to stream large files)",
            "  (none)           standard input until EOF; one-shot mode only, and stdin must not be a terminal",
            "--if-absent fails unless the key is absent; --if-revision <N> fails unless the current revision is N.",
            "Prints rev=<R><TAB>len=<N>. The write is durable when the command returns.",
        ],
    },
    CommandHelp {
        name: "get",
        usage: "get <key> [--hex | --out <path>]",
        summary: "print a value (exact bytes, nothing added)",
        details: &[
            "Writes the exact stored bytes to standard output, with no trailing newline.",
            "--hex prints lowercase hex followed by a newline; --out <path> writes the exact bytes to a file.",
            "Exit status 1 when the key does not exist.",
        ],
    },
    CommandHelp {
        name: "range",
        usage: "range <key> <offset> <len> [--hex | --out <path>]",
        summary: "print bytes [offset, offset+len) of a value",
        details: &[
            "The range is clamped to the value length: offset == length prints nothing, offset > length is an error.",
            "Only the blocks that intersect the range are read and decoded.",
        ],
    },
    CommandHelp {
        name: "head",
        usage: "head <key>",
        summary: "print the current revision and logical length",
        details: &[
            "Prints <key><TAB>rev=<R><TAB>len=<N>; exit status 1 when the key does not exist.",
        ],
    },
    CommandHelp {
        name: "delete",
        usage: "delete <key> [--if-revision <N>]",
        summary: "delete a key",
        details: &[
            "Exit status 1 when there was no live record to delete.",
            "With --history the deleted version is retained (the key's current state becomes a tombstone).",
        ],
    },
    CommandHelp {
        name: "import",
        usage: "import <key> <path> [--if-absent | --if-revision <N>]",
        summary: "stream a local file into a key (bounded memory)",
        details: &[
            "Stores the file's exact bytes and registers the file as a local-file source (see `sources`).",
            "Nothing becomes visible before the final durable commit; an interrupted import leaves only",
            "prepared objects, which `gc` removes.",
        ],
    },
    CommandHelp {
        name: "gen",
        usage: "gen <key> (arith <start> <step> <count> | xof <key-hex> <len> | repeat <total> <motif>) [--if-absent | --if-revision <N>]",
        summary: "store a record described by a built-in generator",
        details: &[
            "arith   <count> little-endian u64 values start, start+step, ... (overflow is an error)",
            "xof     <len> bytes of the BLAKE3 keyed XOF stream; <key-hex> is a 32-byte key as 64 hex digits",
            "repeat  <motif> (the argument's bytes) repeated to exactly <total> bytes",
            "Only the generator id and version, its parameters and a BLAKE3 digest of the output are stored.",
            "The generator code is part of the binary: it is not counted per record.",
        ],
    },
    CommandHelp {
        name: "inspect",
        usage: "inspect <key>",
        summary: "show how a record is stored, metadata included",
        details: &[
            "Shows the revision, logical length, kind (inline, chunks, generated, tombstone), manifest and",
            "envelope sizes, the codec, raw/body sizes and refcount of every unit, the source and the generator.",
            "The stored/logical ratio is always printed with its components (key, manifest, 64-byte envelope",
            "headers, bodies). Shared objects are counted in full; backend pages are not included (see `stats`).",
        ],
    },
    CommandHelp {
        name: "scan",
        usage: "scan [--prefix <p>] [--limit <n>] [--reverse] [--values]",
        summary: "list live records in key order",
        details: &[
            "One line per record: <key><TAB>rev=<R><TAB>len=<N>, plus <TAB>value=<V> with --values.",
            "Keys and values that are not plain printable text are printed in double quotes with escapes",
            "(\\xHH, \\n, ...), the syntax the REPL reads back. --limit 0 (the default) means no limit.",
        ],
    },
    CommandHelp {
        name: "history",
        usage: "history <key>",
        summary: "list the retained revisions of a key, oldest first",
        details: &[
            "Lines rev=<R><TAB>len=<N>, marked `tombstone` for deletions and `current` for the current state.",
            "Earlier revisions are only retained by writes made with --history.",
        ],
    },
    CommandHelp {
        name: "get-at",
        usage: "get-at <key> <revision> [--hex | --out <path>]",
        summary: "print the value of a key at a given revision",
        details: &["Same output rules as `get`; exit status 1 when that revision is not retained."],
    },
    CommandHelp {
        name: "prune-history",
        usage: "prune-history [--key <k>] --keep <N>",
        summary: "drop retained history, keeping the newest N entries per key",
        details: &[
            "Without --key every key is pruned. Prints the number of history entries removed.",
        ],
    },
    CommandHelp {
        name: "sources",
        usage: "sources",
        summary: "list registered import sources",
        details: &[
            "Kind, location, adapter version and the last successful import (revision, bytes, BLAKE3, time).",
        ],
    },
    CommandHelp {
        name: "stats",
        usage: "stats",
        summary: "full space and activity accounting",
        details: &[
            "Entry counts, live logical bytes, engine payload per table, per-codec usage, file apparent and",
            "allocated sizes, backend overhead, ratios printed with their components, block cache, planner",
            "choices and engine counters. Sizes are exact byte counts plus a human-readable form.",
            "Cache, planner and counter figures cover only the current process.",
        ],
    },
    CommandHelp {
        name: "verify",
        usage: "verify [--deep]",
        summary: "check the consistency of the database",
        details: &[
            "Checks manifests, objects, refcounts, hash candidates and params; --deep also decodes every object",
            "and checks its BLAKE3 digest. Exit status 1 when a problem that could return wrong bytes or lose",
            "data is found. Orphan objects and pending imports are reported, not errors (`gc` collects them).",
        ],
    },
    CommandHelp {
        name: "gc",
        usage: "gc",
        summary: "collect abandoned imports, orphan objects/candidates/params; fix refcount drift",
        details: &["Exclusive maintenance; prints what was removed or fixed."],
    },
    CommandHelp {
        name: "compact",
        usage: "compact",
        summary: "ask the backend to return free space to the file system",
        details: &[
            "Prints the file's apparent and allocated sizes before and after. Removing rows alone does not",
            "shrink the database file.",
        ],
    },
    CommandHelp {
        name: "train-dict",
        usage: "train-dict --prefix <p> [--limit <n>] [--expected-uses <n>] [--validation-fraction <f>] [--max-bytes <n>] [--force]",
        summary: "train a Zstd dictionary from the values under a prefix",
        details: &[
            "Samples are the values of up to --limit live keys under the prefix (default 10000).",
            "A fraction of them (--validation-fraction, default 0.2) is held out: the dictionary is installed",
            "only when the projected net gain over --expected-uses future units (default 10000) is positive,",
            "the dictionary's own size included; --force installs it anyway (experiments).",
            "--max-bytes caps the dictionary size (default 64KiB).",
        ],
    },
    CommandHelp {
        name: "train-template",
        usage: "train-template --prefix <p> [--limit <n>] [--expected-uses <n>] [--validation-fraction <f>] [--max-bytes <n>] [--force]",
        summary: "train a TemplatePatchV1 template from the values under a prefix",
        details: &[
            "Same options and installation rule as `train-dict`; --max-bytes caps the template size.",
        ],
    },
    CommandHelp {
        name: "repl",
        usage: "repl",
        summary: "interactive session with the same commands",
        details: &[
            "Tokens are separated by spaces; \"double quotes\" group; the escapes \\\" \\\\ \\n \\t \\r \\0 \\xHH and",
            "\"\\ \" work inside and outside quotes. Any unique prefix of a command name works (\"ins\" is",
            "inspect). Lines starting with # are comments. Errors never end the session; quit or exit does.",
            "In the REPL, put takes its value from --value, --hex or --file (standard input is the REPL).",
        ],
    },
    CommandHelp {
        name: "serve",
        usage: "serve [--addr <host:port>] [--threads <N>]",
        summary: "serve the binary TCP protocol (interface-cost measurements)",
        details: &[
            "Listens on --addr (default 127.0.0.1:7878) with a fixed pool of N worker threads (default: the",
            "available parallelism, at most 64); each worker serves one connection at a time, many requests",
            "per connection. Frames: u32 LE length | u8 op | payload (see the babeldb::cli::protocol docs).",
            "Ops: GET, PUT, DELETE, RANGE, SCAN_PREFIX, PING, PUT_BATCH. No authentication: bind to loopback.",
            "Runs until the process is stopped (Ctrl+C); every acknowledged write was committed durably.",
        ],
    },
    CommandHelp {
        name: "help",
        usage: "help [command]",
        summary: "show help, or the details of one command",
        details: &[],
    },
    CommandHelp {
        name: "version",
        usage: "version",
        summary: "print the program and persistent format versions",
        details: &[],
    },
    CommandHelp {
        name: "quit",
        usage: "quit",
        summary: "leave the REPL (alias: exit)",
        details: &[],
    },
];

/// Where help is shown: the one-shot CLI or the REPL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HelpContext {
    Cli,
    Repl,
}

pub fn command_help(name: &str) -> Option<&'static CommandHelp> {
    COMMANDS.iter().find(|c| c.name == name)
}

/// Every word accepted as a command name (`exit` is an alias of `quit`).
pub fn command_words() -> impl Iterator<Item = &'static str> {
    COMMANDS.iter().map(|c| c.name).chain(["exit"])
}

/// Canonical name of an exact command word.
pub fn canonical_name(word: &str) -> Option<&'static str> {
    if word == "exit" {
        return Some("quit");
    }
    command_help(word).map(|c| c.name)
}

/// Command words starting with `prefix`, sorted.
pub fn prefix_candidates(prefix: &str) -> Vec<&'static str> {
    let mut v: Vec<&'static str> = command_words().filter(|w| w.starts_with(prefix)).collect();
    v.sort_unstable();
    v
}

pub fn version_line() -> String {
    format!(
        "babeldb {} (persistent format version {FORMAT_VERSION}, wire protocol version {})",
        env!("CARGO_PKG_VERSION"),
        super::protocol::PROTOCOL_VERSION
    )
}

/// Synopsis of a command in a context.
pub fn usage_line(h: &CommandHelp, ctx: HelpContext) -> String {
    match ctx {
        HelpContext::Repl => h.usage.to_string(),
        HelpContext::Cli if matches!(h.name, "help" | "version" | "quit") => {
            format!("babeldb {}", h.usage)
        }
        HelpContext::Cli => format!("babeldb --db <path> [global options] {}", h.usage),
    }
}

fn write_command_list(out: &mut dyn Write, skip: &[&str]) -> io::Result<()> {
    for h in COMMANDS.iter().filter(|h| !skip.contains(&h.name)) {
        writeln!(out, "  {:<16} {}", h.name, h.summary)?;
    }
    Ok(())
}

/// General help (`topic == None`) or the help of one command.
pub fn write_help(out: &mut dyn Write, topic: Option<&str>, ctx: HelpContext) -> io::Result<()> {
    if let Some(h) = topic.and_then(command_help) {
        writeln!(out, "usage: {}", usage_line(h, ctx))?;
        writeln!(out)?;
        writeln!(out, "{}", h.summary)?;
        if !h.details.is_empty() {
            writeln!(out)?;
            for l in h.details {
                writeln!(out, "{l}")?;
            }
        }
        return Ok(());
    }
    match ctx {
        HelpContext::Cli => {
            let lines = [
                "usage: babeldb --db <path> [global options] <command> [args]",
                "       babeldb help [command]",
                "",
                "global options (before the command):",
                "  --db <path>          database file (redb); put, import, gen, repl and serve create it",
                "  --mode <mode>        adaptive (default) or babel-pure                          [creation]",
                "  --block-size <N>     block size of values above --inline-max, 512..1MiB (16KiB) [creation]",
                "  --inline-max <N>     values up to N bytes live inside the manifest (1KiB)      [creation]",
                "  --history            retain previous revisions on overwrite and delete (this run)",
                "  -h, --help           show this help",
                "  -V, --version        show the version",
                "[creation] parameters are persisted when the database is created; for an existing",
                "database the persisted values win and a note is printed when the flags differ.",
                "N accepts the suffixes K/KiB, M/MiB and G/GiB (powers of 1024).",
                "",
                "commands:",
            ];
            writeln!(
                out,
                "babeldb {} - embedded key-value store inspired by the Library of Babel",
                env!("CARGO_PKG_VERSION")
            )?;
            writeln!(out)?;
            for l in lines {
                writeln!(out, "{l}")?;
            }
            write_command_list(out, &["quit"])?;
            writeln!(out)?;
            writeln!(
                out,
                "exit status: 0 success; 1 runtime error (including key not found and a failed"
            )?;
            writeln!(
                out,
                "verify); 2 usage error. The command line is parsed before the database is opened,"
            )?;
            writeln!(
                out,
                "so a usage error never changes the database. 'babeldb help <command>' shows details."
            )?;
        }
        HelpContext::Repl => {
            writeln!(
                out,
                "commands (any unique prefix works, e.g. 'ins' for inspect):"
            )?;
            write_command_list(out, &["repl", "serve"])?;
            writeln!(out)?;
            writeln!(
                out,
                "syntax: tokens are separated by spaces; \"double quotes\" group; the escapes"
            )?;
            writeln!(
                out,
                "\\\" \\\\ \\n \\t \\r \\0 \\xHH (any byte) and \"\\ \" work inside and outside quotes;"
            )?;
            writeln!(
                out,
                "a line starting with # is a comment. 'help <command>' shows the details."
            )?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

/// Parse `[command, args...]` given as command-line strings (exact command
/// names; the REPL resolves prefixes before calling [`parse_tokens`]).
pub fn parse(args: &[String]) -> Result<Command, UsageError> {
    let tokens: Vec<Vec<u8>> = args.iter().map(|a| a.as_bytes().to_vec()).collect();
    parse_tokens(&tokens)
}

/// Parse `[command, args...]` given as byte strings.
pub fn parse_tokens(tokens: &[Vec<u8>]) -> Result<Command, UsageError> {
    let Some((first, rest)) = tokens.split_first() else {
        return Err(UsageError::general("missing command"));
    };
    let word = std::str::from_utf8(first).unwrap_or("");
    let Some(name) = canonical_name(word) else {
        return Err(unknown_command(first));
    };
    parse_command(name, rest)
}

/// Usage error for a word that is not a command (with suggestions).
pub fn unknown_command(word: &[u8]) -> UsageError {
    let candidates = std::str::from_utf8(word)
        .ok()
        .filter(|w| !w.is_empty())
        .map(prefix_candidates)
        .unwrap_or_default();
    let hint = match candidates.as_slice() {
        [] => String::from(" (run 'help' for the list of commands)"),
        [one] => format!(" (did you mean '{one}'?)"),
        many => format!(" (did you mean one of: {}?)", many.join(", ")),
    };
    UsageError::general(format!("unknown command {}{hint}", quote(word)))
}

#[derive(Clone, Copy)]
struct FlagDef {
    name: &'static str,
    takes_value: bool,
}

const fn flag(name: &'static str) -> FlagDef {
    FlagDef {
        name,
        takes_value: false,
    }
}

const fn opt(name: &'static str) -> FlagDef {
    FlagDef {
        name,
        takes_value: true,
    }
}

const EXPECT_FLAGS: [FlagDef; 2] = [flag("--if-absent"), opt("--if-revision")];
const OUTPUT_FLAGS: [FlagDef; 2] = [flag("--hex"), opt("--out")];
const TRAIN_FLAGS: [FlagDef; 6] = [
    opt("--prefix"),
    opt("--limit"),
    opt("--expected-uses"),
    opt("--validation-fraction"),
    opt("--max-bytes"),
    flag("--force"),
];

/// Arguments of one command split into positionals and options.
struct Parsed<'a> {
    cmd: &'static str,
    pos: Vec<&'a [u8]>,
    flags: Vec<(&'static str, Option<&'a [u8]>)>,
}

impl<'a> Parsed<'a> {
    /// Options may appear anywhere after the command name; `--opt value` and
    /// `--opt=value` are equivalent; `--` ends the options (for positional
    /// arguments that start with `-`); a lone `-` is a positional.
    fn new(
        cmd: &'static str,
        args: &'a [Vec<u8>],
        defs: &[FlagDef],
    ) -> Result<Parsed<'a>, UsageError> {
        let mut p = Parsed {
            cmd,
            pos: Vec::new(),
            flags: Vec::new(),
        };
        let mut only_positionals = false;
        let mut i = 0;
        while i < args.len() {
            let arg = args[i].as_slice();
            i += 1;
            if only_positionals || !arg.starts_with(b"-") || arg == b"-" {
                p.pos.push(arg);
                continue;
            }
            if arg == b"--" {
                only_positionals = true;
                continue;
            }
            let (name, inline) = match arg.iter().position(|&b| b == b'=') {
                Some(eq) if arg.starts_with(b"--") => (&arg[..eq], Some(&arg[eq + 1..])),
                _ => (arg, None),
            };
            let Some(def) = defs.iter().find(|d| d.name.as_bytes() == name) else {
                return Err(p.unknown_option(name, defs));
            };
            if p.flags.iter().any(|(n, _)| *n == def.name) {
                return Err(p.err(format!("option {} given more than once", def.name)));
            }
            let value = match (def.takes_value, inline) {
                (true, Some(v)) => Some(v),
                (true, None) => match args.get(i) {
                    Some(v) => {
                        i += 1;
                        Some(v.as_slice())
                    }
                    None => return Err(p.err(format!("option {} requires a value", def.name))),
                },
                (false, Some(_)) => {
                    return Err(p.err(format!("option {} does not take a value", def.name)));
                }
                (false, None) => None,
            };
            p.flags.push((def.name, value));
        }
        Ok(p)
    }

    fn err(&self, message: impl Into<String>) -> UsageError {
        UsageError::new(Some(self.cmd), message)
    }

    fn unknown_option(&self, name: &[u8], defs: &[FlagDef]) -> UsageError {
        let name_str = String::from_utf8_lossy(name);
        let hint = if GLOBAL_OPTIONS.contains(&name_str.as_ref()) {
            " (global options go before the command name)"
        } else {
            " (write -- before a positional argument that starts with '-')"
        };
        let valid = if defs.is_empty() {
            format!("'{}' takes no options", self.cmd)
        } else {
            let names: Vec<&str> = defs.iter().map(|d| d.name).collect();
            format!("valid options: {}", names.join(", "))
        };
        self.err(format!(
            "unknown option {} for '{}'{hint}; {valid}",
            quote(name),
            self.cmd
        ))
    }

    fn has(&self, name: &str) -> bool {
        self.flags.iter().any(|(n, _)| *n == name)
    }

    fn value(&self, name: &str) -> Option<&'a [u8]> {
        self.flags
            .iter()
            .find(|(n, _)| *n == name)
            .and_then(|(_, v)| *v)
    }

    fn exact<'b, const N: usize>(
        &self,
        args: &[&'b [u8]],
        names: [&str; N],
    ) -> Result<[&'b [u8]; N], UsageError> {
        if args.len() < N {
            return Err(self.err(format!("missing <{}>", names[args.len()])));
        }
        if args.len() > N {
            return Err(self.err(format!("unexpected argument {}", quote(args[N]))));
        }
        Ok(std::array::from_fn(|i| args[i]))
    }

    fn positionals<const N: usize>(&self, names: [&str; N]) -> Result<[&'a [u8]; N], UsageError> {
        self.exact(&self.pos, names)
    }

    fn text(&self, what: &str, v: &'a [u8]) -> Result<&'a str, UsageError> {
        std::str::from_utf8(v).map_err(|_| self.err(format!("{what} must be valid UTF-8")))
    }

    fn num(&self, what: &str, v: &[u8]) -> Result<u64, UsageError> {
        let s = std::str::from_utf8(v).map_err(|_| self.err(format!("{what}: not a number")))?;
        parse_u64(s).map_err(|e| self.err(format!("{what}: {e}")))
    }

    fn count(&self, what: &str, v: &[u8]) -> Result<usize, UsageError> {
        usize::try_from(self.num(what, v)?).map_err(|_| self.err(format!("{what}: too large")))
    }

    fn size(&self, what: &str, v: &[u8]) -> Result<usize, UsageError> {
        let s = std::str::from_utf8(v).map_err(|_| self.err(format!("{what}: not a size")))?;
        let n = parse_size(s).map_err(|e| self.err(format!("{what}: {e}")))?;
        usize::try_from(n).map_err(|_| self.err(format!("{what}: too large")))
    }

    fn path(&self, what: &str, v: &'a [u8]) -> Result<PathBuf, UsageError> {
        let s = self.text(what, v)?;
        if s.is_empty() {
            return Err(self.err(format!("{what} must not be empty")));
        }
        Ok(PathBuf::from(s))
    }

    /// `--if-absent` / `--if-revision <N>` (mutually exclusive).
    fn expect(&self) -> Result<Expect, UsageError> {
        let revision = self
            .value("--if-revision")
            .map(|v| self.num("--if-revision", v))
            .transpose()?;
        match (self.has("--if-absent"), revision) {
            (true, Some(_)) => {
                Err(self.err("--if-absent and --if-revision are mutually exclusive"))
            }
            (true, None) => Ok(Expect::Absent),
            (false, Some(r)) => Ok(Expect::Revision(r)),
            (false, None) => Ok(Expect::Any),
        }
    }

    /// `--hex` / `--out <path>` (mutually exclusive).
    fn output(&self) -> Result<Output, UsageError> {
        match (self.has("--hex"), self.value("--out")) {
            (true, Some(_)) => Err(self.err("--hex and --out are mutually exclusive")),
            (true, None) => Ok(Output::Hex),
            (false, Some(p)) => Ok(Output::File(self.path("--out", p)?)),
            (false, None) => Ok(Output::Raw),
        }
    }

    fn train_args(&self) -> Result<TrainArgs, UsageError> {
        self.positionals([])?;
        let prefix = self
            .value("--prefix")
            .ok_or_else(|| self.err("missing --prefix <p>"))?;
        let defaults = TrainOptions::default();
        let validation_fraction = match self.value("--validation-fraction") {
            None => defaults.validation_fraction,
            Some(v) => {
                let f: f64 = self
                    .text("--validation-fraction", v)?
                    .parse()
                    .map_err(|_| self.err("--validation-fraction must be a number"))?;
                if !(f > 0.0 && f < 1.0) {
                    return Err(self.err("--validation-fraction must be strictly between 0 and 1"));
                }
                f
            }
        };
        let limit = match self.value("--limit") {
            Some(v) => self.count("--limit", v)?,
            None => DEFAULT_TRAIN_LIMIT,
        };
        let expected_uses = match self.value("--expected-uses") {
            Some(v) => self.num("--expected-uses", v)?,
            None => defaults.expected_uses,
        };
        let max_bytes = match self.value("--max-bytes") {
            Some(v) => self.size("--max-bytes", v)?,
            None => defaults.max_dict_bytes,
        };
        Ok(TrainArgs {
            prefix: prefix.to_vec(),
            limit,
            expected_uses,
            validation_fraction,
            max_bytes,
            force: self.has("--force"),
        })
    }
}

fn parse_command(name: &'static str, rest: &[Vec<u8>]) -> Result<Command, UsageError> {
    match name {
        "put" => {
            let defs = [
                opt("--value"),
                opt("--hex"),
                opt("--file"),
                EXPECT_FLAGS[0],
                EXPECT_FLAGS[1],
            ];
            let p = Parsed::new(name, rest, &defs)?;
            let [key] = p.positionals(["key"])?;
            let given: Vec<&str> = ["--value", "--hex", "--file"]
                .into_iter()
                .filter(|f| p.has(f))
                .collect();
            if given.len() > 1 {
                return Err(p.err(format!("{} are mutually exclusive", given.join(" and "))));
            }
            let value = if let Some(v) = p.value("--value") {
                ValueSource::Bytes(v.to_vec())
            } else if let Some(h) = p.value("--hex") {
                ValueSource::Bytes(hex_decode(h).map_err(|e| p.err(format!("--hex: {e}")))?)
            } else if let Some(f) = p.value("--file") {
                ValueSource::File(p.path("--file", f)?)
            } else {
                ValueSource::Stdin
            };
            Ok(Command::Put {
                key: key.to_vec(),
                value,
                expect: p.expect()?,
            })
        }
        "get" => {
            let p = Parsed::new(name, rest, &OUTPUT_FLAGS)?;
            let [key] = p.positionals(["key"])?;
            Ok(Command::Get {
                key: key.to_vec(),
                output: p.output()?,
            })
        }
        "range" => {
            let p = Parsed::new(name, rest, &OUTPUT_FLAGS)?;
            let [key, offset, len] = p.positionals(["key", "offset", "len"])?;
            Ok(Command::Range {
                key: key.to_vec(),
                offset: p.num("<offset>", offset)?,
                len: p.num("<len>", len)?,
                output: p.output()?,
            })
        }
        "head" | "inspect" | "history" => {
            let p = Parsed::new(name, rest, &[])?;
            let [key] = p.positionals(["key"])?;
            let key = key.to_vec();
            Ok(match name {
                "head" => Command::Head { key },
                "inspect" => Command::Inspect { key },
                _ => Command::History { key },
            })
        }
        "delete" => {
            let p = Parsed::new(name, rest, &[opt("--if-revision")])?;
            let [key] = p.positionals(["key"])?;
            Ok(Command::Delete {
                key: key.to_vec(),
                expect: p.expect()?,
            })
        }
        "import" => {
            let p = Parsed::new(name, rest, &EXPECT_FLAGS)?;
            let [key, path] = p.positionals(["key", "path"])?;
            Ok(Command::Import {
                key: key.to_vec(),
                path: p.path("<path>", path)?,
                expect: p.expect()?,
            })
        }
        "gen" => parse_gen(Parsed::new(name, rest, &EXPECT_FLAGS)?),
        "scan" => {
            let defs = [
                opt("--prefix"),
                opt("--limit"),
                flag("--reverse"),
                flag("--values"),
            ];
            let p = Parsed::new(name, rest, &defs)?;
            p.positionals([])?;
            let limit = match p.value("--limit") {
                Some(v) => p.count("--limit", v)?,
                None => 0,
            };
            Ok(Command::Scan {
                prefix: p.value("--prefix").map(<[u8]>::to_vec),
                limit,
                reverse: p.has("--reverse"),
                values: p.has("--values"),
            })
        }
        "get-at" => {
            let p = Parsed::new(name, rest, &OUTPUT_FLAGS)?;
            let [key, revision] = p.positionals(["key", "revision"])?;
            Ok(Command::GetAt {
                key: key.to_vec(),
                revision: p.num("<revision>", revision)?,
                output: p.output()?,
            })
        }
        "prune-history" => {
            let p = Parsed::new(name, rest, &[opt("--key"), opt("--keep")])?;
            p.positionals([])?;
            let keep = p
                .value("--keep")
                .ok_or_else(|| p.err("missing --keep <N>"))?;
            Ok(Command::PruneHistory {
                key: p.value("--key").map(<[u8]>::to_vec),
                keep: p.count("--keep", keep)?,
            })
        }
        "verify" => {
            let p = Parsed::new(name, rest, &[flag("--deep")])?;
            p.positionals([])?;
            Ok(Command::Verify {
                deep: p.has("--deep"),
            })
        }
        "train-dict" => Ok(Command::TrainDict(
            Parsed::new(name, rest, &TRAIN_FLAGS)?.train_args()?,
        )),
        "train-template" => Ok(Command::TrainTemplate(
            Parsed::new(name, rest, &TRAIN_FLAGS)?.train_args()?,
        )),
        "serve" => {
            let p = Parsed::new(name, rest, &[opt("--addr"), opt("--threads")])?;
            p.positionals([])?;
            let addr = match p.value("--addr") {
                Some(a) => {
                    let a = p.text("--addr", a)?;
                    if a.is_empty() {
                        return Err(p.err("--addr must not be empty"));
                    }
                    a.to_string()
                }
                None => DEFAULT_ADDR.to_string(),
            };
            let threads = match p.value("--threads") {
                Some(t) => {
                    let n = p.count("--threads", t)?;
                    if !(1..=MAX_THREADS).contains(&n) {
                        return Err(p.err(format!("--threads must be between 1 and {MAX_THREADS}")));
                    }
                    Some(n)
                }
                None => None,
            };
            Ok(Command::Serve { addr, threads })
        }
        "help" => {
            let p = Parsed::new(name, rest, &[])?;
            let topic = match p.pos.as_slice() {
                [] => None,
                [t] => {
                    let word = std::str::from_utf8(t).unwrap_or("");
                    let topic = canonical_name(word).ok_or_else(|| {
                        p.err(format!(
                            "unknown command {} (run 'help' for the list)",
                            quote(t)
                        ))
                    })?;
                    Some(topic)
                }
                [_, extra, ..] => {
                    return Err(p.err(format!("unexpected argument {}", quote(extra))));
                }
            };
            Ok(Command::Help { topic })
        }
        _ => {
            let p = Parsed::new(name, rest, &[])?;
            p.positionals([])?;
            Ok(match name {
                "sources" => Command::Sources,
                "stats" => Command::Stats,
                "gc" => Command::Gc,
                "compact" => Command::Compact,
                "repl" => Command::Repl,
                "version" => Command::Version,
                "quit" => Command::Quit,
                other => {
                    return Err(UsageError::general(format!(
                        "command '{other}' has no parser"
                    )));
                }
            })
        }
    }
}

fn parse_gen(p: Parsed<'_>) -> Result<Command, UsageError> {
    let (key, kind, args) = match p.pos.as_slice() {
        [key, kind, args @ ..] => (*key, *kind, args),
        [_] => return Err(p.err("missing generator kind: arith, xof or repeat")),
        [] => return Err(p.err("missing <key>")),
    };
    let spec = match kind {
        b"arith" => {
            let [start, step, count] = p.exact(args, ["start", "step", "count"])?;
            GenSpec::Arith {
                start: p.num("<start>", start)?,
                step: p.num("<step>", step)?,
                count: p.num("<count>", count)?,
            }
        }
        b"xof" => {
            let [key_hex, len] = p.exact(args, ["key-hex", "len"])?;
            let bytes = hex_decode(key_hex).map_err(|e| p.err(format!("<key-hex>: {e}")))?;
            let n = bytes.len();
            let key: [u8; 32] = bytes.try_into().map_err(|_| {
                p.err(format!(
                    "<key-hex> must be 32 bytes (64 hex digits), got {n} bytes"
                ))
            })?;
            GenSpec::Xof {
                key,
                len: p.num("<len>", len)?,
            }
        }
        b"repeat" => {
            let [total, motif] = p.exact(args, ["total", "motif"])?;
            let total = p.num("<total>", total)?;
            if motif.is_empty() && total > 0 {
                return Err(p.err("<motif> must not be empty"));
            }
            GenSpec::Repeat {
                total,
                motif: motif.to_vec(),
            }
        }
        other => {
            return Err(p.err(format!(
                "unknown generator kind {} (expected arith, xof or repeat)",
                quote(other)
            )));
        }
    };
    Ok(Command::Gen {
        key: key.to_vec(),
        spec,
        expect: p.expect()?,
    })
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

/// Read the inputs of a one-shot command before the database is opened:
/// `put --file` and `put` from standard input become in-memory bytes, and
/// `import` checks that its file exists. A failure here leaves the database
/// untouched.
pub fn prepare_inputs(cmd: Command) -> CliResult<Command> {
    if let Command::Import { path, .. } = &cmd {
        let meta = std::fs::metadata(path)
            .map_err(|e| io_context(e, format!("cannot import {}", path.display())))?;
        if !meta.is_file() {
            return Err(CliError::Failed(format!(
                "cannot import {}: not a regular file",
                path.display()
            )));
        }
    }
    Ok(match cmd {
        Command::Put {
            key,
            value: ValueSource::File(path),
            expect,
        } => {
            let bytes = std::fs::read(&path)
                .map_err(|e| io_context(e, format!("cannot read {}", path.display())))?;
            Command::Put {
                key,
                value: ValueSource::Bytes(bytes),
                expect,
            }
        }
        Command::Put {
            key,
            value: ValueSource::Stdin,
            expect,
        } => {
            let mut bytes = Vec::new();
            io::stdin()
                .lock()
                .read_to_end(&mut bytes)
                .map_err(|e| io_context(e, "cannot read standard input"))?;
            Command::Put {
                key,
                value: ValueSource::Bytes(bytes),
                expect,
            }
        }
        other => other,
    })
}

fn read_value(src: &ValueSource) -> CliResult<Cow<'_, [u8]>> {
    Ok(match src {
        ValueSource::Bytes(b) => Cow::Borrowed(b.as_slice()),
        ValueSource::File(path) => Cow::Owned(
            std::fs::read(path)
                .map_err(|e| io_context(e, format!("cannot read {}", path.display())))?,
        ),
        ValueSource::Stdin => {
            let mut v = Vec::new();
            io::stdin()
                .lock()
                .read_to_end(&mut v)
                .map_err(|e| io_context(e, "cannot read standard input"))?;
            Cow::Owned(v)
        }
    })
}

fn not_found(key: &[u8]) -> CliError {
    CliError::NotFound(quote(key))
}

fn emit(out: &mut dyn Write, output: &Output, bytes: &[u8]) -> CliResult<()> {
    match output {
        Output::Raw => out.write_all(bytes)?,
        Output::Hex => {
            out.write_all(hex_encode(bytes).as_bytes())?;
            out.write_all(b"\n")?;
        }
        Output::File(path) => {
            std::fs::write(path, bytes)
                .map_err(|e| io_context(e, format!("cannot write {}", path.display())))?;
            writeln!(
                out,
                "wrote {} to {}",
                fmt_bytes(bytes.len() as u64),
                path.display()
            )?;
        }
    }
    Ok(())
}

/// `rev=<R>\tlen=<N>` after a write whose length is not known up front.
fn write_rev_len<S: Store>(db: &Db<S>, out: &mut dyn Write, key: &[u8], rev: u64) -> CliResult<()> {
    match db.head(key)? {
        Some((r, len)) if r == rev => writeln!(out, "rev={rev}\tlen={len}")?,
        _ => writeln!(out, "rev={rev}")?,
    }
    Ok(())
}

fn collect_samples<S: Store>(db: &Db<S>, a: &TrainArgs) -> CliResult<Vec<Vec<u8>>> {
    let opts = ScanOptions::prefix(&a.prefix)
        .limit(a.limit)
        .with_values(true);
    let samples: Vec<Vec<u8>> = db
        .scan(&opts)?
        .into_iter()
        .filter_map(|item| item.value)
        .collect();
    if samples.is_empty() {
        return Err(CliError::Failed(format!(
            "no live values under prefix {}: nothing to train on",
            quote(&a.prefix)
        )));
    }
    Ok(samples)
}

/// Execute one command against an open database, writing its output to `out`.
/// `repl` and `serve` are top-level commands and are rejected here.
pub fn execute<S: Store>(db: &mut Db<S>, cmd: &Command, out: &mut dyn Write) -> CliResult<Flow> {
    match cmd {
        Command::Put { key, value, expect } => {
            let data = read_value(value)?;
            let rev = db.put(key, &data, *expect)?;
            writeln!(out, "rev={rev}\tlen={}", data.len())?;
        }
        Command::Get { key, output } => {
            let v = db.get(key)?.ok_or_else(|| not_found(key))?;
            emit(out, output, &v)?;
        }
        Command::Range {
            key,
            offset,
            len,
            output,
        } => {
            let v = db
                .get_range(key, *offset, *len)?
                .ok_or_else(|| not_found(key))?;
            emit(out, output, &v)?;
        }
        Command::Head { key } => {
            let (rev, len) = db.head(key)?.ok_or_else(|| not_found(key))?;
            writeln!(out, "{}\trev={rev}\tlen={len}", quote(key))?;
        }
        Command::Delete { key, expect } => {
            if !db.delete(key, *expect)? {
                return Err(CliError::NotFound(format!(
                    "{} (nothing deleted)",
                    quote(key)
                )));
            }
            writeln!(out, "deleted {}", quote(key))?;
        }
        Command::Import { key, path, expect } => {
            let opts = ImportOptions {
                expect: *expect,
                ..ImportOptions::default()
            };
            let rev = db.import_file(key, path, &opts)?;
            write_rev_len(db, out, key, rev)?;
        }
        Command::Gen { key, spec, expect } => {
            let rev = db.put_generated(
                key,
                spec.generator_id(),
                GENERATOR_VERSION,
                &spec.params(),
                *expect,
            )?;
            write_rev_len(db, out, key, rev)?;
        }
        Command::Inspect { key } => {
            let inspection = db.inspect(key)?.ok_or_else(|| not_found(key))?;
            render_inspection(out, &inspection)?;
        }
        Command::Scan {
            prefix,
            limit,
            reverse,
            values,
        } => {
            let opts = match prefix {
                Some(p) => ScanOptions::prefix(p),
                None => ScanOptions::all(),
            }
            .limit(*limit)
            .reverse(*reverse)
            .with_values(*values);
            for item in db.scan(&opts)? {
                write!(
                    out,
                    "{}\trev={}\tlen={}",
                    quote(&item.key),
                    item.revision,
                    item.logical_len
                )?;
                if let Some(v) = &item.value {
                    write!(out, "\tvalue={}", quote(v))?;
                }
                writeln!(out)?;
            }
        }
        Command::History { key } => {
            let entries = db.history(key)?;
            if entries.is_empty() {
                return Err(not_found(key));
            }
            for e in &entries {
                render_history_entry(out, e)?;
            }
        }
        Command::GetAt {
            key,
            revision,
            output,
        } => {
            let v = db.get_at(key, *revision)?.ok_or_else(|| {
                CliError::NotFound(format!("{} at revision {revision}", quote(key)))
            })?;
            emit(out, output, &v)?;
        }
        Command::PruneHistory { key, keep } => {
            let removed = db.prune_history(key.as_deref(), *keep)?;
            writeln!(out, "removed {removed} history entries")?;
        }
        Command::Sources => {
            for (id, desc) in db.sources()? {
                render_source(out, id, &desc)?;
            }
        }
        Command::Stats => render_stats(out, &db.stats()?)?,
        Command::Verify { deep } => {
            let report = db.verify(*deep)?;
            render_verify(out, &report)?;
            if !report.ok() {
                return Err(CliError::Failed(
                    "verify found problems (see the report above)".into(),
                ));
            }
        }
        Command::Gc => render_gc(out, &db.gc()?)?,
        Command::Compact => render_compact(out, &db.compact()?)?,
        Command::TrainDict(args) => {
            let samples = collect_samples(db, args)?;
            let report = db.train_dictionary(&samples, &args.options())?;
            render_train(out, "train-dict", &report, args)?;
        }
        Command::TrainTemplate(args) => {
            let samples = collect_samples(db, args)?;
            let report = db.train_template(&samples, &args.options())?;
            render_train(out, "train-template", &report, args)?;
        }
        Command::Repl => {
            return Err(UsageError::new(
                Some("repl"),
                "'repl' is a top-level command (already in a session?)",
            )
            .into());
        }
        Command::Serve { .. } => {
            let msg =
                "'serve' is a top-level command: babeldb --db <path> serve [--addr <host:port>]";
            return Err(UsageError::new(Some("serve"), msg).into());
        }
        Command::Help { topic } => write_help(out, *topic, HelpContext::Cli)?,
        Command::Version => writeln!(out, "{}", version_line())?,
        Command::Quit => return Ok(Flow::Quit),
    }
    Ok(Flow::Continue)
}

// ---------------------------------------------------------------------------
// Reports
// ---------------------------------------------------------------------------

/// Width of the label column of reports.
const LABEL: usize = 22;

fn row(out: &mut dyn Write, label: &str, value: impl fmt::Display) -> io::Result<()> {
    writeln!(out, "{label:<LABEL$} {value}")
}

fn sub(out: &mut dyn Write, label: &str, value: impl fmt::Display) -> io::Result<()> {
    writeln!(out, "  {label:<w$} {value}", w = LABEL - 2)
}

fn source_kind_name(kind: u8) -> String {
    match kind {
        source_kind::LOCAL_FILE => "local-file".into(),
        source_kind::GENERATOR => "generator".into(),
        source_kind::EXTERNAL => "external".into(),
        k => format!("kind {k}"),
    }
}

fn last_import_summary(desc: &SourceDescriptor) -> String {
    match &desc.last_import {
        Some(li) => format!(
            "rev {}, {}, blake3 {}, at {}",
            li.revision,
            fmt_bytes(li.bytes),
            hex_encode(&li.digest),
            format_unix_ms(li.unix_ms)
        ),
        None => "none".into(),
    }
}

/// Space attributed to one record by `inspect`. Shared objects are counted
/// in full; backend page overhead is not included.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecordSpace {
    pub key: u64,
    /// Encoded manifest, inline envelopes included.
    pub manifest: u64,
    pub inline_envelopes: u64,
    pub object_envelopes: u64,
    pub units: u64,
    /// `units` x 64-byte envelope headers (inline and object units).
    pub envelope_headers: u64,
    pub bodies: u64,
}

impl RecordSpace {
    pub fn of(i: &Inspection) -> RecordSpace {
        let header = ENVELOPE_HEADER_LEN as u64;
        let mut s = RecordSpace {
            key: i.key.len() as u64,
            manifest: i.manifest_bytes,
            units: i.units.len() as u64,
            ..RecordSpace::default()
        };
        for u in &i.units {
            let envelope = header + u64::from(u.body_len);
            if u.object_id.is_some() {
                s.object_envelopes += envelope;
            } else {
                s.inline_envelopes += envelope;
            }
            s.bodies += u64::from(u.body_len);
        }
        s.envelope_headers = s.units * header;
        s
    }

    /// Key + manifest + envelopes of the referenced objects.
    pub fn stored(&self) -> u64 {
        self.key + self.manifest + self.object_envelopes
    }

    /// Manifest bytes that are not inline envelopes.
    pub fn manifest_overhead(&self) -> u64 {
        self.manifest.saturating_sub(self.inline_envelopes)
    }

    /// Everything stored that is not a unit body: key, manifest overhead and
    /// every envelope header.
    pub fn metadata(&self) -> u64 {
        self.key + self.manifest_overhead() + self.envelope_headers
    }
}

pub fn render_inspection(out: &mut dyn Write, i: &Inspection) -> io::Result<()> {
    let space = RecordSpace::of(i);
    row(out, "key", quote(&i.key))?;
    row(out, "revision", i.revision)?;
    row(out, "kind", i.kind)?;
    row(out, "logical length", fmt_bytes(i.logical_len))?;
    let manifest = fmt_bytes(i.manifest_bytes);
    row(
        out,
        "manifest",
        format!("{manifest} (encoded manifest in `records`, inline envelopes included)"),
    )?;
    let encoded = fmt_bytes(i.encoded_bytes);
    row(
        out,
        "encoded (engine)",
        format!("{encoded} (envelopes of the referenced units; shared objects in full)"),
    )?;
    row(out, "units", i.units.len())?;
    if !i.units.is_empty() {
        writeln!(
            out,
            "  {:>5}  {:<14}  {:<24}  {:>10}  {:>10}  {:>10}  {:>8}",
            "#", "location", "codec", "raw_len", "body_len", "envelope", "refcount"
        )?;
        for (n, u) in i.units.iter().enumerate() {
            let location = match u.object_id {
                Some(id) => format!("object {id}"),
                None => "inline".into(),
            };
            let codec = if u.aux_id != 0 {
                format!("{} (param {})", u.codec, u.aux_id)
            } else {
                u.codec.clone()
            };
            let refcount = u
                .refcount
                .map_or_else(|| "-".to_string(), |r| r.to_string());
            let envelope = ENVELOPE_HEADER_LEN as u64 + u64::from(u.body_len);
            writeln!(
                out,
                "  {n:>5}  {location:<14}  {codec:<24}  {:>10}  {:>10}  {envelope:>10}  {refcount:>8}",
                u.raw_len, u.body_len
            )?;
        }
    }
    writeln!(
        out,
        "space (this record; shared objects counted in full; backend pages excluded, see `stats`)"
    )?;
    sub(out, "key", fmt_bytes(space.key))?;
    let inline = fmt_bytes(space.inline_envelopes);
    sub(
        out,
        "manifest",
        format!("{} (inline envelopes {inline})", fmt_bytes(space.manifest)),
    )?;
    sub(out, "object envelopes", fmt_bytes(space.object_envelopes))?;
    sub(
        out,
        "stored total",
        format!(
            "{} = key + manifest + object envelopes",
            fmt_bytes(space.stored())
        ),
    )?;
    sub(
        out,
        "metadata",
        format!(
            "{} = key {} + manifest overhead {} + envelope headers {} ({} x {ENVELOPE_HEADER_LEN} B)",
            fmt_bytes(space.metadata()),
            space.key,
            space.manifest_overhead(),
            space.envelope_headers,
            space.units
        ),
    )?;
    if i.kind == "generated" {
        sub(
            out,
            "bodies",
            "0 B (described by generator parameters inside the manifest)",
        )?;
    } else {
        sub(out, "bodies", fmt_bytes(space.bodies))?;
    }
    if i.logical_len == 0 {
        let stored = fmt_bytes(space.stored());
        sub(
            out,
            "stored/logical",
            format!("n/a (logical length 0; the {stored} stored are all metadata)"),
        )?;
    } else {
        // A generated record is its parameters: they live inside the manifest.
        let parts = if i.kind == "generated" {
            format!(
                "key {} + manifest {} holding the generator parameters",
                space.key, space.manifest
            )
        } else {
            format!("metadata {} + bodies {}", space.metadata(), space.bodies)
        };
        sub(
            out,
            "stored/logical",
            format!(
                "{} = stored total {} / logical {} ({parts})",
                fmt_ratio(space.stored(), i.logical_len),
                space.stored(),
                i.logical_len,
            ),
        )?;
    }
    match &i.source {
        Some((id, desc)) => row(
            out,
            "source",
            format!(
                "#{id} {} {} (adapter v{}); last import: {}",
                source_kind_name(desc.kind),
                quote(desc.location.as_bytes()),
                desc.adapter_version,
                last_import_summary(desc)
            ),
        )?,
        None => row(out, "source", "none")?,
    }
    match &i.generator {
        Some((id, version, name, params_len)) => row(
            out,
            "generator",
            format!(
                "{name} (id {id}, version {version}); params {params_len} B inside the manifest; \
                 the generator code lives in the binary, not counted per record"
            ),
        )?,
        None => row(out, "generator", "none")?,
    }
    Ok(())
}

pub fn render_history_entry(out: &mut dyn Write, e: &HistoryEntry) -> io::Result<()> {
    write!(out, "rev={}\tlen={}", e.revision, e.logical_len)?;
    if e.tombstone {
        write!(out, "\ttombstone")?;
    }
    if e.current {
        write!(out, "\tcurrent")?;
    }
    writeln!(out)
}

pub fn render_source(out: &mut dyn Write, id: u64, desc: &SourceDescriptor) -> io::Result<()> {
    writeln!(out, "source #{id}")?;
    sub(out, "kind", source_kind_name(desc.kind))?;
    sub(out, "location", quote(desc.location.as_bytes()))?;
    sub(out, "adapter version", desc.adapter_version)?;
    sub(out, "last import", last_import_summary(desc))
}

pub fn render_stats(out: &mut dyn Write, s: &Stats) -> io::Result<()> {
    writeln!(out, "database")?;
    sub(out, "backend", s.backend)?;
    sub(out, "mode", s.mode.as_str())?;
    sub(out, "block size", fmt_bytes(u64::from(s.block_size)))?;
    sub(out, "inline max", fmt_bytes(u64::from(s.inline_max)))?;
    writeln!(out, "entries")?;
    sub(out, "live records", s.records)?;
    sub(out, "tombstones", s.tombstones)?;
    sub(out, "objects", s.objects)?;
    sub(out, "hash candidates", s.hash_candidates)?;
    sub(out, "params", s.params)?;
    sub(out, "history entries", s.history_entries)?;
    sub(out, "sources", s.sources)?;
    sub(out, "pending imports", s.pending_imports)?;
    writeln!(out, "logical")?;
    sub(out, "live logical bytes", fmt_bytes(s.logical_bytes))?;
    writeln!(
        out,
        "engine payload by table (keys + values as stored; backend pages excluded)"
    )?;
    sub(out, "record keys", fmt_bytes(s.key_bytes))?;
    let inline = fmt_bytes(s.inline_envelope_bytes);
    sub(
        out,
        "manifests",
        format!(
            "{} (inline envelopes {inline})",
            fmt_bytes(s.manifest_bytes)
        ),
    )?;
    sub(
        out,
        "objects",
        format!(
            "{} (envelopes; each shared object once)",
            fmt_bytes(s.object_bytes)
        ),
    )?;
    sub(out, "hash candidates", fmt_bytes(s.candidate_bytes))?;
    sub(out, "refcounts", fmt_bytes(s.refcount_bytes))?;
    sub(
        out,
        "params",
        format!("{} (dictionaries and templates)", fmt_bytes(s.param_bytes)),
    )?;
    sub(out, "history", fmt_bytes(s.history_bytes))?;
    sub(out, "sources", fmt_bytes(s.source_bytes))?;
    sub(out, "total payload", fmt_bytes(s.payload_bytes()))?;
    writeln!(out, "per codec (units = objects + inline envelopes)")?;
    if s.per_codec.is_empty() {
        writeln!(out, "  (no stored units)")?;
    } else {
        writeln!(
            out,
            "  {:<18} {:>10} {:>16} {:>16} {:>16} {:>9}",
            "codec", "units", "raw bytes", "body bytes", "envelope bytes", "body/raw"
        )?;
        for c in &s.per_codec {
            writeln!(
                out,
                "  {:<18} {:>10} {:>16} {:>16} {:>16} {:>9}",
                c.codec,
                c.units,
                c.raw_bytes,
                c.body_bytes,
                c.envelope_bytes,
                fmt_ratio(c.body_bytes, c.raw_bytes)
            )?;
        }
    }
    writeln!(out, "files (ground truth of the space used)")?;
    if s.files.is_empty() {
        writeln!(out, "  (none: in-memory backend)")?;
    }
    for f in &s.files {
        writeln!(out, "  {}", f.path.display())?;
        sub(out, "  apparent", fmt_bytes(f.apparent_bytes))?;
        let allocated = f.allocated_bytes.map_or_else(
            || "n/a (not reported on this platform)".to_string(),
            fmt_bytes,
        );
        sub(out, "  allocated", allocated)?;
    }
    let payload = s.payload_bytes();
    let apparent = s.file_apparent_bytes();
    let allocated = s.file_allocated_bytes();
    let logical = s.logical_bytes;
    sub(out, "total apparent", fmt_bytes(apparent))?;
    sub(
        out,
        "total allocated",
        allocated.map_or_else(|| "n/a".to_string(), fmt_bytes),
    )?;
    let overhead = fmt_signed_bytes(i128::from(apparent) - i128::from(payload));
    sub(
        out,
        "backend overhead",
        format!("{overhead} = file apparent - engine payload (pages, tree, free space)"),
    )?;
    writeln!(
        out,
        "ratios (each with its components; logical = live logical bytes)"
    )?;
    sub(
        out,
        "payload/logical",
        format!(
            "{} = payload {payload} / logical {logical}",
            fmt_ratio(payload, logical)
        ),
    )?;
    let ratio = fmt_ratio(apparent, logical);
    sub(
        out,
        "apparent/logical",
        format!("{ratio} = file apparent {apparent} / logical {logical}"),
    )?;
    if let Some(a) = allocated {
        let ratio = fmt_ratio(a, logical);
        sub(
            out,
            "allocated/logical",
            format!("{ratio} = file allocated {a} / logical {logical}"),
        )?;
    }
    let c = &s.cache;
    writeln!(out, "block cache (this process)")?;
    sub(out, "capacity", fmt_bytes(c.capacity_bytes as u64))?;
    sub(
        out,
        "used",
        format!(
            "{} in {} entries",
            fmt_bytes(c.used_bytes as u64),
            c.entries
        ),
    )?;
    let hit_ratio = fmt_ratio(c.hits, c.hits.saturating_add(c.misses));
    sub(
        out,
        "hits / misses",
        format!("{} / {} (hit ratio {hit_ratio})", c.hits, c.misses),
    )?;
    sub(
        out,
        "insertions/evictions",
        format!("{} / {}", c.insertions, c.evictions),
    )?;
    writeln!(out, "planner (units encoded by this process)")?;
    for (codec, n) in &s.planner.chosen {
        sub(out, codec, n)?;
    }
    sub(out, "budget fallbacks", s.planner.budget_fallbacks)?;
    sub(out, "roundtrip failures", s.planner.roundtrip_failures)?;
    let k = &s.counters;
    writeln!(out, "engine counters (this process)")?;
    sub(
        out,
        "gets / puts / deletes",
        format!("{} / {} / {}", k.gets, k.puts, k.deletes),
    )?;
    sub(out, "commits", k.commits)?;
    sub(out, "bytes requested", fmt_bytes(k.bytes_requested))?;
    let amplification = fmt_ratio(k.bytes_reconstructed, k.bytes_requested);
    let reconstructed = fmt_bytes(k.bytes_reconstructed);
    sub(
        out,
        "bytes reconstructed",
        format!("{reconstructed} (read amplification {amplification})"),
    )?;
    sub(out, "units decoded", k.units_decoded)?;
    sub(out, "dedupe hits", k.dedupe_hits)?;
    sub(out, "objects written", k.objects_written)
}

pub fn render_verify(out: &mut dyn Write, r: &VerifyReport) -> io::Result<()> {
    let verdict = if r.ok() { "OK" } else { "PROBLEMS FOUND" };
    let depth = if r.deep {
        "deep: every object decoded and its digest checked"
    } else {
        "structural; --deep also decodes every object"
    };
    row(out, "verify", format!("{verdict} ({depth})"))?;
    sub(out, "records checked", r.records_checked)?;
    sub(out, "history checked", r.history_checked)?;
    sub(out, "objects checked", r.objects_checked)?;
    sub(out, "objects decoded", r.objects_decoded)?;
    sub(out, "missing objects", r.missing_objects)?;
    sub(out, "refcount mismatches", r.refcount_mismatches)?;
    sub(out, "digest failures", r.digest_failures)?;
    sub(out, "dangling candidates", r.dangling_candidates)?;
    sub(out, "missing params", r.missing_params)?;
    sub(
        out,
        "orphan objects",
        format!("{} (collectable by gc; not an error)", r.orphan_objects),
    )?;
    sub(
        out,
        "pending imports",
        format!("{} (collectable by gc; not an error)", r.pending_imports),
    )?;
    if !r.issues.is_empty() {
        writeln!(out, "issues (first ones found)")?;
        for issue in &r.issues {
            writeln!(out, "  - {issue}")?;
        }
    }
    Ok(())
}

pub fn render_gc(out: &mut dyn Write, r: &GcReport) -> io::Result<()> {
    writeln!(out, "gc")?;
    sub(out, "abandoned imports", r.abandoned_imports)?;
    sub(out, "objects removed", r.objects_removed)?;
    sub(out, "candidates removed", r.candidates_removed)?;
    sub(out, "params removed", r.params_removed)?;
    sub(out, "refcounts fixed", r.refcounts_fixed)
}

fn before_after(before: u64, after: u64) -> String {
    let delta = fmt_signed_bytes(i128::from(after) - i128::from(before));
    format!(
        "before {}, after {} ({delta})",
        fmt_bytes(before),
        fmt_bytes(after)
    )
}

pub fn render_compact(out: &mut dyn Write, r: &CompactReport) -> io::Result<()> {
    if r.supported {
        row(out, "compact", "done")?;
    } else {
        row(
            out,
            "compact",
            "not supported by this backend (nothing changed)",
        )?;
    }
    sub(
        out,
        "file apparent",
        before_after(r.apparent_before, r.apparent_after),
    )?;
    match (r.allocated_before, r.allocated_after) {
        (Some(b), Some(a)) => sub(out, "file allocated", before_after(b, a)),
        _ => sub(out, "file allocated", "n/a (not reported on this platform)"),
    }
}

pub fn render_train(
    out: &mut dyn Write,
    what: &str,
    r: &TrainReport,
    args: &TrainArgs,
) -> io::Result<()> {
    let verdict = match (r.installed, r.param_id) {
        (true, Some(id)) => format!("installed as param {id}"),
        (true, None) => "installed".to_string(),
        (false, _) if args.force => "not installed".to_string(),
        (false, _) => {
            "not installed (projected net gain not positive; --force installs anyway)".to_string()
        }
    };
    row(out, what, verdict)?;
    sub(out, "kind", &r.kind)?;
    sub(out, "parameter size", fmt_bytes(r.param_bytes as u64))?;
    let samples = format!(
        "{} for training, {} held out for validation",
        r.train_samples, r.validation_samples
    );
    sub(out, "samples", samples)?;
    let without = fmt_bytes(r.validation_bytes_without);
    let with = fmt_bytes(r.validation_bytes_with);
    sub(
        out,
        "validation bodies",
        format!("without {without}, with {with}"),
    )?;
    let gain = fmt_signed_bytes(i128::from(r.projected_net_gain));
    let uses = args.expected_uses;
    sub(
        out,
        "projected net gain",
        format!("{gain} over {uses} expected uses (parameter size and overhead included)"),
    )
}
