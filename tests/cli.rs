//! CLI, REPL, text helpers, wire protocol and TCP server.
//!
//! Tests that need a working engine (stubbed with `todo!()` until the engine
//! and store branches are merged) are `#[ignore = "after merge: needs engine"]`;
//! everything else runs now: parsing, quoting, reports, protocol framing,
//! the server's transport paths (PING, malformed frames, shutdown) and the
//! binary's usage errors, which must never touch the database.

use std::io::Cursor;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command as Process, Output, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use babeldb::cache::CacheStats;
use babeldb::cli::commands::{
    self, Command, GenSpec, HelpContext, Output as Sink, RecordSpace, TrainArgs, ValueSource,
};
use babeldb::cli::protocol::{
    self, MAX_FRAME_LEN, ProtocolError, Reply, Request, decode_frame, op, read_frame, write_frame,
};
use babeldb::cli::repl::{self, ReplOptions, parse_line, resolve_command};
use babeldb::cli::server::{self, Client, handle_request};
use babeldb::cli::text::{hex_encode, quote, tokenize};
use babeldb::cli::{self, GlobalOptions, parse_invocation};
use babeldb::config::Mode;
use babeldb::engine::{Inspection, UnitInfo};
use babeldb::format::{ImportInfo, SourceDescriptor};
use babeldb::maintenance::VerifyReport;
use babeldb::planner::PlannerSnapshot;
use babeldb::stats::{CodecUsage, EngineCountersSnapshot, FileSize, Stats};
use babeldb::{Config, Db, Expect, MemStore, ScanItem};
use proptest::prelude::*;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn args(words: &[&str]) -> Vec<String> {
    words.iter().map(|w| w.to_string()).collect()
}

fn parse(words: &[&str]) -> Result<Command, cli::UsageError> {
    commands::parse(&args(words))
}

fn parse_err(words: &[&str]) -> String {
    match parse(words) {
        Ok(c) => panic!("{words:?} parsed as {c:?}"),
        Err(e) => e.message,
    }
}

fn assert_err_contains(words: &[&str], needle: &str) {
    let msg = parse_err(words);
    assert!(
        msg.contains(needle),
        "{words:?}: '{msg}' does not contain '{needle}'"
    );
}

fn mem_db() -> Db<MemStore> {
    Db::with_store(MemStore::new(), Config::default()).expect("MemStore database")
}

fn bin() -> Process {
    Process::new(env!("CARGO_BIN_EXE_babeldb"))
}

fn run_bin(words: &[&str]) -> Output {
    bin()
        .args(words)
        .stdin(Stdio::null())
        .output()
        .expect("run babeldb")
}

fn run_bin_with_stdin(words: &[&str], input: &[u8]) -> Output {
    use std::io::Write;
    let mut child = bin()
        .args(words)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn babeldb");
    child
        .stdin
        .take()
        .expect("stdin")
        .write_all(input)
        .expect("write stdin");
    child.wait_with_output().expect("wait babeldb")
}

fn stderr(o: &Output) -> String {
    String::from_utf8_lossy(&o.stderr).into_owned()
}

fn stdout(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
}

fn path_str(p: &Path) -> &str {
    p.to_str().expect("utf-8 temp path")
}

/// A report line as `commands` lays it out: label column of 22.
fn report_row(label: &str, value: &str) -> String {
    format!("{label:<22} {value}")
}

/// An indented report line (two spaces, label column of 20).
fn report_sub(label: &str, value: &str) -> String {
    format!("  {label:<20} {value}")
}

// ---------------------------------------------------------------------------
// Parsing: every command, valid and invalid forms
// ---------------------------------------------------------------------------

#[test]
fn parse_put_variants() {
    let put = |value: ValueSource, expect: Expect| Command::Put {
        key: b"k".to_vec(),
        value,
        expect,
    };
    assert_eq!(
        parse(&["put", "k", "--value", "hello"]).unwrap(),
        put(ValueSource::Bytes(b"hello".to_vec()), Expect::Any)
    );
    assert_eq!(
        parse(&["put", "k", "--hex", "00ff0A"]).unwrap(),
        put(ValueSource::Bytes(vec![0, 255, 10]), Expect::Any)
    );
    assert_eq!(
        parse(&["put", "k", "--hex", ""]).unwrap(),
        put(ValueSource::Bytes(Vec::new()), Expect::Any)
    );
    assert_eq!(
        parse(&["put", "k", "--value", ""]).unwrap(),
        put(ValueSource::Bytes(Vec::new()), Expect::Any)
    );
    assert_eq!(
        parse(&["put", "k", "--file", "a.bin"]).unwrap(),
        put(ValueSource::File(PathBuf::from("a.bin")), Expect::Any)
    );
    assert_eq!(
        parse(&["put", "k"]).unwrap(),
        put(ValueSource::Stdin, Expect::Any)
    );
    assert_eq!(
        parse(&["put", "k", "--value=v", "--if-absent"]).unwrap(),
        put(ValueSource::Bytes(b"v".to_vec()), Expect::Absent)
    );
    assert_eq!(
        parse(&["put", "--if-revision", "7", "k", "--value", "v"]).unwrap(),
        put(ValueSource::Bytes(b"v".to_vec()), Expect::Revision(7))
    );
    // A value may start with '-'; `--` makes a key starting with '-' positional.
    assert_eq!(
        parse(&["put", "k", "--value", "-x"]).unwrap(),
        put(ValueSource::Bytes(b"-x".to_vec()), Expect::Any)
    );
    assert_eq!(
        parse(&["put", "--value", "v", "--", "-k"]).unwrap(),
        Command::Put {
            key: b"-k".to_vec(),
            value: ValueSource::Bytes(b"v".to_vec()),
            expect: Expect::Any
        }
    );
    // Values keep every byte (UTF-8 on the command line).
    assert_eq!(
        parse(&["put", "k", "--value", "a\u{e7}\u{e3}o\n"]).unwrap(),
        put(
            ValueSource::Bytes("a\u{e7}\u{e3}o\n".as_bytes().to_vec()),
            Expect::Any
        )
    );
}

#[test]
fn parse_put_errors() {
    assert_err_contains(&["put"], "missing <key>");
    assert_err_contains(&["put", "a", "b", "--value", "v"], "unexpected argument b");
    assert_err_contains(
        &["put", "k", "--value", "a", "--hex", "00"],
        "--value and --hex are mutually exclusive",
    );
    assert_err_contains(
        &["put", "k", "--value", "a", "--file", "f"],
        "mutually exclusive",
    );
    assert_err_contains(&["put", "k", "--hex", "0"], "odd number of hex digits");
    assert_err_contains(&["put", "k", "--hex", "zz"], "invalid hex digit");
    assert_err_contains(
        &[
            "put",
            "k",
            "--value",
            "v",
            "--if-absent",
            "--if-revision",
            "1",
        ],
        "--if-absent and --if-revision are mutually exclusive",
    );
    assert_err_contains(&["put", "k", "--value"], "option --value requires a value");
    assert_err_contains(
        &["put", "k", "--value", "a", "--value", "b"],
        "given more than once",
    );
    assert_err_contains(
        &["put", "k", "--value", "v", "--if-absent=1"],
        "does not take a value",
    );
    assert_err_contains(
        &["put", "k", "--value", "v", "--if-revision", "abc"],
        "not a valid unsigned integer",
    );
    assert_err_contains(&["put", "k", "--bogus"], "unknown option --bogus for 'put'");
    assert_err_contains(
        &["put", "k", "--value", "v", "--db", "x"],
        "global options go before the command name",
    );
    assert_err_contains(&["put", "k", "--file", ""], "--file must not be empty");
    let e = parse(&["put"]).unwrap_err();
    assert_eq!(e.command, Some("put"));
}

#[test]
fn parse_read_commands() {
    assert_eq!(
        parse(&["get", "k"]).unwrap(),
        Command::Get {
            key: b"k".to_vec(),
            output: Sink::Raw
        }
    );
    assert_eq!(
        parse(&["get", "k", "--hex"]).unwrap(),
        Command::Get {
            key: b"k".to_vec(),
            output: Sink::Hex
        }
    );
    assert_eq!(
        parse(&["get", "k", "--out", "o.bin"]).unwrap(),
        Command::Get {
            key: b"k".to_vec(),
            output: Sink::File(PathBuf::from("o.bin"))
        }
    );
    assert_err_contains(
        &["get", "k", "--hex", "--out", "o"],
        "--hex and --out are mutually exclusive",
    );
    assert_err_contains(&["get"], "missing <key>");
    assert_eq!(
        parse(&["range", "k", "0x10", "1_000", "--hex"]).unwrap(),
        Command::Range {
            key: b"k".to_vec(),
            offset: 16,
            len: 1000,
            output: Sink::Hex
        }
    );
    assert_err_contains(&["range", "k", "5"], "missing <len>");
    assert_err_contains(&["range", "k", "-1", "5"], "unknown option -1");
    assert_err_contains(&["range", "k", "x", "5"], "<offset>");
    assert_eq!(
        parse(&["head", "k"]).unwrap(),
        Command::Head { key: b"k".to_vec() }
    );
    assert_err_contains(&["head", "k", "extra"], "unexpected argument extra");
    assert_eq!(
        parse(&["inspect", "k"]).unwrap(),
        Command::Inspect { key: b"k".to_vec() }
    );
    assert_eq!(
        parse(&["history", "k"]).unwrap(),
        Command::History { key: b"k".to_vec() }
    );
    assert_eq!(
        parse(&["get-at", "k", "5", "--hex"]).unwrap(),
        Command::GetAt {
            key: b"k".to_vec(),
            revision: 5,
            output: Sink::Hex
        }
    );
    assert_err_contains(&["get-at", "k", "five"], "<revision>");
    assert_eq!(
        parse(&["scan"]).unwrap(),
        Command::Scan {
            prefix: None,
            limit: 0,
            reverse: false,
            values: false
        }
    );
    assert_eq!(
        parse(&[
            "scan",
            "--prefix",
            "user/",
            "--limit",
            "10",
            "--reverse",
            "--values"
        ])
        .unwrap(),
        Command::Scan {
            prefix: Some(b"user/".to_vec()),
            limit: 10,
            reverse: true,
            values: true
        }
    );
    assert_err_contains(&["scan", "--limit", "x"], "--limit");
    assert_err_contains(&["scan", "user/"], "unexpected argument user/");
    assert_eq!(parse(&["sources"]).unwrap(), Command::Sources);
    assert_eq!(parse(&["stats"]).unwrap(), Command::Stats);
    assert_err_contains(&["stats", "--verbose"], "'stats' takes no options");
    assert_err_contains(&["stats", "x"], "unexpected argument x");
}

#[test]
fn parse_write_and_maintenance_commands() {
    assert_eq!(
        parse(&["delete", "k"]).unwrap(),
        Command::Delete {
            key: b"k".to_vec(),
            expect: Expect::Any
        }
    );
    assert_eq!(
        parse(&["delete", "k", "--if-revision", "3"]).unwrap(),
        Command::Delete {
            key: b"k".to_vec(),
            expect: Expect::Revision(3)
        }
    );
    assert_err_contains(
        &["delete", "k", "--if-absent"],
        "unknown option --if-absent",
    );
    assert_eq!(
        parse(&["import", "k", "data.bin", "--if-absent"]).unwrap(),
        Command::Import {
            key: b"k".to_vec(),
            path: PathBuf::from("data.bin"),
            expect: Expect::Absent
        }
    );
    assert_err_contains(&["import", "k"], "missing <path>");
    assert_eq!(parse(&["verify"]).unwrap(), Command::Verify { deep: false });
    assert_eq!(
        parse(&["verify", "--deep"]).unwrap(),
        Command::Verify { deep: true }
    );
    assert_eq!(parse(&["gc"]).unwrap(), Command::Gc);
    assert_eq!(parse(&["compact"]).unwrap(), Command::Compact);
    assert_eq!(
        parse(&["prune-history", "--keep", "2"]).unwrap(),
        Command::PruneHistory { key: None, keep: 2 }
    );
    assert_eq!(
        parse(&["prune-history", "--key", "k", "--keep", "0"]).unwrap(),
        Command::PruneHistory {
            key: Some(b"k".to_vec()),
            keep: 0
        }
    );
    assert_err_contains(&["prune-history"], "missing --keep <N>");
    assert_eq!(parse(&["repl"]).unwrap(), Command::Repl);
    assert_eq!(parse(&["version"]).unwrap(), Command::Version);
    assert_eq!(parse(&["quit"]).unwrap(), Command::Quit);
    assert_eq!(parse(&["exit"]).unwrap(), Command::Quit);
}

#[test]
fn parse_gen() {
    let generated = |spec: GenSpec, expect: Expect| Command::Gen {
        key: b"g".to_vec(),
        spec,
        expect,
    };
    assert_eq!(
        parse(&["gen", "g", "arith", "1", "2", "3"]).unwrap(),
        generated(
            GenSpec::Arith {
                start: 1,
                step: 2,
                count: 3
            },
            Expect::Any
        )
    );
    let key_hex = "ab".repeat(32);
    assert_eq!(
        parse(&["gen", "g", "xof", &key_hex, "4096", "--if-absent"]).unwrap(),
        generated(
            GenSpec::Xof {
                key: [0xab; 32],
                len: 4096
            },
            Expect::Absent
        )
    );
    assert_eq!(
        parse(&["gen", "g", "repeat", "10", "ab"]).unwrap(),
        generated(
            GenSpec::Repeat {
                total: 10,
                motif: b"ab".to_vec()
            },
            Expect::Any
        )
    );
    assert_eq!(
        parse(&["gen", "g", "repeat", "0", ""]).unwrap(),
        generated(
            GenSpec::Repeat {
                total: 0,
                motif: Vec::new()
            },
            Expect::Any
        )
    );
    assert_err_contains(
        &["gen", "g", "repeat", "5", ""],
        "<motif> must not be empty",
    );
    assert_err_contains(
        &["gen", "g", "xof", &"ab".repeat(31), "10"],
        "must be 32 bytes",
    );
    assert_err_contains(&["gen", "g", "xof", "zz", "10"], "<key-hex>");
    assert_err_contains(&["gen", "g", "arith", "1", "2"], "missing <count>");
    assert_err_contains(
        &["gen", "g", "arith", "1", "2", "3", "4"],
        "unexpected argument 4",
    );
    assert_err_contains(&["gen", "g", "bogus"], "unknown generator kind bogus");
    assert_err_contains(&["gen", "g"], "missing generator kind");
    assert_err_contains(&["gen"], "missing <key>");
    // Parameters are exactly the documented layouts.
    let spec = GenSpec::Arith {
        start: 1,
        step: 2,
        count: 3,
    };
    assert_eq!(spec.params().len(), 24);
    assert_eq!(spec.generator_id(), babeldb::generator::ids::ARITH_U64);
    let spec = GenSpec::Repeat {
        total: 7,
        motif: b"xy".to_vec(),
    };
    assert_eq!(spec.params(), [&7u64.to_le_bytes()[..], b"xy"].concat());
}

#[test]
fn parse_train_serve_help() {
    let defaults = babeldb::planner::TrainOptions::default();
    assert_eq!(
        parse(&["train-dict", "--prefix", "chat/"]).unwrap(),
        Command::TrainDict(TrainArgs {
            prefix: b"chat/".to_vec(),
            limit: commands::DEFAULT_TRAIN_LIMIT,
            expected_uses: defaults.expected_uses,
            validation_fraction: defaults.validation_fraction,
            max_bytes: defaults.max_dict_bytes,
            force: false,
        })
    );
    let parsed = parse(&[
        "train-template",
        "--prefix",
        "",
        "--limit",
        "5",
        "--expected-uses",
        "100",
        "--validation-fraction",
        "0.5",
        "--max-bytes",
        "16KiB",
        "--force",
    ])
    .unwrap();
    let Command::TrainTemplate(a) = parsed else {
        panic!("not train-template")
    };
    assert_eq!(
        (
            a.prefix.as_slice(),
            a.limit,
            a.expected_uses,
            a.max_bytes,
            a.force
        ),
        (&b""[..], 5, 100, 16384, true)
    );
    assert!((a.validation_fraction - 0.5).abs() < 1e-12);
    assert!(!a.options().require_gain);
    assert_err_contains(&["train-dict"], "missing --prefix <p>");
    assert_err_contains(
        &[
            "train-dict",
            "--prefix",
            "p",
            "--validation-fraction",
            "1.5",
        ],
        "strictly between 0 and 1",
    );
    assert_err_contains(
        &["train-dict", "--prefix", "p", "--validation-fraction", "0"],
        "strictly between 0 and 1",
    );
    assert_err_contains(
        &["train-dict", "--prefix", "p", "--max-bytes", "12kb"],
        "--max-bytes",
    );

    assert_eq!(
        parse(&["serve"]).unwrap(),
        Command::Serve {
            addr: commands::DEFAULT_ADDR.to_string(),
            threads: None
        }
    );
    assert_eq!(
        parse(&["serve", "--addr", "0.0.0.0:9000", "--threads", "8"]).unwrap(),
        Command::Serve {
            addr: "0.0.0.0:9000".to_string(),
            threads: Some(8)
        }
    );
    assert_err_contains(&["serve", "--threads", "0"], "between 1 and 1024");
    assert_err_contains(&["serve", "--threads", "5000"], "between 1 and 1024");
    assert_err_contains(&["serve", "--addr", ""], "--addr must not be empty");

    assert_eq!(parse(&["help"]).unwrap(), Command::Help { topic: None });
    assert_eq!(
        parse(&["help", "put"]).unwrap(),
        Command::Help { topic: Some("put") }
    );
    assert_eq!(
        parse(&["help", "exit"]).unwrap(),
        Command::Help {
            topic: Some("quit")
        }
    );
    assert_err_contains(&["help", "bogus"], "unknown command bogus");
    assert_err_contains(&["help", "a", "b"], "unexpected argument b");
}

#[test]
fn unknown_commands_get_suggestions() {
    assert_err_contains(&[], "missing command");
    assert_err_contains(&["ins", "k"], "did you mean 'inspect'?");
    assert_err_contains(&["g"], "did you mean one of: gc, gen, get, get-at?");
    assert_err_contains(&["bogus"], "run 'help' for the list of commands");
    // One-shot mode never expands prefixes (scripts must not change meaning).
    assert!(parse(&["ins", "k"]).is_err());
}

#[test]
fn every_command_has_help_and_a_parser() {
    for h in commands::COMMANDS {
        assert_eq!(commands::canonical_name(h.name), Some(h.name));
        assert!(h.usage.starts_with(h.name), "{} usage: {}", h.name, h.usage);
        let mut text = Vec::new();
        commands::write_help(&mut text, Some(h.name), HelpContext::Cli).unwrap();
        let text = String::from_utf8(text).unwrap();
        assert!(text.contains(h.usage), "{text}");
        // Parsing the bare name reaches the command's own parser (no "unknown command").
        if let Err(e) = parse(&[h.name]) {
            assert_eq!(e.command, Some(h.name), "{}: {}", h.name, e.message);
        }
    }
    let mut general = Vec::new();
    commands::write_help(&mut general, None, HelpContext::Cli).unwrap();
    let general = String::from_utf8(general).unwrap();
    assert!(general.contains("exit status"));
    assert!(general.contains("train-template"));
}

// ---------------------------------------------------------------------------
// Global options and configuration
// ---------------------------------------------------------------------------

#[test]
fn invocation_global_options() {
    let inv = parse_invocation(&args(&[
        "--db",
        "x.redb",
        "--mode",
        "babel-pure",
        "--block-size",
        "4KiB",
        "--inline-max=512",
        "--history",
        "put",
        "k",
        "--value",
        "v",
    ]))
    .unwrap();
    assert_eq!(
        inv.globals,
        GlobalOptions {
            db: Some(PathBuf::from("x.redb")),
            mode: Some(Mode::BabelPure),
            block_size: Some(4096),
            inline_max: Some(512),
            history: true,
        }
    );
    assert_eq!(inv.command.name(), "put");
    let cfg = cli::config_for(&inv.globals).unwrap();
    assert_eq!(
        (cfg.mode, cfg.block_size, cfg.inline_max, cfg.keep_history),
        (Mode::BabelPure, 4096, 512, true)
    );

    let inv = parse_invocation(&args(&["--db=y", "get", "k"])).unwrap();
    assert_eq!(inv.globals.db, Some(PathBuf::from("y")));
    assert_eq!(
        parse_invocation(&args(&["--help"])).unwrap().command,
        Command::Help { topic: None }
    );
    assert_eq!(
        parse_invocation(&args(&["-V"])).unwrap().command,
        Command::Version
    );

    let err = |words: &[&str]| parse_invocation(&args(words)).unwrap_err().message;
    assert!(err(&["--mode", "fast", "stats"]).contains("unknown mode fast"));
    assert!(err(&["--db"]).contains("--db requires a value"));
    assert!(err(&["--db", "a", "--db", "b", "stats"]).contains("given more than once"));
    assert!(err(&["--hex", "get", "k"]).contains("unknown global option --hex"));
    assert!(err(&["--history=1", "stats"]).contains("does not take a value"));
    assert!(err(&["--block-size", "huge", "stats"]).contains("--block-size"));
    assert!(err(&["--block-size", "8GiB", "stats"]).contains("too large"));
    assert!(err(&[]).contains("missing command"));
    assert!(err(&["--db", "x"]).contains("missing command"));
}

#[test]
fn creation_parameters_are_validated_before_opening() {
    let config = |g: GlobalOptions| cli::config_for(&g);
    assert!(config(GlobalOptions::default()).is_ok());
    let e = config(GlobalOptions {
        block_size: Some(100),
        ..GlobalOptions::default()
    })
    .unwrap_err();
    assert!(
        e.message.contains("invalid creation parameters"),
        "{}",
        e.message
    );
    let e = config(GlobalOptions {
        inline_max: Some(20_000),
        ..GlobalOptions::default()
    })
    .unwrap_err();
    assert!(e.message.contains("inline_max"), "{}", e.message);
    let ok = config(GlobalOptions {
        block_size: Some(65536),
        inline_max: Some(20_000),
        ..GlobalOptions::default()
    });
    assert!(ok.is_ok());
}

// ---------------------------------------------------------------------------
// Tokenizer and quoting
// ---------------------------------------------------------------------------

fn toks(line: &str) -> Vec<Vec<u8>> {
    tokenize(line.as_bytes()).unwrap()
}

#[test]
fn tokenizer_quotes_and_escapes() {
    assert_eq!(
        toks("put  k\t--value v"),
        vec![
            b"put".to_vec(),
            b"k".to_vec(),
            b"--value".to_vec(),
            b"v".to_vec()
        ]
    );
    assert_eq!(toks(r#"put "a b" --value "x\ty""#)[1], b"a b");
    assert_eq!(toks(r#"put "a b" --value "x\ty""#)[3], b"x\ty");
    assert_eq!(toks(r#"a"b c"d"#), vec![b"ab cd".to_vec()]);
    assert_eq!(
        toks(r#"put "" x"#),
        vec![b"put".to_vec(), Vec::new(), b"x".to_vec()]
    );
    assert_eq!(toks(r"\x00\xff\xAB"), vec![vec![0, 0xff, 0xab]]);
    assert_eq!(
        toks(r#"\"q\" \\ a\ b \n\r\0"#),
        vec![
            b"\"q\"".to_vec(),
            b"\\".to_vec(),
            b"a b".to_vec(),
            b"\n\r\0".to_vec()
        ]
    );
    assert_eq!(toks("   "), Vec::<Vec<u8>>::new());
    assert_eq!(toks("x\r\n"), vec![b"x".to_vec()]);
    // Bytes that are not UTF-8 pass through unchanged.
    assert_eq!(tokenize(b"a\xffb").unwrap(), vec![b"a\xffb".to_vec()]);

    let e = tokenize(br#"get "unterminated"#).unwrap_err();
    assert_eq!(e.column, 5);
    assert!(e.message.contains("unterminated double quote"));
    // `\n` is a valid escape, so `C:\new\file` fails at `\f` instead of being
    // stored with a newline.
    let e = tokenize(br"import k C:\new\file").unwrap_err();
    assert!(e.message.contains("unknown escape \\f"), "{}", e.message);
    let e = tokenize(br"a \q").unwrap_err();
    assert_eq!(e.column, 3);
    assert!(e.message.contains("unknown escape \\q"));
    assert!(
        tokenize(br"\x4")
            .unwrap_err()
            .message
            .contains("two hex digits")
    );
    assert!(
        tokenize(br"\xg0")
            .unwrap_err()
            .message
            .contains("two hex digits")
    );
    assert!(
        tokenize(br"abc\")
            .unwrap_err()
            .message
            .contains("dangling backslash")
    );
}

#[test]
fn quote_examples() {
    assert_eq!(quote(b"user/42"), "user/42");
    assert_eq!(quote("a\u{e7}\u{e3}o".as_bytes()), "a\u{e7}\u{e3}o");
    assert_eq!(quote(b""), "\"\"");
    assert_eq!(quote(b"a b"), "\"a b\"");
    assert_eq!(quote(&[0, 255]), r#""\x00\xff""#);
    assert_eq!(quote(b"say \"hi\"\n"), r#""say \"hi\"\n""#);
    assert_eq!(quote(b"#tag"), "\"#tag\"");
    assert_eq!(quote(b"C:\\x"), r#""C:\\x""#);
    // Invisible and reordering characters are always escaped.
    assert_eq!(quote("\u{202E}x".as_bytes()), r#""\xe2\x80\xaex""#);
    assert_eq!(quote("\u{a0}".as_bytes()), r#""\xc2\xa0""#);
}

fn special_bytes() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(
        prop_oneof![
            any::<u8>(),
            Just(b'"'),
            Just(b'\\'),
            Just(b' '),
            Just(b'#'),
            Just(b'\t'),
            Just(b'\n'),
            Just(b'x'),
        ],
        0..48,
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn quote_is_the_inverse_of_tokenize(bytes in special_bytes()) {
        let q = quote(&bytes);
        prop_assert_eq!(tokenize(q.as_bytes()).unwrap(), vec![bytes]);
    }

    #[test]
    fn quote_roundtrips_unicode(s in any::<String>()) {
        let q = quote(s.as_bytes());
        prop_assert_eq!(tokenize(q.as_bytes()).unwrap(), vec![s.into_bytes()]);
    }

    #[test]
    fn quoted_lines_split_back_into_their_tokens(tokens in proptest::collection::vec(special_bytes(), 0..6)) {
        let line: Vec<String> = tokens.iter().map(|t| quote(t)).collect();
        prop_assert_eq!(tokenize(line.join(" ").as_bytes()).unwrap(), tokens);
    }

    #[test]
    fn tokenize_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
        let _ = tokenize(&bytes);
        let _ = parse_line(&bytes);
    }
}

// ---------------------------------------------------------------------------
// REPL: prefix matching, line parsing, sessions
// ---------------------------------------------------------------------------

#[test]
fn prefix_matching() {
    assert_eq!(resolve_command(b"ins").unwrap(), "inspect");
    assert_eq!(resolve_command(b"inspect").unwrap(), "inspect");
    assert_eq!(resolve_command(b"get").unwrap(), "get"); // exact beats get-at
    assert_eq!(resolve_command(b"get-").unwrap(), "get-at");
    assert_eq!(resolve_command(b"st").unwrap(), "stats");
    assert_eq!(resolve_command(b"q").unwrap(), "quit");
    assert_eq!(resolve_command(b"ex").unwrap(), "quit");
    assert_eq!(resolve_command(b"train-t").unwrap(), "train-template");
    let e = resolve_command(b"g").unwrap_err();
    assert_eq!(
        e.message,
        "ambiguous command 'g': could be gc, gen, get, get-at"
    );
    let e = resolve_command(b"s").unwrap_err();
    assert!(
        e.message.contains("scan, serve, sources, stats"),
        "{}",
        e.message
    );
    assert!(
        resolve_command(b"ver")
            .unwrap_err()
            .message
            .contains("verify, version")
    );
    assert!(
        resolve_command(b"zzz")
            .unwrap_err()
            .message
            .contains("unknown command zzz")
    );
    assert!(resolve_command(b"").is_err());
    assert!(resolve_command(b"\xff").is_err());
}

#[test]
fn repl_line_parsing() {
    assert_eq!(parse_line(b"   # comment").unwrap(), None);
    assert_eq!(parse_line(b"").unwrap(), None);
    assert_eq!(parse_line(b" \t\r\n").unwrap(), None);
    assert_eq!(
        parse_line(b"ins k").unwrap(),
        Some(Command::Inspect { key: b"k".to_vec() })
    );
    assert_eq!(
        parse_line(b"help ins").unwrap(),
        Some(Command::Help {
            topic: Some("inspect")
        })
    );
    assert!(
        parse_line(b"help s")
            .unwrap_err()
            .message
            .contains("ambiguous")
    );
    assert_eq!(
        parse_line(br#"pu "a b" --value "\x00\"x""#).unwrap(),
        Some(Command::Put {
            key: b"a b".to_vec(),
            value: ValueSource::Bytes(b"\0\"x".to_vec()),
            expect: Expect::Any
        })
    );
    // Keys may hold any byte in the REPL.
    assert_eq!(
        parse_line(br"get \xff\x00").unwrap(),
        Some(Command::Get {
            key: vec![0xff, 0],
            output: Sink::Raw
        })
    );
    assert!(
        parse_line(b"put k")
            .unwrap_err()
            .message
            .contains("--value, --hex or --file")
    );
    assert!(
        parse_line(b"repl")
            .unwrap_err()
            .message
            .contains("already in the REPL")
    );
    assert!(
        parse_line(b"serve")
            .unwrap_err()
            .message
            .contains("top-level command")
    );
    let e = parse_line(br#"get "k"#).unwrap_err();
    assert!(
        e.message.contains("syntax error at column 5"),
        "{}",
        e.message
    );
}

fn run_session(db: &mut Db<MemStore>, script: &str) -> (String, String) {
    let mut input = Cursor::new(script.as_bytes().to_vec());
    let (mut out, mut err) = (Vec::new(), Vec::new());
    repl::run(db, &mut input, &mut out, &mut err, &ReplOptions::default()).unwrap();
    (
        String::from_utf8(out).unwrap(),
        String::from_utf8(err).unwrap(),
    )
}

#[test]
fn repl_errors_never_end_the_session() {
    let mut db = mem_db();
    let script = "# comment\n\nversion\nhelp\nhelp ins\nbogus\nins\n\"unterminated\nput k\ng\nget k\nversion\nquit\nversion\n";
    let (out, err) = run_session(&mut db, script);
    // `version` ran before and after the errors, but not after `quit`.
    assert_eq!(out.matches("wire protocol version").count(), 2, "{out}");
    assert!(out.contains("commands (any unique prefix works"), "{out}");
    assert!(out.contains("usage: inspect <key>"), "{out}");
    assert!(err.contains("unknown command bogus"), "{err}");
    assert!(
        err.contains("error: missing <key>\nusage: inspect <key>"),
        "{err}"
    );
    assert!(
        err.contains("syntax error at column 1: unterminated double quote"),
        "{err}"
    );
    assert!(err.contains("--value, --hex or --file"), "{err}");
    assert!(err.contains("ambiguous command 'g'"), "{err}");
    // Before the engine is merged `get` panics inside the engine (the session
    // survives it); afterwards the key is simply not found.
    assert!(
        err.contains("not found: k") || err.contains("internal error"),
        "{err}"
    );
}

#[test]
fn repl_ends_at_end_of_input_and_prints_prompts() {
    let mut db = mem_db();
    let mut input = Cursor::new(b"version\n".to_vec());
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let opts = ReplOptions {
        prompt: Some("db> ".into()),
        banner: Some("hello".into()),
    };
    repl::run(&mut db, &mut input, &mut out, &mut err, &opts).unwrap();
    let out = String::from_utf8(out).unwrap();
    assert!(out.starts_with("hello\ndb> babeldb "), "{out}");
    assert!(out.ends_with("db> \n"), "{out}");
    assert!(err.is_empty());
}

// ---------------------------------------------------------------------------
// Reports
// ---------------------------------------------------------------------------

fn unit(
    object_id: Option<u64>,
    codec: &str,
    raw_len: u32,
    body_len: u32,
    refcount: Option<u64>,
) -> UnitInfo {
    UnitInfo {
        object_id,
        codec: codec.to_string(),
        raw_len,
        body_len,
        aux_id: 0,
        refcount,
    }
}

fn inspection(
    key: &[u8],
    kind: &'static str,
    logical_len: u64,
    manifest_bytes: u64,
    units: Vec<UnitInfo>,
) -> Inspection {
    let encoded_bytes = units.iter().map(|u| 64 + u64::from(u.body_len)).sum();
    Inspection {
        key: key.to_vec(),
        revision: 7,
        logical_len,
        kind,
        manifest_bytes,
        encoded_bytes,
        units,
        source: None,
        generator: None,
    }
}

fn render_inspection(i: &Inspection) -> String {
    let mut out = Vec::new();
    commands::render_inspection(&mut out, i).unwrap();
    String::from_utf8(out).unwrap()
}

#[test]
fn inspect_shows_the_ratio_only_with_its_components() {
    let mut i = inspection(
        b"k1",
        "chunks",
        17_384,
        60,
        vec![
            unit(Some(1), "ZstdV1", 16_384, 100, Some(2)),
            unit(Some(2), "RawV1", 1_000, 1_000, Some(1)),
        ],
    );
    i.source = Some((
        3,
        SourceDescriptor {
            kind: babeldb::format::source_kind::LOCAL_FILE,
            location: "C:\\data\\x.bin".into(),
            adapter_version: 1,
            last_import: Some(ImportInfo {
                revision: 7,
                bytes: 17_384,
                digest: [0xab; 32],
                unix_ms: 1_000_000_000_123,
            }),
        },
    ));
    let space = RecordSpace::of(&i);
    assert_eq!(space.object_envelopes, 164 + 1_064);
    assert_eq!(space.stored(), 2 + 60 + 1_228);
    assert_eq!(space.metadata(), 2 + 60 + 128);
    assert_eq!(space.bodies, 1_100);
    assert_eq!(space.metadata() + space.bodies, space.stored());

    let text = render_inspection(&i);
    assert!(
        !text.contains('%'),
        "a percentage leaked into inspect:\n{text}"
    );
    let ratio_line = text
        .lines()
        .find(|l| l.contains("stored/logical"))
        .expect("ratio line");
    assert!(
        ratio_line
            .contains("0.0742 = stored total 1290 / logical 17384 (metadata 190 + bodies 1100)"),
        "{ratio_line}"
    );
    let kind = report_row("kind", "chunks");
    for needle in [
        kind.as_str(),
        "metadata",
        "envelope headers 128 (2 x 64 B)",
        "object 1",
        "ZstdV1",
        "local-file",
        "2001-09-09T01:46:40.123Z",
    ] {
        assert!(text.contains(needle), "missing '{needle}' in:\n{text}");
    }
}

#[test]
fn inspect_counts_inline_envelopes_once() {
    let i = inspection(
        b"k",
        "inline",
        15,
        100,
        vec![unit(None, "RawV1", 15, 15, None)],
    );
    let space = RecordSpace::of(&i);
    assert_eq!(space.inline_envelopes, 79);
    assert_eq!(space.stored(), 101);
    assert_eq!(space.metadata(), 1 + 21 + 64);
    assert_eq!(space.metadata() + space.bodies, space.stored());
    let text = render_inspection(&i);
    assert!(
        text.contains("6.7333 = stored total 101 / logical 15 (metadata 86 + bodies 15)"),
        "{text}"
    );
    assert!(text.contains("inline"));

    let empty = inspection(b"e", "inline", 0, 80, vec![unit(None, "RawV1", 0, 0, None)]);
    let text = render_inspection(&empty);
    assert!(text.contains("n/a (logical length 0"), "{text}");
    assert!(!text.contains('%'));

    let mut generated = inspection(b"g", "generated", 8_000, 70, Vec::new());
    generated.generator = Some((1, 1, "arith-u64".into(), 24));
    let text = render_inspection(&generated);
    assert!(
        text.contains("arith-u64 (id 1, version 1); params 24 B inside the manifest"),
        "{text}"
    );
    assert!(
        text.contains("= stored total 71 / logical 8000 (key 1 + manifest 70 holding the generator parameters)"),
        "{text}"
    );
}

fn sample_stats() -> Stats {
    Stats {
        backend: "redb",
        mode: Mode::Adaptive,
        block_size: 16_384,
        inline_max: 1_024,
        records: 10,
        tombstones: 1,
        objects: 25,
        hash_candidates: 25,
        params: 0,
        history_entries: 2,
        sources: 1,
        pending_imports: 0,
        logical_bytes: 409_600,
        key_bytes: 70,
        manifest_bytes: 660,
        inline_envelope_bytes: 0,
        object_bytes: 411_200,
        candidate_bytes: 1_100,
        refcount_bytes: 400,
        param_bytes: 0,
        history_bytes: 132,
        source_bytes: 60,
        meta_bytes: 180,
        pending_import_bytes: 0,
        per_codec: vec![CodecUsage {
            codec: "RawV1".into(),
            units: 25,
            raw_bytes: 409_600,
            body_bytes: 409_600,
            envelope_bytes: 411_200,
        }],
        files: vec![FileSize {
            path: PathBuf::from("db.redb"),
            apparent_bytes: 1_048_576,
            allocated_bytes: Some(1_052_672),
        }],
        cache: CacheStats {
            capacity_bytes: 64 << 20,
            used_bytes: 0,
            entries: 0,
            hits: 3,
            misses: 1,
            insertions: 1,
            evictions: 0,
        },
        planner: PlannerSnapshot {
            chosen: vec![("RawV1".into(), 25)],
            budget_fallbacks: 0,
            roundtrip_failures: 0,
        },
        counters: EngineCountersSnapshot::default(),
    }
}

#[test]
fn stats_report_is_complete() {
    let s = sample_stats();
    assert_eq!(s.payload_bytes(), 413_622);
    let mut out = Vec::new();
    commands::render_stats(&mut out, &s).unwrap();
    let text = String::from_utf8(out).unwrap();
    for needle in [
        report_sub("total payload", "413622 B (403.9 KiB)"),
        report_sub("  apparent", "1048576 B (1.0 MiB)"),
        report_sub("  allocated", "1052672 B (1.0 MiB)"),
        report_sub("backend overhead", "+634954 B (620.1 KiB)"),
        report_sub(
            "allocated/logical",
            "2.5700 = file allocated 1052672 / logical 409600",
        ),
        "hit ratio 0.7500".to_string(),
        "RawV1".to_string(),
        "hash candidates".to_string(),
        "engine counters (this process)".to_string(),
    ] {
        assert!(text.contains(&needle), "missing '{needle}' in:\n{text}");
    }
    assert!(!text.contains('%'));

    let mut no_alloc = sample_stats();
    no_alloc.files[0].allocated_bytes = None;
    let mut out = Vec::new();
    commands::render_stats(&mut out, &no_alloc).unwrap();
    let text = String::from_utf8(out).unwrap();
    assert!(
        text.contains("n/a (not reported on this platform)"),
        "{text}"
    );
    assert!(!text.contains("allocated/logical"));
}

#[test]
fn verify_report() {
    let render = |r: &VerifyReport| {
        let mut out = Vec::new();
        commands::render_verify(&mut out, r).unwrap();
        String::from_utf8(out).unwrap()
    };
    let ok = VerifyReport {
        deep: true,
        records_checked: 3,
        ..VerifyReport::default()
    };
    assert!(render(&ok).starts_with(&report_row("verify", "OK (deep")));
    let bad = VerifyReport {
        missing_objects: 1,
        issues: vec!["object 5 missing".into()],
        ..VerifyReport::default()
    };
    let text = render(&bad);
    assert!(text.contains("PROBLEMS FOUND"));
    assert!(text.contains("  - object 5 missing"));
}

// ---------------------------------------------------------------------------
// Wire protocol
// ---------------------------------------------------------------------------

fn roundtrip_request(request: &Request<'_>) {
    let mut frame = Vec::new();
    request.encode(&mut frame).unwrap();
    let (body, used) = decode_frame(&frame).unwrap();
    assert_eq!(used, frame.len());
    assert_eq!(&Request::decode(body).unwrap(), request);
}

fn roundtrip_reply(op: u8, reply: &Reply) {
    let mut frame = Vec::new();
    reply.encode(&mut frame).unwrap();
    let (body, used) = decode_frame(&frame).unwrap();
    assert_eq!(used, frame.len());
    assert_eq!(&Reply::decode(op, body).unwrap(), reply);
}

#[test]
fn requests_roundtrip() {
    let binary: Vec<u8> = (0..=255u8).collect();
    roundtrip_request(&Request::Ping);
    roundtrip_request(&Request::Get { key: b"user/1" });
    roundtrip_request(&Request::Get { key: b"" });
    roundtrip_request(&Request::Put {
        key: b"k",
        value: &binary,
    });
    roundtrip_request(&Request::Delete { key: &binary });
    roundtrip_request(&Request::Range {
        key: b"k",
        offset: u64::MAX,
        len: 7,
    });
    roundtrip_request(&Request::ScanPrefix {
        prefix: b"p",
        limit: 0,
        reverse: true,
        with_values: false,
    });
    roundtrip_request(&Request::ScanPrefix {
        prefix: b"",
        limit: u32::MAX,
        reverse: false,
        with_values: true,
    });
    roundtrip_request(&Request::PutBatch { items: vec![] });
    roundtrip_request(&Request::PutBatch {
        items: vec![(&b"a"[..], &b"1"[..]), (&b""[..], &binary[..])],
    });
    // Exact layout of one request: len | op | key.
    let mut frame = Vec::new();
    Request::Get { key: b"ab" }.encode(&mut frame).unwrap();
    assert_eq!(frame, [7, 0, 0, 0, op::GET, 2, 0, 0, 0, b'a', b'b']);
}

#[test]
fn replies_roundtrip() {
    let item = |k: &[u8], v: Option<&[u8]>| ScanItem {
        key: k.to_vec(),
        revision: 9,
        logical_len: v.map_or(3, |v| v.len() as u64),
        value: v.map(<[u8]>::to_vec),
    };
    roundtrip_reply(op::GET, &Reply::Value(b"hello".to_vec()));
    roundtrip_reply(op::GET, &Reply::NotFound);
    roundtrip_reply(op::RANGE, &Reply::Value(Vec::new()));
    roundtrip_reply(op::PUT, &Reply::Revision(u64::MAX));
    roundtrip_reply(op::DELETE, &Reply::Done);
    roundtrip_reply(op::DELETE, &Reply::NotFound);
    roundtrip_reply(op::PING, &Reply::Done);
    roundtrip_reply(op::PUT_BATCH, &Reply::Revisions(vec![1, 2, 3]));
    roundtrip_reply(op::PUT_BATCH, &Reply::Revisions(vec![]));
    roundtrip_reply(op::SCAN_PREFIX, &Reply::Items(vec![]));
    roundtrip_reply(
        op::SCAN_PREFIX,
        &Reply::Items(vec![
            item(b"a", None),
            item(b"b", Some(b"\0\xff")),
            item(b"", Some(b"")),
        ]),
    );
    roundtrip_reply(op::GET, &Reply::Error("boom: \u{e7}".into()));
    // Long error messages are cut at a character boundary.
    let mut frame = Vec::new();
    Reply::Error("\u{e9}".repeat(5000))
        .encode(&mut frame)
        .unwrap();
    let (body, _) = decode_frame(&frame).unwrap();
    let Reply::Error(m) = Reply::decode(op::GET, body).unwrap() else {
        panic!("not an error")
    };
    assert!(m.len() <= protocol::MAX_ERROR_MESSAGE && m.chars().all(|c| c == '\u{e9}'));
}

#[test]
fn malformed_frames_are_errors() {
    assert!(matches!(decode_frame(&[]), Err(ProtocolError::Truncated)));
    assert!(matches!(
        decode_frame(&[1, 0]),
        Err(ProtocolError::Truncated)
    ));
    assert!(matches!(
        decode_frame(&0u32.to_le_bytes()),
        Err(ProtocolError::EmptyFrame)
    ));
    assert!(matches!(
        decode_frame(&[10, 0, 0, 0, 1, 2, 3]),
        Err(ProtocolError::Truncated)
    ));
    // The length is checked before the body is even looked at.
    let too_big = (MAX_FRAME_LEN + 1).to_le_bytes();
    assert!(
        matches!(decode_frame(&too_big), Err(ProtocolError::FrameTooLarge { len }) if len == u64::from(MAX_FRAME_LEN) + 1)
    );
    assert!(matches!(
        decode_frame(&u32::MAX.to_le_bytes()),
        Err(ProtocolError::FrameTooLarge { .. })
    ));
    // Two frames back to back: the first one is split off exactly.
    let mut two = Vec::new();
    write_frame(&mut two, &[op::PING]).unwrap();
    write_frame(&mut two, &[op::GET, 0, 0, 0, 0]).unwrap();
    let (body, used) = decode_frame(&two).unwrap();
    assert_eq!((body, used), (&[op::PING][..], 5));
    assert!(matches!(
        write_frame(&mut Vec::new(), &[]),
        Err(ProtocolError::EmptyFrame)
    ));

    fn decode(body: &[u8]) -> Result<Request<'_>, ProtocolError> {
        Request::decode(body)
    }
    assert!(matches!(decode(&[]), Err(ProtocolError::EmptyFrame)));
    assert!(matches!(decode(&[99]), Err(ProtocolError::UnknownOp(99))));
    assert!(matches!(
        decode(&[op::GET, 10, 0, 0, 0, b'a']),
        Err(ProtocolError::Malformed(_))
    ));
    assert!(matches!(
        decode(&[op::GET, 1, 0, 0]),
        Err(ProtocolError::Malformed(_))
    ));
    assert!(
        matches!(decode(&[op::PING, 0]), Err(ProtocolError::Malformed(m)) if m.contains("trailing"))
    );
    let mut scan = vec![op::SCAN_PREFIX, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0];
    assert!(
        matches!(decode(&scan), Err(ProtocolError::Malformed(m)) if m.contains("reverse must be 0 or 1"))
    );
    scan[9] = 1;
    assert_eq!(
        decode(&scan).unwrap(),
        Request::ScanPrefix {
            prefix: b"",
            limit: 0,
            reverse: true,
            with_values: false
        }
    );
    // A huge PUT_BATCH count is rejected before allocating for it.
    let batch = [op::PUT_BATCH, 0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0];
    assert!(matches!(decode(&batch), Err(ProtocolError::Malformed(m)) if m.contains("cannot fit")));

    assert!(matches!(
        Reply::decode(op::GET, &[7]),
        Err(ProtocolError::UnknownStatus(7))
    ));
    assert!(matches!(
        Reply::decode(op::GET, &[]),
        Err(ProtocolError::EmptyFrame)
    ));
    assert!(matches!(
        Reply::decode(99, &[0]),
        Err(ProtocolError::UnknownOp(99))
    ));
    assert!(matches!(
        Reply::decode(op::GET, &[1, 0]),
        Err(ProtocolError::Malformed(_))
    ));
    assert!(matches!(
        Reply::decode(op::PUT_BATCH, &[0, 2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0]),
        Err(ProtocolError::Malformed(_))
    ));
    assert!(matches!(
        Reply::decode(op::SCAN_PREFIX, &[0, 9, 0, 0, 0]),
        Err(ProtocolError::Malformed(_))
    ));
    assert_eq!(
        Reply::decode(op::PUT, &[2, 0xff, b'x']).unwrap(),
        Reply::Error("\u{fffd}x".into())
    );
}

#[test]
fn read_frame_validates_before_allocating() {
    let mut body = Vec::new();
    assert!(!read_frame(&mut Cursor::new(Vec::new()), &mut body).unwrap());
    assert!(matches!(
        read_frame(&mut Cursor::new(vec![5, 0]), &mut body),
        Err(ProtocolError::Truncated)
    ));
    assert!(matches!(
        read_frame(&mut Cursor::new(vec![0, 0, 0, 0]), &mut body),
        Err(ProtocolError::EmptyFrame)
    ));

    let mut fresh = Vec::new();
    let oversized = (MAX_FRAME_LEN + 1).to_le_bytes().to_vec();
    assert!(matches!(
        read_frame(&mut Cursor::new(oversized), &mut fresh),
        Err(ProtocolError::FrameTooLarge { .. })
    ));
    assert_eq!(
        fresh.capacity(),
        0,
        "nothing may be allocated for a rejected length"
    );

    // A valid but huge declared length with few bytes behind it: truncated,
    // and the buffer only grew with what arrived (plus a bounded reservation).
    let mut lying = (60u32 << 20).to_le_bytes().to_vec();
    lying.extend_from_slice(&[op::PING; 10]);
    let mut buf = Vec::new();
    assert!(matches!(
        read_frame(&mut Cursor::new(lying), &mut buf),
        Err(ProtocolError::Truncated)
    ));
    assert!(buf.capacity() <= 2 << 20, "capacity {}", buf.capacity());

    let mut stream = Vec::new();
    Request::Put {
        key: b"k",
        value: b"v",
    }
    .encode(&mut stream)
    .unwrap();
    Request::Ping.encode(&mut stream).unwrap();
    let mut reader = Cursor::new(stream);
    assert!(read_frame(&mut reader, &mut body).unwrap());
    assert_eq!(
        Request::decode(&body).unwrap(),
        Request::Put {
            key: b"k",
            value: b"v"
        }
    );
    assert!(read_frame(&mut reader, &mut body).unwrap());
    assert_eq!(Request::decode(&body).unwrap(), Request::Ping);
    assert!(!read_frame(&mut reader, &mut body).unwrap());
}

#[test]
fn oversized_messages_are_refused_when_encoding() {
    let max = MAX_FRAME_LEN as usize;
    let mut out = vec![0xee];
    // status (1) + length (4) + value: the largest value that fits exactly.
    let fits = vec![0u8; max - 5];
    Reply::Value(fits).encode(&mut out).unwrap();
    assert_eq!(out.len(), 1 + 4 + max);
    let mut out = vec![0xee];
    let too_big = vec![0u8; max - 4];
    assert!(matches!(
        Reply::Value(too_big).encode(&mut out),
        Err(ProtocolError::FrameTooLarge { .. })
    ));
    assert_eq!(out, [0xee], "a refused frame leaves the buffer as it was");
    let mut out = Vec::new();
    let huge_key = vec![0u8; max];
    assert!(matches!(
        Request::Get { key: &huge_key }.encode(&mut out),
        Err(ProtocolError::FrameTooLarge { .. })
    ));
    assert!(out.is_empty());
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1024))]

    #[test]
    fn decoders_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..300), reply_op in 0u8..10) {
        let _ = Request::decode(&bytes);
        let _ = Reply::decode(reply_op, &bytes);
        let _ = decode_frame(&bytes);
        let mut reader = Cursor::new(bytes.clone());
        let mut body = Vec::new();
        for _ in 0..8 {
            match read_frame(&mut reader, &mut body) {
                Ok(true) => { let _ = Request::decode(&body); }
                Ok(false) | Err(_) => break,
            }
        }
    }

    #[test]
    fn random_requests_roundtrip(
        key in proptest::collection::vec(any::<u8>(), 0..40),
        value in proptest::collection::vec(any::<u8>(), 0..200),
        offset in any::<u64>(),
        limit in any::<u32>(),
        flags in any::<(bool, bool)>(),
    ) {
        roundtrip_request(&Request::Put { key: &key, value: &value });
        roundtrip_request(&Request::Range { key: &key, offset, len: offset.rotate_left(7) });
        roundtrip_request(&Request::ScanPrefix { prefix: &key, limit, reverse: flags.0, with_values: flags.1 });
        roundtrip_request(&Request::PutBatch { items: vec![(&key[..], &value[..]), (&value[..], &key[..])] });
        let items = vec![ScanItem { key: key.clone(), revision: offset, logical_len: value.len() as u64, value: flags.1.then(|| value.clone()) }];
        roundtrip_reply(op::SCAN_PREFIX, &Reply::Items(items));
    }
}

// ---------------------------------------------------------------------------
// TCP server (transport paths that do not need the engine)
// ---------------------------------------------------------------------------

fn client(addr: std::net::SocketAddr) -> Client {
    let c = Client::connect(addr).expect("connect");
    c.set_timeout(Some(Duration::from_secs(20)))
        .expect("timeout");
    c
}

fn error_message(body: &[u8]) -> String {
    match Reply::decode(op::PING, body).expect("decodable reply") {
        Reply::Error(m) => m,
        other => panic!("expected an ERROR reply, got {other:?}"),
    }
}

#[test]
fn server_ping_and_malformed_frames() {
    let handle = server::serve(Arc::new(mem_db()), "127.0.0.1:0", 2).expect("serve");
    let addr = handle.local_addr();
    let mut c = client(addr);
    for _ in 0..3 {
        c.ping().unwrap();
    }
    // Malformed request inside a well-delimited frame: ERROR, connection kept.
    let mut frame = Vec::new();
    write_frame(&mut frame, &[99]).unwrap();
    let body = c.send_raw(&frame).unwrap().expect("reply");
    assert!(error_message(&body).contains("unknown op 99"));
    c.ping().unwrap();
    let mut frame = Vec::new();
    write_frame(&mut frame, &[op::GET, 100, 0, 0, 0, b'a']).unwrap();
    let body = c.send_raw(&frame).unwrap().expect("reply");
    assert!(error_message(&body).contains("malformed"));
    c.ping().unwrap();
    // Zero-length frame: ERROR, then the connection is closed.
    let body = c.send_raw(&0u32.to_le_bytes()).unwrap().expect("reply");
    assert!(error_message(&body).contains("empty frame"));
    assert!(c.ping().is_err());
    // Oversized declared length: ERROR (nothing allocated), then closed.
    let mut c2 = client(addr);
    let body = c2
        .send_raw(&(MAX_FRAME_LEN + 1).to_le_bytes())
        .unwrap()
        .expect("reply");
    assert!(error_message(&body).contains("exceeds the maximum"));
    assert!(c2.ping().is_err());

    let stats = handle.stop();
    assert_eq!(stats.connections, 2);
    assert_eq!(stats.requests, 7);
    assert_eq!(stats.error_replies, 4);
    assert!(stats.bytes_in > 0 && stats.bytes_out > 0);
}

#[test]
fn server_stops_with_idle_and_queued_connections() {
    let handle = server::serve(Arc::new(mem_db()), "127.0.0.1:0", 2).expect("serve");
    let addr = handle.local_addr();
    let mut busy: Vec<Client> = (0..2).map(|_| client(addr)).collect();
    for c in &mut busy {
        c.ping().unwrap();
    }
    // Both workers hold a connection; this one waits for a free worker.
    let _queued = client(addr);
    let started = Instant::now();
    handle.stop();
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "stop took {:?}",
        started.elapsed()
    );
    for c in &mut busy {
        assert!(c.ping().is_err());
    }
    // Dropping a handle also stops the server.
    let handle = server::serve(Arc::new(mem_db()), "127.0.0.1:0", 1).expect("serve");
    let mut c = client(handle.local_addr());
    c.ping().unwrap();
    drop(handle);
    assert!(c.ping().is_err());
}

#[test]
fn server_serves_concurrent_clients() {
    let handle = server::serve(Arc::new(mem_db()), "127.0.0.1:0", 4).expect("serve");
    let addr = handle.local_addr();
    std::thread::scope(|s| {
        for _ in 0..4 {
            s.spawn(move || {
                let mut c = client(addr);
                for _ in 0..50 {
                    c.ping().unwrap();
                }
            });
        }
    });
    let stats = handle.stop();
    assert_eq!(stats.requests, 200);
    assert_eq!(stats.error_replies, 0);
}

#[test]
fn handle_request_rejects_bad_requests() {
    let db = mem_db();
    assert_eq!(handle_request(&db, &[op::PING]), Reply::Done);
    match handle_request(&db, &[42]) {
        Reply::Error(m) => assert!(m.contains("bad request: unknown op 42"), "{m}"),
        other => panic!("{other:?}"),
    }
    match handle_request(&db, &[op::PUT, 1, 0, 0, 0]) {
        Reply::Error(m) => assert!(m.contains("bad request"), "{m}"),
        other => panic!("{other:?}"),
    }
}

// ---------------------------------------------------------------------------
// The binary: usage errors and pre-open failures never touch the database
// ---------------------------------------------------------------------------

#[test]
fn bin_help_and_version() {
    let o = run_bin(&["help"]);
    assert_eq!(o.status.code(), Some(0));
    let text = stdout(&o);
    for h in commands::COMMANDS.iter().filter(|h| h.name != "quit") {
        assert!(text.contains(h.name), "help lacks {}", h.name);
    }
    let o = run_bin(&["help", "put"]);
    assert_eq!(o.status.code(), Some(0));
    assert!(stdout(&o).contains("usage: babeldb --db <path> [global options] put <key>"));
    assert_eq!(run_bin(&["--help"]).status.code(), Some(0));
    let o = run_bin(&["--version"]);
    assert_eq!(o.status.code(), Some(0));
    assert!(stdout(&o).starts_with("babeldb "));
    let o = run_bin(&["help", "bogus"]);
    assert_eq!(o.status.code(), Some(2));
    assert!(stderr(&o).contains("unknown command bogus"));
    let o = run_bin(&[]);
    assert_eq!(o.status.code(), Some(2));
    assert!(stderr(&o).contains("error: missing command"));
}

#[test]
fn bin_usage_errors_never_touch_the_database() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("never.redb");
    let p = path_str(&db);
    let key_hex = "00".repeat(5);
    let cases: Vec<Vec<&str>> = vec![
        vec!["--db", p, "bogus"],
        vec!["--db", p, "put", "k", "--value", "a", "--hex", "00"],
        vec![
            "--db",
            p,
            "put",
            "k",
            "--value",
            "v",
            "--if-absent",
            "--if-revision",
            "3",
        ],
        vec!["--db", p, "put", "k", "--value", "v", "extra"],
        vec!["--db", p, "--block-size", "100", "put", "k", "--value", "v"],
        vec![
            "--db",
            p,
            "--inline-max",
            "1MiB",
            "--block-size",
            "4KiB",
            "put",
            "k",
            "--value",
            "v",
        ],
        vec!["--db", p, "--mode", "weird", "put", "k", "--value", "v"],
        vec!["--db", p, "scan", "--limit", "nope"],
        vec!["--db", p, "gen", "k", "xof", &key_hex, "5"],
        vec!["--db", p, "serve", "--threads", "0"],
        vec!["--db", p, "get", "k", "--hex", "--out", "x"],
        vec!["--db", p, "quit"],
        vec!["put", "k", "--value", "v", "--db", p],
    ];
    for case in &cases {
        let o = run_bin(case);
        assert_eq!(o.status.code(), Some(2), "{case:?}: {}", stderr(&o));
        assert!(
            stderr(&o).starts_with("error: "),
            "{case:?}: {}",
            stderr(&o)
        );
        assert!(!db.exists(), "{case:?} created the database");
    }
    let o = run_bin(&["put", "k", "--value", "v"]);
    assert_eq!(o.status.code(), Some(2));
    assert!(stderr(&o).contains("missing --db <path>"));
}

#[test]
fn bin_runtime_failures_before_open_leave_no_database() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("absent.redb");
    let p = path_str(&db);
    // Commands that only read never create a database.
    for words in [
        vec!["get", "k"],
        vec!["stats"],
        vec!["verify"],
        vec!["inspect", "k"],
        vec!["delete", "k"],
    ] {
        let mut full = vec!["--db", p];
        full.extend(words.iter().copied());
        let o = run_bin(&full);
        assert_eq!(o.status.code(), Some(1), "{full:?}");
        assert!(stderr(&o).contains("does not exist"), "{}", stderr(&o));
        assert!(!db.exists());
    }
    // Inputs are read before the database is opened.
    let missing = dir.path().join("missing.bin");
    let m = path_str(&missing);
    for words in [vec!["put", "k", "--file", m], vec!["import", "k", m]] {
        let mut full = vec!["--db", p];
        full.extend(words.iter().copied());
        let o = run_bin(&full);
        assert_eq!(o.status.code(), Some(1), "{full:?}: {}", stderr(&o));
        assert!(!db.exists(), "{full:?} created the database");
    }
    let o = run_bin(&["--db", p, "import", "k", path_str(dir.path())]);
    assert_eq!(o.status.code(), Some(1));
    assert!(stderr(&o).contains("not a regular file"));
    // The listen address is bound before the database is opened.
    let blocker = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = blocker.local_addr().unwrap().to_string();
    let o = run_bin(&["--db", p, "serve", "--addr", &addr]);
    assert_eq!(o.status.code(), Some(1), "{}", stderr(&o));
    assert!(stderr(&o).contains("cannot listen on"), "{}", stderr(&o));
    assert!(!db.exists());
}

// ---------------------------------------------------------------------------
// End to end (need the engine and the redb store)
// ---------------------------------------------------------------------------

fn ok(o: &Output) -> &Output {
    assert_eq!(
        o.status.code(),
        Some(0),
        "stdout: {}\nstderr: {}",
        stdout(o),
        stderr(o)
    );
    o
}

fn pseudo_random(n: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    (0..n)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 24) as u8
        })
        .collect()
}

#[test]
#[ignore = "after merge: needs engine"]
fn e2e_put_get_exact_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("e2e.redb");
    let p = path_str(&db);
    let o = run_bin(&["--db", p, "put", "greeting", "--value", "hello\r\nworld"]);
    assert!(stdout(ok(&o)).starts_with("rev="));
    assert_eq!(
        ok(&run_bin(&["--db", p, "get", "greeting"])).stdout,
        b"hello\r\nworld"
    );

    let all: Vec<u8> = (0..=255u8).collect();
    ok(&run_bin(&[
        "--db",
        p,
        "put",
        "bin",
        "--hex",
        &hex_encode(&all),
    ]));
    assert_eq!(ok(&run_bin(&["--db", p, "get", "bin"])).stdout, all);
    assert_eq!(
        ok(&run_bin(&["--db", p, "get", "bin", "--hex"])).stdout,
        format!("{}\n", hex_encode(&all)).into_bytes()
    );
    assert_eq!(
        ok(&run_bin(&["--db", p, "range", "bin", "250", "100"])).stdout,
        &all[250..]
    );
    assert!(
        ok(&run_bin(&["--db", p, "head", "bin"]))
            .stdout
            .ends_with(b"\tlen=256\n")
    );

    let data = pseudo_random(100_000, 7);
    let src = dir.path().join("src.bin");
    let copy = dir.path().join("copy.bin");
    std::fs::write(&src, &data).unwrap();
    ok(&run_bin(&[
        "--db",
        p,
        "put",
        "file",
        "--file",
        path_str(&src),
    ]));
    ok(&run_bin(&[
        "--db",
        p,
        "get",
        "file",
        "--out",
        path_str(&copy),
    ]));
    assert_eq!(std::fs::read(&copy).unwrap(), data);

    let piped = b"\x00\r\n\xff piped";
    ok(&run_bin_with_stdin(&["--db", p, "put", "piped"], piped));
    assert_eq!(ok(&run_bin(&["--db", p, "get", "piped"])).stdout, piped);
    ok(&run_bin_with_stdin(&["--db", p, "put", "empty"], b""));
    assert_eq!(ok(&run_bin(&["--db", p, "get", "empty"])).stdout, b"");

    let o = run_bin(&["--db", p, "get", "nope"]);
    assert_eq!(o.status.code(), Some(1));
    assert!(stderr(&o).contains("not found: nope"));
    let o = run_bin(&["--db", p, "put", "greeting", "--value", "x", "--if-absent"]);
    assert_eq!(o.status.code(), Some(1), "{}", stderr(&o));
}

#[test]
#[ignore = "after merge: needs engine"]
fn e2e_import_inspect_stats_verify() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("e2e.redb");
    let p = path_str(&db);
    let mut data = b"line of text\n".repeat(8_000);
    data.extend(pseudo_random(60_000, 3));
    let src = dir.path().join("data.bin");
    std::fs::write(&src, &data).unwrap();
    let o = run_bin(&["--db", p, "import", "doc", path_str(&src)]);
    assert!(
        stdout(ok(&o)).contains(&format!("len={}", data.len())),
        "{}",
        stdout(&o)
    );
    let copy = dir.path().join("copy.bin");
    ok(&run_bin(&[
        "--db",
        p,
        "get",
        "doc",
        "--out",
        path_str(&copy),
    ]));
    assert_eq!(std::fs::read(&copy).unwrap(), data);

    let text = stdout(ok(&run_bin(&["--db", p, "inspect", "doc"])));
    let kind = report_row("kind", "chunks");
    for needle in [
        kind.as_str(),
        "stored/logical",
        "metadata",
        "envelope headers",
        "local-file",
    ] {
        assert!(text.contains(needle), "missing '{needle}':\n{text}");
    }
    assert!(!text.contains('%'));
    let text = stdout(ok(&run_bin(&["--db", p, "stats"])));
    let records = report_sub("live records", "1");
    for needle in [
        "total payload",
        "apparent",
        "allocated",
        "per codec",
        records.as_str(),
    ] {
        assert!(text.contains(needle), "missing '{needle}':\n{text}");
    }
    assert!(stdout(ok(&run_bin(&["--db", p, "verify"]))).contains("OK"));
    assert!(stdout(ok(&run_bin(&["--db", p, "verify", "--deep"]))).contains("OK (deep"));
    assert!(stdout(ok(&run_bin(&["--db", p, "sources"]))).contains("local-file"));
    ok(&run_bin(&["--db", p, "gc"]));
    ok(&run_bin(&["--db", p, "compact"]));
}

#[test]
#[ignore = "after merge: needs engine"]
fn e2e_gen_scan_history_delete() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("e2e.redb");
    let p = path_str(&db);
    ok(&run_bin(&[
        "--db", p, "gen", "seq", "arith", "0", "1", "1000",
    ]));
    assert_eq!(
        ok(&run_bin(&["--db", p, "range", "seq", "8", "8", "--hex"])).stdout,
        b"0100000000000000\n"
    );
    ok(&run_bin(&["--db", p, "gen", "rep", "repeat", "10", "ab"]));
    assert_eq!(
        ok(&run_bin(&["--db", p, "get", "rep"])).stdout,
        b"ababababab"
    );

    ok(&run_bin(&[
        "--db",
        p,
        "--history",
        "put",
        "h",
        "--value",
        "v1",
    ]));
    let first = stdout(ok(&run_bin(&["--db", p, "head", "h"])));
    let rev1 = first
        .split("rev=")
        .nth(1)
        .and_then(|s| s.split('\t').next())
        .unwrap()
        .to_string();
    ok(&run_bin(&[
        "--db",
        p,
        "--history",
        "put",
        "h",
        "--value",
        "v2",
    ]));
    let history = stdout(ok(&run_bin(&["--db", p, "history", "h"])));
    assert_eq!(history.lines().count(), 2, "{history}");
    assert!(history.lines().last().unwrap().ends_with("\tcurrent"));
    assert_eq!(
        ok(&run_bin(&["--db", p, "get-at", "h", &rev1])).stdout,
        b"v1"
    );
    assert!(
        stdout(ok(&run_bin(&["--db", p, "prune-history", "--keep", "0"])))
            .contains("removed 1 history entries")
    );

    let scan = stdout(ok(&run_bin(&["--db", p, "scan", "--values"])));
    assert_eq!(scan.lines().count(), 3, "{scan}");
    assert!(scan.contains("h\trev="), "{scan}");
    assert!(scan.contains("\tvalue=v2"), "{scan}");
    let reversed = stdout(ok(&run_bin(&[
        "--db",
        p,
        "scan",
        "--reverse",
        "--limit",
        "1",
    ])));
    assert!(reversed.starts_with("seq\t"), "{reversed}");

    assert!(stdout(ok(&run_bin(&["--db", p, "delete", "h"]))).contains("deleted h"));
    let o = run_bin(&["--db", p, "delete", "h"]);
    assert_eq!(o.status.code(), Some(1));
    assert!(stderr(&o).contains("nothing deleted"));
}

#[test]
#[ignore = "after merge: needs engine"]
fn e2e_creation_parameters_note() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("e2e.redb");
    let p = path_str(&db);
    ok(&run_bin(&[
        "--db",
        p,
        "--block-size",
        "4096",
        "--mode",
        "babel-pure",
        "put",
        "k",
        "--value",
        "v",
    ]));
    let o = run_bin(&[
        "--db",
        p,
        "--block-size",
        "8192",
        "--mode",
        "adaptive",
        "get",
        "k",
    ]);
    assert_eq!(ok(&o).stdout, b"v");
    let err = stderr(&o);
    assert!(
        err.contains(
            "note: --block-size 8192 ignored: this database was created with block size 4096"
        ),
        "{err}"
    );
    assert!(
        err.contains(
            "note: --mode adaptive ignored: this database was created with mode babel-pure"
        ),
        "{err}"
    );
    let text = stdout(ok(&run_bin(&["--db", p, "stats"])));
    assert!(text.contains("babel-pure"), "{text}");
}

#[test]
#[ignore = "after merge: needs engine"]
fn e2e_execute_on_memstore() {
    let mut db = mem_db();
    let mut run = |words: &[&str]| {
        let mut out = Vec::new();
        let flow = cli::execute(&mut db, &parse(words).unwrap(), &mut out).unwrap();
        assert_eq!(flow, cli::Flow::Continue);
        out
    };
    assert!(
        String::from_utf8(run(&["put", "k", "--value", "v1"]))
            .unwrap()
            .starts_with("rev=")
    );
    assert_eq!(run(&["get", "k"]), b"v1");
    assert!(
        String::from_utf8(run(&["head", "k"]))
            .unwrap()
            .ends_with("\tlen=2\n")
    );
    let inspect = String::from_utf8(run(&["inspect", "k"])).unwrap();
    assert!(
        inspect.contains("inline") && inspect.contains("stored/logical"),
        "{inspect}"
    );
    let stats = String::from_utf8(run(&["stats"])).unwrap();
    assert!(stats.contains("(none: in-memory backend)"), "{stats}");
    let mut out = Vec::new();
    let err = cli::execute(&mut db, &parse(&["get", "missing"]).unwrap(), &mut out).unwrap_err();
    assert!(matches!(err, cli::CliError::NotFound(_)));
    assert_eq!(err.exit_code(), cli::EXIT_RUNTIME);
}

#[test]
#[ignore = "after merge: needs engine"]
fn e2e_repl_session_on_memstore() {
    let mut db = mem_db();
    let script = "put a --value 1\nput \"b c\" --hex 00ff\nget a\nsc\nhea a\ndel a\nget a\nquit\nput z --value never\n";
    let (out, err) = run_session(&mut db, script);
    assert!(out.contains("\n1\n"), "{out}");
    assert!(out.contains("\"b c\"\trev="), "{out}");
    assert!(out.contains("deleted a"), "{out}");
    assert!(err.contains("not found: a"), "{err}");
    assert_eq!(db.get(b"z").unwrap(), None);
    assert_eq!(db.get(b"b c").unwrap(), Some(vec![0, 0xff]));
}

#[test]
#[ignore = "after merge: needs engine"]
fn e2e_server_roundtrip_via_client() {
    let db = Arc::new(mem_db());
    let handle = server::serve(Arc::clone(&db), "127.0.0.1:0", 2).expect("serve");
    let mut c = client(handle.local_addr());
    let r1 = c.put(b"user/1", b"hello").unwrap();
    assert_eq!(c.get(b"user/1").unwrap().as_deref(), Some(&b"hello"[..]));
    assert_eq!(db.get(b"user/1").unwrap().as_deref(), Some(&b"hello"[..]));
    assert_eq!(c.get(b"missing").unwrap(), None);
    assert_eq!(
        c.range(b"user/1", 1, 3).unwrap().as_deref(),
        Some(&b"ell"[..])
    );
    assert!(matches!(
        c.range(b"user/1", 99, 1),
        Err(ProtocolError::Remote(_))
    ));
    c.ping().unwrap();
    let binary: Vec<u8> = (0..=255u8).collect();
    let revs = c
        .put_batch(&[(&b"user/2"[..], &b"two"[..]), (&b"user/3"[..], &binary[..])])
        .unwrap();
    assert_eq!(revs.len(), 2);
    assert!(revs[0] > r1 && revs[1] > r1);
    let items = c.scan_prefix(b"user/", 0, false, true).unwrap();
    let keys: Vec<&[u8]> = items.iter().map(|i| i.key.as_slice()).collect();
    assert_eq!(keys, [&b"user/1"[..], b"user/2", b"user/3"]);
    assert_eq!(items[2].value.as_deref(), Some(&binary[..]));
    let reversed = c.scan_prefix(b"user/", 2, true, false).unwrap();
    assert_eq!(
        reversed
            .iter()
            .map(|i| i.key.as_slice())
            .collect::<Vec<_>>(),
        [&b"user/3"[..], b"user/2"]
    );
    assert!(reversed.iter().all(|i| i.value.is_none()));
    assert!(c.delete(b"user/2").unwrap());
    assert!(!c.delete(b"user/2").unwrap());
    assert_eq!(c.put_batch(&[]).unwrap(), Vec::<u64>::new());
    handle.stop();
}

#[test]
#[ignore = "after merge: needs engine"]
fn e2e_serve_binary_with_client() {
    use std::io::BufRead;
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("served.redb");
    let mut child = bin()
        .args([
            "--db",
            path_str(&db),
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--threads",
            "2",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn serve");
    let mut line = String::new();
    std::io::BufReader::new(child.stdout.take().unwrap())
        .read_line(&mut line)
        .unwrap();
    let addr = line
        .split(" on ")
        .nth(1)
        .and_then(|s| s.split(' ').next())
        .expect("address in banner")
        .to_string();
    let mut c = client(addr.parse().unwrap());
    c.put(b"k", b"v").unwrap();
    assert_eq!(c.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
    child.kill().unwrap();
    child.wait().unwrap();
    // Every acknowledged write was durable.
    assert_eq!(
        ok(&run_bin(&["--db", path_str(&db), "get", "k"])).stdout,
        b"v"
    );
}
