//! Interactive session over the typed commands: the same parser
//! ([`commands::parse_tokens`]) and executor ([`commands::execute`]) as the
//! one-shot CLI, fed line by line through [`tokenize`].
//!
//! - Syntax: spaces separate tokens, `"double quotes"` group, and the escapes
//!   `\" \\ \n \t \r \0 \xHH` and `\ ` work inside and outside quotes (see
//!   [`tokenize`]); keys and values printed by commands use the same syntax
//!   ([`crate::cli::text::quote`]), so they can be pasted back.
//! - A command may be abbreviated to any unique prefix (`ins` is `inspect`),
//!   a lightweight substitute for completion; an ambiguous prefix is an error
//!   listing the candidates; an exact name always wins (`get` vs `get-at`).
//! - `help`, `help <command>`, `quit` / `exit`; lines starting with `#` are
//!   comments.
//! - Errors (usage, engine, I/O, even a panic inside a command) are reported
//!   and the session continues; only `quit`, `exit` or the end of input end it.

use std::io::{self, BufRead, Write};
use std::panic::{self, AssertUnwindSafe};

use super::commands::{self, Command, Flow, HelpContext, ValueSource};
use super::text::tokenize;
use super::{CliError, UsageError, panic_message};
use crate::engine::Db;
use crate::store::Store;

/// Presentation of a session.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReplOptions {
    /// Printed before each line (`None` when the input is not a terminal).
    pub prompt: Option<String>,
    /// Printed once at the start.
    pub banner: Option<String>,
}

/// Resolve a command word: an exact name (or alias), else a unique prefix.
pub fn resolve_command(word: &[u8]) -> Result<&'static str, UsageError> {
    let Ok(text) = std::str::from_utf8(word) else {
        return Err(commands::unknown_command(word));
    };
    if let Some(name) = commands::canonical_name(text) {
        return Ok(name);
    }
    let candidates = if text.is_empty() {
        Vec::new()
    } else {
        commands::prefix_candidates(text)
    };
    match candidates.as_slice() {
        [one] => Ok(commands::canonical_name(one).unwrap_or(one)),
        [] => Err(commands::unknown_command(word)),
        many => Err(UsageError::general(format!(
            "ambiguous command '{text}': could be {}",
            many.join(", ")
        ))),
    }
}

/// Parse one REPL line. `Ok(None)` for blank and comment lines.
pub fn parse_line(line: &[u8]) -> Result<Option<Command>, UsageError> {
    let trimmed = line.trim_ascii();
    if trimmed.is_empty() || trimmed.starts_with(b"#") {
        return Ok(None);
    }
    let mut tokens =
        tokenize(line).map_err(|e| UsageError::general(format!("syntax error at {e}")))?;
    let Some(first) = tokens.first() else {
        return Ok(None);
    };
    let name = resolve_command(first)?;
    tokens[0] = name.as_bytes().to_vec();
    if name == "help" && tokens.len() == 2 {
        tokens[1] = resolve_command(&tokens[1])?.as_bytes().to_vec();
    }
    let command = commands::parse_tokens(&tokens)?;
    match &command {
        Command::Put {
            value: ValueSource::Stdin,
            ..
        } => Err(UsageError::new(
            Some("put"),
            "in the REPL the value must be given with --value, --hex or --file",
        )),
        Command::Repl => Err(UsageError::new(Some("repl"), "already in the REPL")),
        Command::Serve { .. } => Err(UsageError::new(
            Some("serve"),
            "'serve' is a top-level command: babeldb --db <path> serve [--addr <host:port>]",
        )),
        _ => Ok(Some(command)),
    }
}

/// Remembers the last byte written, so the session can end a value printed
/// without a trailing newline before showing the next prompt.
struct LineTracker<'a> {
    inner: &'a mut dyn Write,
    last: Option<u8>,
}

impl Write for LineTracker<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        if n > 0 {
            self.last = Some(buf[n - 1]);
        }
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

fn report_usage(err: &mut dyn Write, e: &UsageError) -> io::Result<()> {
    writeln!(err, "error: {}", e.message)?;
    if let Some(help) = e.command.and_then(commands::command_help) {
        writeln!(
            err,
            "usage: {}",
            commands::usage_line(help, HelpContext::Repl)
        )?;
    }
    Ok(())
}

fn report_error(err: &mut dyn Write, e: &CliError) -> io::Result<()> {
    match e {
        CliError::Usage(u) => report_usage(err, u),
        CliError::NotFound(_) => writeln!(err, "{e}"),
        other => writeln!(err, "error: {other}"),
    }
}

/// Run a session until `quit`/`exit` or the end of `input`. Command output
/// goes to `out`, errors to `err`. Only I/O errors on `input`/`out`/`err`
/// end the session early.
pub fn run<S: Store>(
    db: &mut Db<S>,
    input: &mut dyn BufRead,
    out: &mut dyn Write,
    err: &mut dyn Write,
    opts: &ReplOptions,
) -> io::Result<()> {
    if let Some(banner) = &opts.banner {
        writeln!(out, "{banner}")?;
    }
    let mut line = Vec::new();
    loop {
        if let Some(prompt) = &opts.prompt {
            write!(out, "{prompt}")?;
        }
        out.flush()?;
        line.clear();
        if input.read_until(b'\n', &mut line)? == 0 {
            if opts.prompt.is_some() {
                writeln!(out)?;
            }
            return out.flush();
        }
        let command = match parse_line(&line) {
            Ok(Some(command)) => command,
            Ok(None) => continue,
            Err(e) => {
                report_usage(err, &e)?;
                continue;
            }
        };
        if let Command::Help { topic } = &command {
            commands::write_help(out, *topic, HelpContext::Repl)?;
            continue;
        }
        let mut tracker = LineTracker {
            inner: &mut *out,
            last: None,
        };
        let outcome = panic::catch_unwind(AssertUnwindSafe(|| {
            commands::execute(db, &command, &mut tracker)
        }));
        let unterminated = tracker.last.is_some_and(|b| b != b'\n');
        if unterminated {
            out.write_all(b"\n")?;
        }
        out.flush()?;
        match outcome {
            Ok(Ok(Flow::Quit)) => return Ok(()),
            Ok(Ok(Flow::Continue)) => {}
            Ok(Err(e)) => report_error(err, &e)?,
            Err(payload) => writeln!(
                err,
                "error: internal error (the session continues): {}",
                panic_message(payload.as_ref())
            )?,
        }
    }
}
