//! Pure text helpers of the CLI and the REPL: the REPL tokenizer and its exact
//! inverse ([`quote`]), hex, number and size parsing, human-readable sizes and
//! UTC timestamps.

use std::fmt;

/// Error of [`tokenize`], with the 1-based byte column where it was detected.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TokenizeError {
    pub column: usize,
    pub message: String,
}

impl fmt::Display for TokenizeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "column {}: {}", self.column, self.message)
    }
}

impl std::error::Error for TokenizeError {}

/// Split one REPL line into tokens. Tokens are arbitrary bytes.
///
/// - Unquoted spaces, tabs, CR and LF separate tokens.
/// - `"..."` groups bytes, spaces included; `""` is an empty token; quoted and
///   unquoted parts written next to each other form one token (`a"b c"` is `ab c`).
/// - Escapes work inside and outside quotes: `\"` `\\` `\n` `\t` `\r` `\0`,
///   `\xHH` (any byte) and `\ ` (a space). Any other escape is an error, so
///   `C:\new` is rejected instead of silently containing a newline.
pub fn tokenize(line: &[u8]) -> Result<Vec<Vec<u8>>, TokenizeError> {
    let mut tokens = Vec::new();
    let mut current: Option<Vec<u8>> = None;
    // Column of the opening quote while inside quotes.
    let mut open_quote: Option<usize> = None;
    let mut i = 0;
    while i < line.len() {
        let b = line[i];
        match b {
            b'"' => {
                open_quote = match open_quote {
                    Some(_) => None,
                    None => Some(i + 1),
                };
                current.get_or_insert_with(Vec::new);
                i += 1;
            }
            b'\\' => {
                let (byte, used) = unescape(line, i)?;
                current.get_or_insert_with(Vec::new).push(byte);
                i += used;
            }
            b' ' | b'\t' | b'\r' | b'\n' if open_quote.is_none() => {
                if let Some(t) = current.take() {
                    tokens.push(t);
                }
                i += 1;
            }
            _ => {
                current.get_or_insert_with(Vec::new).push(b);
                i += 1;
            }
        }
    }
    if let Some(column) = open_quote {
        return Err(TokenizeError {
            column,
            message: "unterminated double quote".into(),
        });
    }
    if let Some(t) = current {
        tokens.push(t);
    }
    Ok(tokens)
}

/// Decode the escape starting at `line[i] == b'\\'`: (byte, bytes consumed).
fn unescape(line: &[u8], i: usize) -> Result<(u8, usize), TokenizeError> {
    let err = |message: String| TokenizeError {
        column: i + 1,
        message,
    };
    let Some(&c) = line.get(i + 1) else {
        return Err(err(
            "dangling backslash at end of line (write \\\\ for a backslash)".into(),
        ));
    };
    let byte = match c {
        b'"' => b'"',
        b'\\' => b'\\',
        b'n' => b'\n',
        b't' => b'\t',
        b'r' => b'\r',
        b'0' => 0,
        b' ' => b' ',
        b'x' => {
            let hi = line.get(i + 2).and_then(|&h| hex_value(h));
            let lo = line.get(i + 3).and_then(|&h| hex_value(h));
            return match (hi, lo) {
                (Some(h), Some(l)) => Ok(((h << 4) | l, 4)),
                _ => Err(err("\\x must be followed by two hex digits".into())),
            };
        }
        other => {
            let shown = if other.is_ascii_graphic() {
                format!("\\{}", other as char)
            } else {
                format!("\\ followed by byte 0x{other:02x}")
            };
            return Err(err(format!(
                "unknown escape {shown} (write \\\\ for a literal backslash; \
                 forward slashes also work in Windows paths)"
            )));
        }
    };
    Ok((byte, 2))
}

/// Characters that are valid UTF-8 but invisible or reorder text on a
/// terminal: always escaped by [`quote`].
fn is_invisible(c: char) -> bool {
    matches!(c,
        '\u{200B}'..='\u{200F}' | '\u{202A}'..='\u{202E}' | '\u{2060}'..='\u{2064}'
        | '\u{2066}'..='\u{2069}' | '\u{FEFF}')
}

fn is_plain(bytes: &[u8]) -> bool {
    match std::str::from_utf8(bytes) {
        Ok(s) => {
            !s.is_empty()
                && !s.starts_with('#')
                && s.chars().all(|c| {
                    !c.is_whitespace()
                        && !c.is_control()
                        && !is_invisible(c)
                        && c != '"'
                        && c != '\\'
                })
        }
        Err(_) => false,
    }
}

fn push_hex_escape(s: &mut String, b: u8) {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    s.push_str("\\x");
    s.push(DIGITS[(b >> 4) as usize] as char);
    s.push(DIGITS[(b & 15) as usize] as char);
}

/// Exact inverse of [`tokenize`] for one token: `tokenize(quote(b)) == [b]`
/// for any bytes. Plain printable UTF-8 is returned as is; anything else is
/// written in double quotes with escapes (`\xHH` for control characters,
/// invisible characters, non-space whitespace and bytes that are not UTF-8).
pub fn quote(bytes: &[u8]) -> String {
    if is_plain(bytes) {
        return String::from_utf8_lossy(bytes).into_owned();
    }
    let mut s = String::with_capacity(bytes.len() + 2);
    s.push('"');
    for chunk in bytes.utf8_chunks() {
        for c in chunk.valid().chars() {
            match c {
                '"' => s.push_str("\\\""),
                '\\' => s.push_str("\\\\"),
                '\n' => s.push_str("\\n"),
                '\t' => s.push_str("\\t"),
                '\r' => s.push_str("\\r"),
                ' ' => s.push(' '),
                c if c.is_control() || c.is_whitespace() || is_invisible(c) => {
                    let mut buf = [0u8; 4];
                    for &b in c.encode_utf8(&mut buf).as_bytes() {
                        push_hex_escape(&mut s, b);
                    }
                }
                c => s.push(c),
            }
        }
        for &b in chunk.invalid() {
            push_hex_escape(&mut s, b);
        }
    }
    s.push('"');
    s
}

// ---------------------------------------------------------------------------
// Hex
// ---------------------------------------------------------------------------

pub fn hex_value(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Lowercase hex of `bytes` (table-driven: cheap for large values).
pub fn hex_encode(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(DIGITS[(b >> 4) as usize] as char);
        out.push(DIGITS[(b & 15) as usize] as char);
    }
    out
}

/// Decode hex digits (either case, no separators, even count; empty is allowed).
pub fn hex_decode(s: &[u8]) -> Result<Vec<u8>, String> {
    if !s.len().is_multiple_of(2) {
        return Err(format!("odd number of hex digits ({})", s.len()));
    }
    s.chunks_exact(2)
        .enumerate()
        .map(|(i, pair)| match (hex_value(pair[0]), hex_value(pair[1])) {
            (Some(h), Some(l)) => Ok((h << 4) | l),
            _ => Err(format!("invalid hex digit at position {}", 2 * i + 1)),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Numbers and sizes
// ---------------------------------------------------------------------------

/// Unsigned integer: decimal or `0x` hex, `_` separators allowed.
pub fn parse_u64(s: &str) -> Result<u64, String> {
    let digits: String = s.chars().filter(|&c| c != '_').collect();
    let parsed = match digits
        .strip_prefix("0x")
        .or_else(|| digits.strip_prefix("0X"))
    {
        Some(h) => u64::from_str_radix(h, 16),
        None => digits.parse::<u64>(),
    };
    parsed.map_err(|e| format!("'{s}' is not a valid unsigned integer ({e})"))
}

/// Size in bytes: an unsigned integer with an optional binary suffix
/// (`K`/`KiB`, `M`/`MiB`, `G`/`GiB`, `B`; case-insensitive; powers of 1024).
pub fn parse_size(s: &str) -> Result<u64, String> {
    const SUFFIXES: [(&str, u64); 7] = [
        ("kib", 1 << 10),
        ("mib", 1 << 20),
        ("gib", 1 << 30),
        ("k", 1 << 10),
        ("m", 1 << 20),
        ("g", 1 << 30),
        ("b", 1),
    ];
    let lower = s.trim().to_ascii_lowercase();
    // Hex values may end in 'b' (0x1b): suffixes only apply to decimal input.
    let (number, multiplier) = if lower.starts_with("0x") {
        (lower.as_str(), 1)
    } else {
        SUFFIXES
            .iter()
            .find_map(|(suffix, m)| lower.strip_suffix(suffix).map(|n| (n.trim_end(), *m)))
            .unwrap_or((lower.as_str(), 1))
    };
    let n = parse_u64(number)
        .map_err(|_| format!("'{s}' is not a valid size (examples: 4096, 16KiB, 1M)"))?;
    n.checked_mul(multiplier)
        .ok_or_else(|| format!("size '{s}' overflows 64 bits"))
}

/// "512 B" or "16.0 KiB" (binary units).
pub fn human_size(n: u64) -> String {
    const UNITS: [&str; 6] = ["KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];
    if n < 1024 {
        return format!("{n} B");
    }
    let mut value = n as f64 / 1024.0;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}

/// Exact byte count plus the human-readable form: "16384 B (16.0 KiB)".
pub fn fmt_bytes(n: u64) -> String {
    if n < 1024 {
        format!("{n} B")
    } else {
        format!("{n} B ({})", human_size(n))
    }
}

/// Signed byte difference with the same conventions as [`fmt_bytes`].
pub fn fmt_signed_bytes(n: i128) -> String {
    let magnitude = u64::try_from(n.unsigned_abs()).unwrap_or(u64::MAX);
    if n < 0 {
        format!("-{}", fmt_bytes(magnitude))
    } else {
        format!("+{}", fmt_bytes(magnitude))
    }
}

/// `num / den` with 4 decimals, or "n/a" when `den == 0`.
pub fn fmt_ratio(num: u64, den: u64) -> String {
    if den == 0 {
        "n/a".into()
    } else {
        format!("{:.4}", num as f64 / den as f64)
    }
}

// ---------------------------------------------------------------------------
// Time
// ---------------------------------------------------------------------------

/// Milliseconds since the Unix epoch as an ISO-8601 UTC timestamp.
pub fn format_unix_ms(ms: u64) -> String {
    let secs = ms / 1000;
    let (days, sod) = (secs / 86_400, secs % 86_400);
    let (y, m, d) = civil_from_days(days as i64);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{:03}Z",
        sod / 3600,
        sod % 3600 / 60,
        sod % 60,
        ms % 1000
    )
}

/// Proleptic Gregorian (year, month, day) of a day count since 1970-01-01
/// (H. Hinnant's `civil_from_days`).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps() {
        assert_eq!(format_unix_ms(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(
            format_unix_ms(1_000_000_000_123),
            "2001-09-09T01:46:40.123Z"
        );
        assert_eq!(format_unix_ms(951_782_400_000), "2000-02-29T00:00:00.000Z");
        assert_eq!(
            format_unix_ms(1_709_164_800_000),
            "2024-02-29T00:00:00.000Z"
        );
    }

    #[test]
    fn sizes() {
        assert_eq!(parse_size("16KiB").unwrap(), 16384);
        assert_eq!(parse_size("1m").unwrap(), 1 << 20);
        assert_eq!(parse_size("512").unwrap(), 512);
        assert_eq!(parse_size("512b").unwrap(), 512);
        assert_eq!(parse_size("0x1b").unwrap(), 27);
        assert!(parse_size("12kb").is_err());
        assert!(parse_size("").is_err());
        assert_eq!(fmt_bytes(512), "512 B");
        assert_eq!(fmt_bytes(16384), "16384 B (16.0 KiB)");
        assert_eq!(fmt_signed_bytes(-2048), "-2048 B (2.0 KiB)");
    }

    #[test]
    fn hex() {
        assert_eq!(hex_encode(&[0, 0xab, 0xff]), "00abff");
        assert_eq!(hex_decode(b"00ABff").unwrap(), vec![0, 0xab, 0xff]);
        assert!(hex_decode(b"abc").is_err());
        assert!(hex_decode(b"zz").is_err());
        assert_eq!(hex_decode(b"").unwrap(), Vec::<u8>::new());
    }
}
