//! Backend conformance suite: every `Store` must pass it.
//!
//! - `run_all`: functional checks, each on a fresh empty store from `make`.
//! - `run_persistent`: commit, close, reopen from a directory.
//! - `run_concurrent`: reader threads take snapshots while a writer commits.
//!
//! A violated expectation is returned as `Err(Error::Integrity { object_id: None, .. })`
//! whose detail starts with `store conformance [<check>]`; backend errors propagate as they
//! are. The suite itself never panics (a panicking worker thread is reported as an error).
//!
//! Contract pinned down by these checks (identical on every backend):
//! - keys are never empty (the engine never writes one and LMDB rejects them), but scan
//!   bounds may be empty slices; keys up to 511 bytes are checked (LMDB default limit);
//! - keys order as unsigned bytes, a prefix before its extensions;
//! - an empty value is present (`Some(vec![])`), unlike a missing key;
//! - empty or inverted scan ranges visit nothing and succeed;
//! - a scan callback may read (get/len/scan, same or other table) through the transaction
//!   being scanned; `Ok(false)` stops the scan, `Err` aborts it with that very error;
//! - a write transaction sees its own writes; dropping it without commit discards them;
//! - read transactions are snapshots; several may be open at once, also on the thread that
//!   holds the write transaction (LMDB: `MDB_NOTLS`), and they never wait for the writer;
//! - `begin_write` waits until the live write transaction commits or is dropped;
//! - `Durability::Deferred` commits are visible at once and survive a reopen once an
//!   `Immediate` commit followed them.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::ops::Bound::{self, Excluded, Included, Unbounded};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

use super::{Durability, ReadTxn, Store, Table, WriteTxn};
use crate::error::{Error, Result};

type Model = BTreeMap<Vec<u8>, Vec<u8>>;
type Entries = Vec<(Vec<u8>, Vec<u8>)>;

const RECORDS: Table = Table::Records;
/// How long a thread waits for another before the check fails instead of hanging.
const WAIT: Duration = Duration::from_secs(30);
const STOP_MARKER: &str = "conformance: callback stop marker";

fn fail(check: &str, detail: impl std::fmt::Display) -> Error {
    Error::integrity(None, format!("store conformance [{check}]: {detail}"))
}

fn ensure(ok: bool, check: &str, detail: impl FnOnce() -> String) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(fail(check, detail()))
    }
}

/// Printable, bounded form of a key or value.
fn show(b: &[u8]) -> String {
    const MAX: usize = 24;
    let mut s = String::from("\"");
    for &c in b.iter().take(MAX) {
        if c.is_ascii_graphic() && c != b'"' && c != b'\\' {
            s.push(char::from(c));
        } else {
            let _ = write!(s, "\\x{c:02x}");
        }
    }
    s.push('"');
    if b.len() > MAX {
        let _ = write!(s, "..({} bytes)", b.len());
    }
    s
}

fn show_opt(v: Option<&[u8]>) -> String {
    v.map_or_else(|| "None".to_string(), show)
}

fn show_bound(b: Bound<&[u8]>) -> String {
    match b {
        Included(k) => format!("Included({})", show(k)),
        Excluded(k) => format!("Excluded({})", show(k)),
        Unbounded => "Unbounded".to_string(),
    }
}

fn show_entry(e: Option<&(Vec<u8>, Vec<u8>)>) -> String {
    e.map_or_else(
        || "(end)".to_string(),
        |(k, v)| format!("{} => {}", show(k), show(v)),
    )
}

fn ensure_value(
    check: &str,
    what: impl FnOnce() -> String,
    got: Option<&[u8]>,
    want: Option<&[u8]>,
) -> Result<()> {
    if got == want {
        return Ok(());
    }
    let detail = match (got, want) {
        (Some(g), Some(w)) => {
            let at = g
                .iter()
                .zip(w)
                .position(|(x, y)| x != y)
                .unwrap_or(g.len().min(w.len()));
            format!(
                "{}: got {} ({} bytes), want {} ({} bytes), first difference at byte {at}",
                what(),
                show(g),
                g.len(),
                show(w),
                w.len()
            )
        }
        _ => format!("{}: got {}, want {}", what(), show_opt(got), show_opt(want)),
    };
    Err(fail(check, detail))
}

fn ensure_entries(
    check: &str,
    what: impl FnOnce() -> String,
    got: &[(Vec<u8>, Vec<u8>)],
    want: &[(Vec<u8>, Vec<u8>)],
) -> Result<()> {
    if got == want {
        return Ok(());
    }
    let i = got
        .iter()
        .zip(want)
        .position(|(g, w)| g != w)
        .unwrap_or(got.len().min(want.len()));
    Err(fail(
        check,
        format!(
            "{}: {} entries, want {}; first difference at #{i}: got {}, want {}",
            what(),
            got.len(),
            want.len(),
            show_entry(got.get(i)),
            show_entry(want.get(i))
        ),
    ))
}

#[derive(Clone, Copy)]
struct Scan<'k> {
    start: Bound<&'k [u8]>,
    end: Bound<&'k [u8]>,
    reverse: bool,
}

impl<'k> Scan<'k> {
    fn new(start: Bound<&'k [u8]>, end: Bound<&'k [u8]>, reverse: bool) -> Self {
        Scan {
            start,
            end,
            reverse,
        }
    }

    fn all(reverse: bool) -> Self {
        Scan::new(Unbounded, Unbounded, reverse)
    }

    fn describe(&self, table: Table) -> String {
        format!(
            "scan({table:?}, {}, {}, reverse={})",
            show_bound(self.start),
            show_bound(self.end),
            self.reverse
        )
    }

    fn contains(&self, k: &[u8]) -> bool {
        let above = match self.start {
            Included(s) => k >= s,
            Excluded(s) => k > s,
            Unbounded => true,
        };
        let below = match self.end {
            Included(e) => k <= e,
            Excluded(e) => k < e,
            Unbounded => true,
        };
        above && below
    }
}

fn inc(k: &[u8]) -> Bound<&[u8]> {
    Included(k)
}

fn exc(k: &[u8]) -> Bound<&[u8]> {
    Excluded(k)
}

fn collect<T: ReadTxn + ?Sized>(t: &T, table: Table, q: Scan<'_>) -> Result<Entries> {
    let mut out = Entries::new();
    t.scan(table, q.start, q.end, q.reverse, &mut |k, v| {
        out.push((k.to_vec(), v.to_vec()));
        Ok(true)
    })?;
    Ok(out)
}

/// Expected scan result, computed without `BTreeMap::range` (which panics on some empty ranges).
fn expected(model: &Model, q: Scan<'_>) -> Entries {
    let mut out: Entries = model
        .iter()
        .filter(|(k, _)| q.contains(k))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    if q.reverse {
        out.reverse();
    }
    out
}

fn scan_matches<T: ReadTxn + ?Sized>(
    check: &str,
    label: &str,
    t: &T,
    table: Table,
    model: &Model,
    q: Scan<'_>,
) -> Result<()> {
    let got = collect(t, table, q)?;
    ensure_entries(
        check,
        || format!("{label}: {}", q.describe(table)),
        &got,
        &expected(model, q),
    )
}

/// Everything `t` shows of `table` equals `model`: len, point gets and full scans both ways.
fn check_table<T: ReadTxn + ?Sized>(
    check: &str,
    label: &str,
    t: &T,
    table: Table,
    model: &Model,
) -> Result<()> {
    let len = t.len(table)?;
    ensure(len == model.len() as u64, check, || {
        format!("{label}: len({table:?}) = {len}, want {}", model.len())
    })?;
    for (k, v) in model {
        let got = t.get(table, k)?;
        ensure_value(
            check,
            || format!("{label}: get({table:?}, {})", show(k)),
            got.as_deref(),
            Some(v.as_slice()),
        )?;
    }
    // get_many: the same values, in order, None for a missing key.
    let missing: &[u8] = b"\x00get_many missing\xff";
    let mut keys: Vec<&[u8]> = model.keys().map(Vec::as_slice).collect();
    if !model.contains_key(missing) {
        keys.insert(keys.len() / 2, missing);
    }
    let got = t.get_many(table, &keys)?;
    ensure(got.len() == keys.len(), check, || {
        format!("{label}: get_many({table:?}) returned {} values for {} keys", got.len(), keys.len())
    })?;
    for (k, v) in keys.iter().zip(&got) {
        ensure_value(
            check,
            || format!("{label}: get_many({table:?}) at {}", show(k)),
            v.as_deref(),
            model.get(*k).map(Vec::as_slice),
        )?;
    }
    for reverse in [false, true] {
        scan_matches(check, label, t, table, model, Scan::all(reverse))?;
    }
    Ok(())
}

fn model_of(entries: &[(&str, &str)]) -> Model {
    entries
        .iter()
        .map(|(k, v)| (k.as_bytes().to_vec(), v.as_bytes().to_vec()))
        .collect()
}

fn put_all<W: WriteTxn + ?Sized>(w: &mut W, table: Table, model: &Model) -> Result<()> {
    for (k, v) in model {
        w.put(table, k, v)?;
    }
    Ok(())
}

fn commit_model<S: Store>(
    s: &S,
    table: Table,
    model: &Model,
    durability: Durability,
) -> Result<()> {
    let mut w = s.begin_write()?;
    put_all(&mut w, table, model)?;
    w.commit(durability)
}

/// Run every functional check against stores produced by `make` (each call must return a
/// fresh, empty store).
pub fn run_all<S: Store>(make: &mut dyn FnMut() -> S) -> Result<()> {
    basic_operations(&make())?;
    tables_are_independent(&make())?;
    read_your_writes(&make())?;
    drop_aborts(&make())?;
    snapshot_isolation(&make())?;
    scan_bounds(&make())?;
    scan_control(&make())?;
    nested_reads(&make())?;
    binary_keys(&make())?;
    value_sizes(&make())?;
    many_keys(&make())?;
    compaction(&mut make())
}

fn basic_operations<S: Store>(s: &S) -> Result<()> {
    const C: &str = "basic_operations";
    ensure(!s.backend_name().is_empty(), C, || {
        "backend_name() is empty".into()
    })?;
    for f in s.files() {
        ensure(f.exists(), C, || {
            format!("files() lists {}, which does not exist", f.display())
        })?;
    }
    let r = s.begin_read()?;
    for table in Table::ALL {
        check_table(C, "fresh store", &r, table, &Model::new())?;
    }
    let missing = r.get(RECORDS, b"missing")?;
    ensure_value(
        C,
        || "fresh store: get(missing)".into(),
        missing.as_deref(),
        None,
    )?;
    drop(r);

    let mut w = s.begin_write()?;
    ensure(!w.remove(RECORDS, b"missing")?, C, || {
        "remove of a missing key returned true".into()
    })?;
    w.put(RECORDS, b"k1", b"v1")?;
    w.put(RECORDS, b"k2", b"v2")?;
    w.put(RECORDS, b"k3", b"")?;
    w.put(RECORDS, b"k2", b"v2 overwritten")?;
    ensure(w.remove(RECORDS, b"k1")?, C, || {
        "remove of a key put in the same transaction returned false".into()
    })?;
    ensure(!w.remove(RECORDS, b"k1")?, C, || {
        "second remove of a key returned true".into()
    })?;
    w.put(RECORDS, b"k1", b"v1 again")?;
    w.commit(Durability::Immediate)?;
    // k3 holds an empty value: present, and distinct from a missing key
    let mut model = model_of(&[("k1", "v1 again"), ("k2", "v2 overwritten"), ("k3", "")]);
    check_table(
        C,
        "after the first commit",
        &s.begin_read()?,
        RECORDS,
        &model,
    )?;

    let mut w = s.begin_write()?;
    w.put(RECORDS, b"k2", b"v2 third")?;
    ensure(w.remove(RECORDS, b"k3")?, C, || {
        "remove of a committed key returned false".into()
    })?;
    w.put(RECORDS, b"k4", b"v4")?;
    w.commit(Durability::Deferred)?;
    model.insert(b"k2".to_vec(), b"v2 third".to_vec());
    model.remove(b"k3".as_slice());
    model.insert(b"k4".to_vec(), b"v4".to_vec());
    check_table(
        C,
        "after the second commit",
        &s.begin_read()?,
        RECORDS,
        &model,
    )?;

    let mut w = s.begin_write()?;
    for k in model.keys() {
        ensure(w.remove(RECORDS, k)?, C, || {
            format!("remove({}) returned false", show(k))
        })?;
    }
    w.commit(Durability::Immediate)?;
    check_table(
        C,
        "after removing everything",
        &s.begin_read()?,
        RECORDS,
        &Model::new(),
    )
}

fn tables_are_independent<S: Store>(s: &S) -> Result<()> {
    const C: &str = "tables_are_independent";
    let mut models = Vec::new();
    let mut w = s.begin_write()?;
    for (i, table) in Table::ALL.into_iter().enumerate() {
        let mut m = Model::new();
        m.insert(b"shared".to_vec(), table.name().as_bytes().to_vec());
        for j in 0..=i {
            m.insert(format!("only-{i}-{j}").into_bytes(), vec![i as u8; j]);
        }
        put_all(&mut w, table, &m)?;
        models.push(m);
    }
    w.commit(Durability::Immediate)?;
    let r = s.begin_read()?;
    for (table, m) in Table::ALL.into_iter().zip(&models) {
        check_table(C, "after commit", &r, table, m)?;
    }
    drop(r);

    let mut w = s.begin_write()?;
    ensure(w.remove(Table::Objects, b"shared")?, C, || {
        "remove(Objects, shared) returned false".into()
    })?;
    w.commit(Durability::Immediate)?;
    let r = s.begin_read()?;
    for (table, m) in Table::ALL.into_iter().zip(&mut models) {
        if table == Table::Objects {
            m.remove(b"shared".as_slice());
        }
        check_table(C, "after removing a key from one table", &r, table, m)?;
    }
    Ok(())
}

fn read_your_writes<S: Store>(s: &S) -> Result<()> {
    const C: &str = "read_your_writes";
    let committed = model_of(&[("a", "1"), ("b", "2"), ("c", "3")]);
    let meta = model_of(&[("m", "meta")]);
    commit_model(s, RECORDS, &committed, Durability::Immediate)?;
    commit_model(s, Table::Meta, &meta, Durability::Immediate)?;

    let mut w = s.begin_write()?;
    w.put(RECORDS, b"b", b"2 new")?;
    ensure(w.remove(RECORDS, b"c")?, C, || {
        "remove of a committed key returned false".into()
    })?;
    w.put(RECORDS, b"d", b"4")?;
    w.put(RECORDS, b"a0", b"")?;
    let pending = model_of(&[("a", "1"), ("a0", ""), ("b", "2 new"), ("d", "4")]);
    let label = "inside the write transaction";
    check_table(C, label, &w, RECORDS, &pending)?;
    scan_matches(
        C,
        label,
        &w,
        RECORDS,
        &pending,
        Scan::new(inc(b"a0"), exc(b"d"), false),
    )?;
    scan_matches(
        C,
        label,
        &w,
        RECORDS,
        &pending,
        Scan::new(exc(b"a"), inc(b"d"), true),
    )?;
    check_table(C, label, &w, Table::Meta, &meta)?;
    let during = s.begin_read()?;
    check_table(
        C,
        "reader while a write transaction is open",
        &during,
        RECORDS,
        &committed,
    )?;
    w.commit(Durability::Deferred)?;
    check_table(C, "after commit", &s.begin_read()?, RECORDS, &pending)?;
    check_table(C, "after commit", &s.begin_read()?, Table::Meta, &meta)?;
    check_table(
        C,
        "reader begun before the commit",
        &during,
        RECORDS,
        &committed,
    )
}

fn drop_aborts<S: Store>(s: &S) -> Result<()> {
    const C: &str = "drop_aborts";
    let records = model_of(&[("k1", "v1")]);
    let meta = model_of(&[("m", "1")]);
    commit_model(s, RECORDS, &records, Durability::Immediate)?;
    commit_model(s, Table::Meta, &meta, Durability::Immediate)?;
    {
        let mut w = s.begin_write()?;
        w.put(RECORDS, b"k1", b"changed")?;
        w.put(RECORDS, b"k2", b"new")?;
        w.remove(Table::Meta, b"m")?;
        for i in 0..1000u32 {
            w.put(Table::Objects, &i.to_be_bytes(), &[7u8; 100])?;
        }
        w.put(Table::Objects, b"big", &vec![1u8; 1 << 20])?;
        // dropped here without commit
    }
    let label = "after dropping a write transaction";
    let r = s.begin_read()?;
    check_table(C, label, &r, RECORDS, &records)?;
    check_table(C, label, &r, Table::Meta, &meta)?;
    check_table(C, label, &r, Table::Objects, &Model::new())?;
    drop(r);

    let mut w = s.begin_write()?;
    check_table(C, "next write transaction", &w, RECORDS, &records)?;
    check_table(
        C,
        "next write transaction",
        &w,
        Table::Objects,
        &Model::new(),
    )?;
    w.put(RECORDS, b"k3", b"v3")?;
    w.commit(Durability::Immediate)?;
    let records = model_of(&[("k1", "v1"), ("k3", "v3")]);
    check_table(
        C,
        "commit after an aborted transaction",
        &s.begin_read()?,
        RECORDS,
        &records,
    )?;

    drop(s.begin_write()?);
    s.begin_write()?.commit(Durability::Deferred)?;
    s.begin_write()?.commit(Durability::Immediate)?;
    check_table(
        C,
        "after empty transactions",
        &s.begin_read()?,
        RECORDS,
        &records,
    )
}

fn snapshot_isolation<S: Store>(s: &S) -> Result<()> {
    const C: &str = "snapshot_isolation";
    let old = model_of(&[("a", "1"), ("b", "1")]);
    let old_meta = model_of(&[("x", "1")]);
    commit_model(s, RECORDS, &old, Durability::Immediate)?;
    commit_model(s, Table::Meta, &old_meta, Durability::Immediate)?;

    let before = s.begin_read()?;
    let mut w = s.begin_write()?;
    w.put(RECORDS, b"a", b"2")?;
    w.remove(RECORDS, b"b")?;
    w.put(RECORDS, b"c", b"1")?;
    w.put(Table::Meta, b"x", b"2")?;
    let during = s.begin_read()?;
    check_table(
        C,
        "reader begun during the write transaction",
        &during,
        RECORDS,
        &old,
    )?;
    w.commit(Durability::Immediate)?;
    let new = model_of(&[("a", "2"), ("c", "1")]);
    let new_meta = model_of(&[("x", "2")]);
    let after = s.begin_read()?;
    for (label, r, records, meta) in [
        ("reader begun before the commit", &before, &old, &old_meta),
        (
            "reader begun during the write transaction",
            &during,
            &old,
            &old_meta,
        ),
        ("reader begun after the commit", &after, &new, &new_meta),
    ] {
        check_table(C, label, r, RECORDS, records)?;
        check_table(C, label, r, Table::Meta, meta)?;
    }

    let mut latest = new.clone();
    for i in 0..3 {
        let k = format!("d{i}");
        let mut w = s.begin_write()?;
        w.put(RECORDS, k.as_bytes(), b"later")?;
        w.commit(Durability::Deferred)?;
        latest.insert(k.into_bytes(), b"later".to_vec());
    }
    check_table(C, "old snapshot after more commits", &before, RECORDS, &old)?;
    check_table(
        C,
        "newer snapshot after more commits",
        &after,
        RECORDS,
        &new,
    )?;
    drop(after);
    drop(during);
    check_table(
        C,
        "old snapshot after newer ones closed",
        &before,
        RECORDS,
        &old,
    )?;
    check_table(C, "fresh reader", &s.begin_read()?, RECORDS, &latest)
}

fn scan_bounds<S: Store>(s: &S) -> Result<()> {
    const C: &str = "scan_bounds";
    let keys: [&[u8]; 7] = [b"a", b"b", b"b\x00", b"ba", b"c", b"e", b"\xff"];
    // every key, gaps between keys, both ends, and the empty slice
    let probes: [&[u8]; 15] = [
        b"",
        b"\x00",
        b"a",
        b"aa",
        b"b",
        b"b\x00",
        b"b\x00\x00",
        b"ba",
        b"bb",
        b"c",
        b"d",
        b"e",
        b"f",
        b"\xff",
        b"\xff\xff",
    ];
    let model: Model = keys
        .iter()
        .map(|&k| (k.to_vec(), [b"val-".as_slice(), k].concat()))
        .collect();
    commit_model(s, RECORDS, &model, Durability::Immediate)?;
    // neighbours in other tables must never show up in a scan of Records
    commit_model(
        s,
        Table::Meta,
        &model_of(&[("a", "meta"), ("z", "meta")]),
        Durability::Immediate,
    )?;
    commit_model(
        s,
        Table::Objects,
        &model_of(&[("0", "obj"), ("bb", "obj")]),
        Durability::Immediate,
    )?;

    all_bounds(C, "reader", &s.begin_read()?, &model, &probes)?;

    let mut w = s.begin_write()?;
    w.remove(RECORDS, b"c")?;
    w.put(RECORDS, b"d", b"val-d")?;
    w.put(RECORDS, b"b", b"val-b2")?;
    let mut pending = model.clone();
    pending.remove(b"c".as_slice());
    pending.insert(b"d".to_vec(), b"val-d".to_vec());
    pending.insert(b"b".to_vec(), b"val-b2".to_vec());
    all_bounds(C, "write transaction", &w, &pending, &probes)
}

/// Every combination of Unbounded/Included/Excluded bounds over `probes`, both directions,
/// including inverted and empty ranges.
fn all_bounds<T: ReadTxn + ?Sized>(
    check: &str,
    label: &str,
    t: &T,
    model: &Model,
    probes: &[&[u8]],
) -> Result<()> {
    let mut bounds = vec![Unbounded];
    for &p in probes {
        bounds.push(Included(p));
        bounds.push(Excluded(p));
    }
    for &start in &bounds {
        for &end in &bounds {
            for reverse in [false, true] {
                scan_matches(
                    check,
                    label,
                    t,
                    RECORDS,
                    model,
                    Scan::new(start, end, reverse),
                )?;
            }
        }
    }
    Ok(())
}

fn scan_control<S: Store>(s: &S) -> Result<()> {
    const C: &str = "scan_control";
    let model: Model = (0..300u32)
        .map(|i| (format!("k{i:04}").into_bytes(), i.to_le_bytes().to_vec()))
        .collect();
    commit_model(s, RECORDS, &model, Durability::Immediate)?;
    stop_and_errors(C, "reader", &s.begin_read()?, &model)?;

    let mut w = s.begin_write()?;
    w.put(RECORDS, b"k0150+", b"pending")?;
    let mut pending = model.clone();
    pending.insert(b"k0150+".to_vec(), b"pending".to_vec());
    stop_and_errors(C, "write transaction", &w, &pending)
}

fn stop_and_errors<T: ReadTxn + ?Sized>(
    check: &str,
    label: &str,
    t: &T,
    model: &Model,
) -> Result<()> {
    for reverse in [false, true] {
        let all = expected(model, Scan::all(reverse));
        let n = all.len();
        // around typical page and batch boundaries, and past the end
        for stop_after in [
            1,
            2,
            31,
            32,
            33,
            95,
            96,
            97,
            223,
            224,
            225,
            n.saturating_sub(1),
            n,
            n + 1,
        ] {
            let mut seen = Entries::new();
            t.scan(RECORDS, Unbounded, Unbounded, reverse, &mut |k, v| {
                seen.push((k.to_vec(), v.to_vec()));
                Ok(seen.len() < stop_after)
            })?;
            let want = all.get(..stop_after.min(n)).unwrap_or_default();
            ensure_entries(
                check,
                || format!("{label}: scan stopped after {stop_after} entries, reverse={reverse}"),
                &seen,
                want,
            )?;
        }

        let bounded = Scan::new(inc(b"k0100"), exc(b"k0200"), reverse);
        let mut seen = Entries::new();
        t.scan(RECORDS, bounded.start, bounded.end, reverse, &mut |k, v| {
            seen.push((k.to_vec(), v.to_vec()));
            Ok(seen.len() < 10)
        })?;
        let want = expected(model, bounded);
        let want = want.get(..10.min(want.len())).unwrap_or_default();
        ensure_entries(
            check,
            || {
                format!(
                    "{label}: {} stopped after 10 entries",
                    bounded.describe(RECORDS)
                )
            },
            &seen,
            want,
        )?;

        let mut calls = 0;
        let result = t.scan(RECORDS, Unbounded, Unbounded, reverse, &mut |_, _| {
            calls += 1;
            if calls == 3 {
                Err(Error::InvalidArgument(STOP_MARKER.into()))
            } else {
                Ok(true)
            }
        });
        match result {
            Err(Error::InvalidArgument(m)) if m == STOP_MARKER => {}
            other => {
                return Err(fail(
                    check,
                    format!("{label}: the callback error was not returned as is, got {other:?}"),
                ));
            }
        }
        ensure(calls == 3, check, || {
            format!(
                "{label}: callback called {calls} times, want 3 (the first error ends the scan)"
            )
        })?;
    }
    Ok(())
}

fn nested_reads<S: Store>(s: &S) -> Result<()> {
    const C: &str = "nested_reads";
    let mut records: Model = (0..100u32)
        .map(|i| {
            (
                format!("n{i:03}").into_bytes(),
                format!("value {i}").into_bytes(),
            )
        })
        .collect();
    let mut meta = model_of(&[("meta-a", "1")]);
    commit_model(s, RECORDS, &records, Durability::Immediate)?;
    commit_model(s, Table::Meta, &meta, Durability::Immediate)?;
    nested(C, "reader", &s.begin_read()?, &records, &meta)?;

    let mut w = s.begin_write()?;
    w.put(RECORDS, b"n050+", b"pending")?;
    w.put(Table::Meta, b"meta-b", b"2")?;
    records.insert(b"n050+".to_vec(), b"pending".to_vec());
    meta.insert(b"meta-b".to_vec(), b"2".to_vec());
    nested(C, "write transaction", &w, &records, &meta)
}

/// Reads issued from inside a scan callback, through the transaction being scanned.
fn nested<T: ReadTxn + ?Sized>(
    check: &str,
    label: &str,
    t: &T,
    records: &Model,
    meta: &Model,
) -> Result<()> {
    for reverse in [false, true] {
        let mut visited = 0usize;
        t.scan(RECORDS, Unbounded, Unbounded, reverse, &mut |k, v| {
            visited += 1;
            let same = t.get(RECORDS, k)?;
            ensure_value(
                check,
                || {
                    format!(
                        "{label}: get(Records, {}) inside a scan of Records",
                        show(k)
                    )
                },
                same.as_deref(),
                Some(v),
            )?;
            for (mk, mv) in meta {
                let other = t.get(Table::Meta, mk)?;
                ensure_value(
                    check,
                    || format!("{label}: get(Meta, {}) inside a scan of Records", show(mk)),
                    other.as_deref(),
                    Some(mv.as_slice()),
                )?;
            }
            let n = t.len(RECORDS)?;
            ensure(n == records.len() as u64, check, || {
                format!(
                    "{label}: len(Records) inside a scan = {n}, want {}",
                    records.len()
                )
            })?;
            let one = collect(t, RECORDS, Scan::new(Included(k), Included(k), !reverse))?;
            ensure_entries(
                check,
                || format!("{label}: scan of one key inside a scan"),
                &one,
                &[(k.to_vec(), v.to_vec())],
            )?;
            Ok(true)
        })?;
        ensure(visited == records.len(), check, || {
            format!(
                "{label}: the outer scan visited {visited} entries, want {}",
                records.len()
            )
        })?;
    }
    Ok(())
}

/// Smallest key above every key that starts with `prefix` (None: no such key).
fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < 0xff {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

fn binary_keys<S: Store>(s: &S) -> Result<()> {
    const C: &str = "binary_keys";
    let keys: Vec<Vec<u8>> = vec![
        vec![0x00],
        vec![0x00, 0x00],
        vec![0x00, 0xff],
        vec![0x01],
        vec![0x7f],
        vec![0x80],
        vec![0xfe, 0xff],
        vec![0xff],
        vec![0xff, 0x00],
        vec![0xff, 0xff],
        vec![0xff, 0xff, 0xff],
        b"a".to_vec(),
        b"a\x00b".to_vec(),
        b"a\xff".to_vec(),
        vec![0x00; 511],
        vec![0xff; 511],
        (0..=255u8).collect(),
        (0..=255u8).rev().collect(),
    ];
    let model: Model = keys
        .iter()
        .enumerate()
        .map(|(i, k)| (k.clone(), pattern(i as u64, i % 7)))
        .collect();
    let mut w = s.begin_write()?;
    // descending insertion: nothing may depend on insertion order
    for (k, v) in model.iter().rev() {
        w.put(RECORDS, k, v)?;
    }
    check_table(C, "inside the write transaction", &w, RECORDS, &model)?;
    w.commit(Durability::Immediate)?;
    let r = s.begin_read()?;
    check_table(C, "reader", &r, RECORDS, &model)?;
    let prefixes: [&[u8]; 6] = [b"\x00", b"\x00\x00", b"\x7f", b"\xff", b"\xff\xff", b"a"];
    for prefix in prefixes {
        let end = prefix_end(prefix);
        let end = end.as_deref().map_or(Unbounded, Excluded);
        for reverse in [false, true] {
            scan_matches(
                C,
                "prefix scan",
                &r,
                RECORDS,
                &model,
                Scan::new(inc(prefix), end, reverse),
            )?;
        }
    }
    Ok(())
}

/// Deterministic pseudo-random bytes (xorshift), so misplaced or truncated values show.
fn pattern(seed: u64, len: usize) -> Vec<u8> {
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 32) as u8
        })
        .collect()
}

fn value_sizes<S: Store>(s: &S) -> Result<()> {
    const C: &str = "value_sizes";
    const SIZES: [usize; 9] = [0, 1, 100, 4095, 4096, 4097, 65_536, 1 << 20, (1 << 20) + 1];
    let mut model: Model = SIZES
        .iter()
        .enumerate()
        .map(|(i, &n)| (format!("size-{n:08}").into_bytes(), pattern(i as u64, n)))
        .collect();
    let mut w = s.begin_write()?;
    put_all(&mut w, RECORDS, &model)?;
    check_table(C, "inside the write transaction", &w, RECORDS, &model)?;
    w.commit(Durability::Immediate)?;
    check_table(C, "after commit", &s.begin_read()?, RECORDS, &model)?;

    // grow the small values and shrink the large ones
    let mut w = s.begin_write()?;
    for (i, ((k, v), &n)) in model.iter_mut().zip(SIZES.iter().rev()).enumerate() {
        *v = pattern(100 + i as u64, n);
        w.put(RECORDS, k, v)?;
    }
    w.commit(Durability::Deferred)?;
    check_table(
        C,
        "after resizing every value",
        &s.begin_read()?,
        RECORDS,
        &model,
    )
}

fn many_keys<S: Store>(s: &S) -> Result<()> {
    const C: &str = "many_keys";
    const N: u64 = 10_000;
    let table = Table::Objects;
    let mut model = Model::new();
    let mut w = s.begin_write()?;
    // a permutation of 0..N (7919 is prime and does not divide N)
    for i in 0..N {
        let id = (i * 7919) % N;
        let v = format!("object {id}").into_bytes();
        w.put(table, &id.to_be_bytes(), &v)?;
        model.insert(id.to_be_bytes().to_vec(), v);
    }
    check_table(C, "inside the write transaction", &w, table, &model)?;
    w.commit(Durability::Immediate)?;
    let r = s.begin_read()?;
    check_table(C, "reader", &r, table, &model)?;
    let (lo, hi) = (1234u64.to_be_bytes(), 8765u64.to_be_bytes());
    for reverse in [false, true] {
        // ranges spanning many pages, and inverted ones whose ends lie pages apart
        for (start, end) in [
            (inc(&lo), exc(&hi)),
            (exc(&lo), inc(&hi)),
            (inc(&hi), inc(&lo)),
            (exc(&hi), exc(&lo)),
            (Unbounded, exc(&lo)),
            (exc(&hi), Unbounded),
        ] {
            scan_matches(
                C,
                "reader",
                &r,
                table,
                &model,
                Scan::new(start, end, reverse),
            )?;
        }
    }
    drop(r);

    let mut w = s.begin_write()?;
    for id in (0..N).step_by(2) {
        let k = id.to_be_bytes();
        ensure(w.remove(table, &k)?, C, || {
            format!("remove({id}) returned false")
        })?;
        model.remove(k.as_slice());
    }
    check_table(
        C,
        "every other key removed, inside the transaction",
        &w,
        table,
        &model,
    )?;
    w.commit(Durability::Deferred)?;
    check_table(
        C,
        "every other key removed",
        &s.begin_read()?,
        table,
        &model,
    )
}

fn compaction<S: Store>(s: &mut S) -> Result<()> {
    const C: &str = "compaction";
    let mut model = Model::new();
    let mut w = s.begin_write()?;
    for i in 0..2000u32 {
        let v = pattern(u64::from(i), 1000);
        w.put(Table::Objects, &i.to_be_bytes(), &v)?;
        model.insert(i.to_be_bytes().to_vec(), v);
    }
    w.commit(Durability::Immediate)?;
    let mut w = s.begin_write()?;
    for i in (0..2000u32).filter(|i| !i.is_multiple_of(4)) {
        w.remove(Table::Objects, &i.to_be_bytes())?;
        model.remove(i.to_be_bytes().as_slice());
    }
    w.commit(Durability::Immediate)?;
    // Ok(false) (unsupported) is fine; the data must be intact either way
    s.compact()?;
    check_table(C, "after compact", &s.begin_read()?, Table::Objects, &model)?;
    let after = model_of(&[("after", "compact")]);
    commit_model(s, RECORDS, &after, Durability::Immediate)?;
    check_table(
        C,
        "a commit after compact",
        &s.begin_read()?,
        RECORDS,
        &after,
    )?;
    check_table(
        C,
        "a commit after compact",
        &s.begin_read()?,
        Table::Objects,
        &model,
    )
}

/// Commit, close, reopen. `dir` must be an empty directory and every `open(dir)` must open
/// (creating it the first time) the same store.
pub fn run_persistent<S: Store>(open: &mut dyn FnMut(&Path) -> S, dir: &Path) -> Result<()> {
    const C: &str = "persistence";
    let mut records: Model = (0..1000u32)
        .map(|i| {
            (
                format!("p{i:05}").into_bytes(),
                pattern(u64::from(i), (i % 64) as usize),
            )
        })
        .collect();
    let mut objects = Model::new();
    objects.insert(b"big".to_vec(), pattern(99, 1 << 20));
    let mut meta = model_of(&[("m", "1")]);
    {
        let s = open(dir);
        ensure(!s.files().is_empty(), C, || {
            "files() is empty for a persistent store".into()
        })?;
        let mut w = s.begin_write()?;
        put_all(&mut w, RECORDS, &records)?;
        put_all(&mut w, Table::Objects, &objects)?;
        put_all(&mut w, Table::Meta, &meta)?;
        w.commit(Durability::Immediate)?;
        let mut w = s.begin_write()?;
        w.put(RECORDS, b"uncommitted", b"x")?;
        w.remove(Table::Meta, b"m")?;
        drop(w);
    }
    {
        let s = open(dir);
        let r = s.begin_read()?;
        for (table, m) in [
            (RECORDS, &records),
            (Table::Objects, &objects),
            (Table::Meta, &meta),
        ] {
            check_table(C, "after reopen", &r, table, m)?;
        }
        drop(r);

        let mut w = s.begin_write()?;
        w.put(RECORDS, b"deferred-1", b"d1")?;
        w.remove(RECORDS, b"p00000")?;
        w.commit(Durability::Deferred)?;
        records.insert(b"deferred-1".to_vec(), b"d1".to_vec());
        records.remove(b"p00000".as_slice());
        check_table(
            C,
            "right after a deferred commit",
            &s.begin_read()?,
            RECORDS,
            &records,
        )?;
        let mut w = s.begin_write()?;
        w.put(RECORDS, b"deferred-2", b"d2")?;
        w.commit(Durability::Deferred)?;
        records.insert(b"deferred-2".to_vec(), b"d2".to_vec());
        let mut w = s.begin_write()?;
        w.put(Table::Meta, b"m", b"2")?;
        w.commit(Durability::Immediate)?;
        meta.insert(b"m".to_vec(), b"2".to_vec());
    }
    let s = open(dir);
    let r = s.begin_read()?;
    let label = "deferred commits, then an immediate one, then reopen";
    let empty = Model::new();
    for table in Table::ALL {
        let m = match table {
            Table::Records => &records,
            Table::Objects => &objects,
            Table::Meta => &meta,
            _ => &empty,
        };
        check_table(C, label, &r, table, m)?;
    }
    let mut bytes = 0;
    for f in s.files() {
        bytes += std::fs::metadata(&f)?.len();
    }
    ensure(bytes > 0, C, || {
        "the files() of a store holding data are empty".into()
    })
}

/// Blocking rules and snapshot consistency across threads. `store` must be empty.
pub fn run_concurrent<S: Store>(store: Arc<S>) -> Result<()> {
    readers_do_not_wait(&*store)?;
    writers_serialize(&*store)?;
    snapshots_under_commits(&*store)
}

/// `begin_read` + `get` on another thread complete while a write transaction is open.
fn readers_do_not_wait<S: Store>(store: &S) -> Result<()> {
    const C: &str = "readers_do_not_wait";
    commit_model(
        store,
        Table::Meta,
        &model_of(&[("rdnw", "committed")]),
        Durability::Immediate,
    )?;
    let (tx, rx) = mpsc::channel();
    thread::scope(|scope| {
        let mut w = store.begin_write()?;
        w.put(Table::Meta, b"rdnw", b"pending")?;
        scope.spawn(move || {
            let seen = store.begin_read().and_then(|r| r.get(Table::Meta, b"rdnw"));
            let _ = tx.send(seen);
        });
        let got = rx.recv_timeout(WAIT);
        // releases a reader that (wrongly) waits for the writer
        drop(w);
        match got {
            Ok(seen) => ensure_value(
                C,
                || "reader on another thread while a write transaction is open".into(),
                seen?.as_deref(),
                Some(b"committed"),
            ),
            Err(_) => Err(fail(
                C,
                format!("a reader on another thread waited over {WAIT:?} for the writer"),
            )),
        }
    })
}

/// A second `begin_write` returns only once the first transaction committed, and sees it.
fn writers_serialize<S: Store>(store: &S) -> Result<()> {
    const C: &str = "writers_serialize";
    let committed = AtomicBool::new(false);
    let (started_tx, started_rx) = mpsc::channel();
    let (seen_tx, seen_rx) = mpsc::channel();
    thread::scope(|scope| {
        let mut w = store.begin_write()?;
        w.put(Table::Meta, b"serial", b"first")?;
        let committed = &committed;
        let second = scope.spawn(move || -> Result<()> {
            let _ = started_tx.send(());
            let mut w2 = store.begin_write()?;
            let flag = committed.load(Ordering::Acquire);
            let _ = seen_tx.send((flag, w2.get(Table::Meta, b"serial")?));
            w2.put(Table::Meta, b"serial", b"second")?;
            w2.commit(Durability::Immediate)
        });
        started_rx
            .recv_timeout(WAIT)
            .map_err(|_| fail(C, "the second writer thread did not start"))?;
        // with the first transaction still open, give the second writer time to (wrongly) get in
        thread::sleep(Duration::from_millis(100));
        committed.store(true, Ordering::Release);
        w.commit(Durability::Immediate)?;
        let (flag, seen) = seen_rx.recv_timeout(WAIT).map_err(|_| {
            fail(
                C,
                "the second begin_write did not return after the first commit",
            )
        })?;
        ensure(flag, C, || {
            "begin_write returned while another write transaction was open".into()
        })?;
        ensure_value(
            C,
            || "second writer: get(Meta, serial)".into(),
            seen.as_deref(),
            Some(b"first"),
        )?;
        second
            .join()
            .map_err(|_| fail(C, "the second writer thread panicked"))??;
        let last = store.begin_read()?.get(Table::Meta, b"serial")?;
        ensure_value(
            C,
            || "after both commits".into(),
            last.as_deref(),
            Some(b"second"),
        )
    })
}

const READERS: usize = 8;
const GENERATIONS: u64 = 200;
const GEN_KEYS: usize = 64;
/// Generations whose marker stays in `History`.
const KEEP: u64 = 5;
const MIN_SNAPSHOTS: u64 = 20;

/// One commit: every Records key and the Params counter take the value `g`; History holds
/// one marker per generation for the last `KEEP` generations.
fn write_generation<W: WriteTxn + ?Sized>(w: &mut W, g: u64) -> Result<()> {
    for i in 0..GEN_KEYS {
        w.put(RECORDS, format!("c/{i:03}").as_bytes(), &g.to_be_bytes())?;
    }
    w.put(Table::Params, b"gen", &g.to_be_bytes())?;
    if g > 0 {
        w.put(Table::History, &g.to_be_bytes(), b"marker")?;
    }
    if g > KEEP {
        w.remove(Table::History, &(g - KEEP).to_be_bytes())?;
    }
    Ok(())
}

/// Checks that a snapshot holds exactly one generation and returns it.
fn check_generation<T: ReadTxn + ?Sized>(t: &T) -> Result<u64> {
    const C: &str = "concurrent_snapshots";
    let raw = t
        .get(Table::Params, b"gen")?
        .ok_or_else(|| fail(C, "the generation counter is missing"))?;
    let g = <[u8; 8]>::try_from(raw.as_slice())
        .map(u64::from_be_bytes)
        .map_err(|_| fail(C, format!("bad generation counter {}", show(&raw))))?;
    let want = g.to_be_bytes();
    let mut seen = 0usize;
    let mut mixed = None;
    t.scan(RECORDS, Unbounded, Unbounded, false, &mut |k, v| {
        seen += 1;
        if v != want.as_slice() {
            mixed = Some(format!("{} = {}", show(k), show(v)));
            return Ok(false);
        }
        Ok(true)
    })?;
    if let Some(entry) = mixed {
        return Err(fail(
            C,
            format!("snapshot of generation {g} also holds {entry}"),
        ));
    }
    let len = t.len(RECORDS)?;
    ensure(seen == GEN_KEYS && len == GEN_KEYS as u64, C, || {
        format!("generation {g}: scan saw {seen} keys and len says {len}, want {GEN_KEYS}")
    })?;
    let markers = collect(t, Table::History, Scan::all(true))?;
    let expected_markers: Entries = (g.saturating_sub(KEEP - 1).max(1)..=g)
        .rev()
        .map(|x| (x.to_be_bytes().to_vec(), b"marker".to_vec()))
        .collect();
    ensure_entries(
        C,
        || format!("History markers in a snapshot of generation {g}"),
        &markers,
        &expected_markers,
    )?;
    Ok(g)
}

fn read_generations<S: Store>(store: &S, done: &AtomicBool) -> Result<u64> {
    const C: &str = "concurrent_snapshots";
    let mut last = 0;
    let mut snapshots = 0;
    loop {
        let finished = done.load(Ordering::Acquire);
        let g = check_generation(&store.begin_read()?)?;
        ensure(g >= last, C, || {
            format!("a later snapshot went back from generation {last} to {g}")
        })?;
        ensure(!finished || g == GENERATIONS, C, || {
            format!(
                "a snapshot begun after the last commit returned sees generation {g}, want {GENERATIONS}"
            )
        })?;
        last = g;
        snapshots += 1;
        if finished && snapshots >= MIN_SNAPSHOTS {
            return Ok(snapshots);
        }
    }
}

/// `READERS` threads keep taking snapshots while this thread commits `GENERATIONS` times.
fn snapshots_under_commits<S: Store>(store: &S) -> Result<()> {
    const C: &str = "concurrent_snapshots";
    let mut w = store.begin_write()?;
    write_generation(&mut w, 0)?;
    w.commit(Durability::Immediate)?;
    let done = AtomicBool::new(false);
    thread::scope(|scope| {
        let readers: Vec<_> = (0..READERS)
            .map(|_| scope.spawn(|| read_generations(store, &done)))
            .collect();
        let written = (1..=GENERATIONS).try_for_each(|g| {
            let mut w = store.begin_write()?;
            write_generation(&mut w, g)?;
            w.commit(if g.is_multiple_of(50) {
                Durability::Immediate
            } else {
                Durability::Deferred
            })
        });
        done.store(true, Ordering::Release);
        let mut result = written;
        for reader in readers {
            let outcome = reader
                .join()
                .unwrap_or_else(|_| Err(fail(C, "a reader thread panicked")));
            if result.is_ok() {
                result = outcome.map(|_| ());
            }
        }
        result
    })?;
    let g = check_generation(&store.begin_read()?)?;
    ensure(g == GENERATIONS, C, || {
        format!("final generation {g}, want {GENERATIONS}")
    })
}
