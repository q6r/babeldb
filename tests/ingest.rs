//! Streaming ingestion: `Db::import` (any `ByteSource`) and `Db::import_file`.
//!
//! - Exact bytes: zeros, CRLF vs LF vs CR, trailing spaces, invalid UTF-8,
//!   BOMs, all 256 byte values, the empty file and sizes around `inline_max`
//!   and `block_size`, in Adaptive and BabelPure, on MemStore and redb
//!   (after reopen too); short reads and "0 means end of input".
//! - A confirmed import stays readable after its source file is deleted.
//! - `sources()` describes the import: LOCAL_FILE descriptor reused by
//!   location, `last_import.revision` = returned revision, `digest` =
//!   BLAKE3(file), `bytes` = file length; explicit `source_id` is updated too.
//! - A source failing after K blocks: error, the key keeps its previous value
//!   (or stays absent), and after `gc()` verify is ok with no pending import
//!   and no leftover object (also across a reopen on redb).
//! - Expectations: `Expect::Absent` over an existing key (and a wrong
//!   revision) is a RevisionConflict that changes nothing.
//! - Dedupe: importing the same content twice (or put + import) shares every
//!   object in Adaptive and shares nothing in BabelPure; overwriting/deleting
//!   imported records releases every object.
//!
//! Run: `cargo test --test ingest`; the slow large-file test:
//! `cargo test --test ingest -- --ignored`.

mod common;

use std::io;
use std::path::Path;

use babeldb::format::{SourceDescriptor, source_kind};
use babeldb::ingest::{ByteSource, ImportOptions};
use babeldb::maintenance::GcReport;
use babeldb::source::LOCAL_FILE_ADAPTER_VERSION;
use babeldb::store::Store;
use babeldb::{Config, Db, Error, Expect, Mode, Revision};
use common::{
    BLOCK, INLINE, Pattern, Rng, assert_bytes_eq, is_conflict, key_str, mem_db, open_redb,
    pattern_bytes, small_config, temp_dir, unix_ms, verify_ok,
};

const BS: usize = BLOCK as usize;
const IM: usize = INLINE as usize;

/// Small blocks and small import batches: even modest files span several
/// intermediate import transactions.
fn ingest_config(mode: Mode) -> Config {
    Config {
        import_batch_bytes: 4096,
        ..small_config(mode)
    }
}

fn blake3_of(bytes: &[u8]) -> [u8; 32] {
    *blake3::hash(bytes).as_bytes()
}

/// In-memory source; optionally returns short reads (only 0 means end).
struct MemSource {
    data: Vec<u8>,
    pos: usize,
    short_reads: Option<Rng>,
}

impl MemSource {
    fn new(data: Vec<u8>) -> MemSource {
        MemSource {
            data,
            pos: 0,
            short_reads: None,
        }
    }

    fn short_reads(data: Vec<u8>, seed: u64) -> MemSource {
        MemSource {
            short_reads: Some(Rng::new(seed)),
            ..MemSource::new(data)
        }
    }
}

impl ByteSource for MemSource {
    fn read_block(&mut self, buf: &mut [u8]) -> babeldb::Result<usize> {
        let mut n = (self.data.len() - self.pos).min(buf.len());
        if let Some(rng) = &mut self.short_reads
            && n > 1
        {
            n = 1 + rng.below(n as u64) as usize;
        }
        buf[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

/// Serves `data`, then signals the end (0), then would serve garbage: the
/// importer must stop at the first 0.
struct EndThenGarbage {
    inner: MemSource,
    ended: bool,
}

impl ByteSource for EndThenGarbage {
    fn read_block(&mut self, buf: &mut [u8]) -> babeldb::Result<usize> {
        if self.ended {
            buf.fill(0xEE);
            return Ok(buf.len());
        }
        let n = self.inner.read_block(buf)?;
        if n == 0 {
            self.ended = true;
        }
        Ok(n)
    }
}

/// Serves `ok_reads` successful reads, then fails.
struct FailingSource {
    inner: MemSource,
    ok_reads: usize,
    reads: usize,
}

impl ByteSource for FailingSource {
    fn read_block(&mut self, buf: &mut [u8]) -> babeldb::Result<usize> {
        if self.reads == self.ok_reads {
            return Err(Error::Io(io::Error::other("injected source failure")));
        }
        self.reads += 1;
        self.inner.read_block(buf)
    }
}

fn byte_cases() -> Vec<(String, Vec<u8>)> {
    let all: Vec<u8> = (0..=255u8).collect();
    let mut reversed = all.clone();
    reversed.reverse();
    let mut cases: Vec<(String, Vec<u8>)> = vec![
        ("empty".into(), Vec::new()),
        ("one-zero".into(), vec![0]),
        ("zeros-inline".into(), vec![0; IM]),
        ("zeros-inline-plus-1".into(), vec![0; IM + 1]),
        ("zeros-block".into(), vec![0; BS]),
        ("zeros-multi".into(), vec![0; 5 * BS + 3]),
        ("lf".into(), b"line one\nline two\n".to_vec()),
        ("crlf".into(), b"line one\r\nline two\r\n".to_vec()),
        ("cr-only".into(), b"old mac\rline\r".to_vec()),
        ("mixed-newlines".into(), b"a\r\nb\nc\rd\n\r\n\n".to_vec()),
        (
            "no-final-newline".into(),
            b"last line without newline".to_vec(),
        ),
        (
            "trailing-spaces".into(),
            b"trailing   \n  \t \n   ".to_vec(),
        ),
        (
            "invalid-utf8".into(),
            vec![
                0xC3, 0x28, 0xA0, 0xA1, 0xE2, 0x28, 0xA1, 0xF0, 0x28, 0x8C, 0xBC, 0xFF, 0xFE, 0xC0,
                0x80, 0xED, 0xA0, 0x80,
            ],
        ),
        (
            "utf8-bom".into(),
            "\u{FEFF}ção 日本語 🦀\n".as_bytes().to_vec(),
        ),
        ("utf16le-bom".into(), vec![0xFF, 0xFE, b'h', 0, b'i', 0]),
        ("all-256".into(), all.clone()),
        ("all-256-reversed".into(), reversed),
        ("all-256-x5".into(), all.repeat(5)),
    ];
    for len in [IM - 1, IM, IM + 1, BS - 1, BS, BS + 1, 2 * BS, 3 * BS + 7] {
        cases.push((
            format!("mixed-{len}"),
            pattern_bytes(Pattern::Mixed, len, len as u64),
        ));
    }
    cases
}

/// Full read, head, and a few ranges of an imported record.
#[track_caller]
fn assert_record<S: Store>(
    db: &Db<S>,
    key: &[u8],
    expected: &[u8],
    rev: Option<Revision>,
    ctx: &str,
) {
    let got = db
        .get(key)
        .unwrap_or_else(|e| panic!("{ctx}: get failed: {e}"))
        .unwrap_or_else(|| panic!("{ctx}: {} is absent", key_str(key)));
    assert_bytes_eq(&got, expected, ctx);
    let (head_rev, len) = db
        .head(key)
        .unwrap_or_else(|e| panic!("{ctx}: head failed: {e}"))
        .unwrap_or_else(|| panic!("{ctx}: head of {} is None", key_str(key)));
    assert_eq!(len, expected.len() as u64, "{ctx}: head length");
    if let Some(rev) = rev {
        assert_eq!(head_rev, rev, "{ctx}: revision returned by the import");
    }
    let n = expected.len() as u64;
    for (off, len) in [
        (0, 1),
        (n / 2, 7),
        (n.saturating_sub(1), 1),
        (n, 0),
        (n, 10),
        (0, u64::MAX),
    ] {
        let part = db
            .get_range(key, off, len)
            .unwrap_or_else(|e| panic!("{ctx}: get_range({off}, {len}) failed: {e}"))
            .unwrap_or_else(|| panic!("{ctx}: get_range of a live key is None"));
        let end = off.saturating_add(len).min(n);
        assert_bytes_eq(
            &part,
            &expected[off as usize..end as usize],
            &format!("{ctx}: get_range({off}, {len})"),
        );
    }
}

#[track_caller]
fn gc_and_check_clean<S: Store>(db: &mut Db<S>, ctx: &str) -> GcReport {
    let gc = db.gc().unwrap_or_else(|e| panic!("{ctx}: gc failed: {e}"));
    let rep = verify_ok(db, true, ctx);
    assert_eq!(rep.pending_imports, 0, "{ctx}: pending imports after gc");
    assert_eq!(rep.orphan_objects, 0, "{ctx}: orphan objects after gc");
    let st = db
        .stats()
        .unwrap_or_else(|e| panic!("{ctx}: stats failed: {e}"));
    assert_eq!(
        st.pending_imports, 0,
        "{ctx}: stats.pending_imports after gc"
    );
    gc
}

/// Import every byte case with `import_file`; returns (key, bytes, revision).
fn import_file_cases<S: Store>(
    db: &Db<S>,
    dir: &Path,
    label: &str,
) -> Vec<(Vec<u8>, Vec<u8>, Revision)> {
    let mut written = Vec::new();
    for (name, bytes) in byte_cases() {
        let path = dir.join(format!("{name}.bin"));
        std::fs::write(&path, &bytes).expect("write the source file");
        let key = format!("file/{name}").into_bytes();
        let ctx = format!("{label} import_file {name}");
        let rev = db
            .import_file(&key, &path, &ImportOptions::default())
            .unwrap_or_else(|e| panic!("{ctx}: failed: {e}"));
        assert_record(db, &key, &bytes, Some(rev), &ctx);
        written.push((key, bytes, rev));
    }
    written
}

#[test]
fn import_file_preserves_exact_bytes_mem() {
    for mode in [Mode::Adaptive, Mode::BabelPure] {
        let dir = temp_dir("babeldb-ingest-");
        let mut db = mem_db(ingest_config(mode));
        let label = format!("mem {mode:?}");
        let written = import_file_cases(&db, dir.path(), &label);
        let crlf = db.get(b"file/crlf").unwrap();
        let lf = db.get(b"file/lf").unwrap();
        assert_ne!(crlf, lf, "{label}: CRLF must not be normalized to LF");
        verify_ok(&db, true, &label);
        let gc = gc_and_check_clean(&mut db, &label);
        assert_eq!(
            gc,
            GcReport::default(),
            "{label}: completed imports leave nothing to collect"
        );
        for (key, bytes, rev) in &written {
            assert_record(&db, key, bytes, Some(*rev), &format!("{label} after gc"));
        }
    }
}

#[test]
fn import_file_preserves_exact_bytes_redb() {
    for mode in [Mode::Adaptive, Mode::BabelPure] {
        let dir = temp_dir("babeldb-ingest-");
        let db_path = dir.path().join("ingest.redb");
        let label = format!("redb {mode:?}");
        let written = {
            let db = open_redb(&db_path, ingest_config(mode));
            import_file_cases(&db, dir.path(), &label)
        };
        let mut db = open_redb(&db_path, ingest_config(mode));
        for (key, bytes, rev) in &written {
            assert_record(
                &db,
                key,
                bytes,
                Some(*rev),
                &format!("{label} after reopen"),
            );
        }
        verify_ok(&db, true, &label);
        gc_and_check_clean(&mut db, &label);
    }
}

#[test]
fn import_from_byte_source_is_exact_for_any_read_pattern() {
    for mode in [Mode::Adaptive, Mode::BabelPure] {
        let mut db = mem_db(ingest_config(mode));
        for (i, (name, bytes)) in byte_cases().into_iter().enumerate() {
            let ctx = format!("{mode:?} {name}");
            let full = format!("src/full/{name}").into_bytes();
            let rev = db
                .import(
                    &full,
                    &mut MemSource::new(bytes.clone()),
                    &ImportOptions::default(),
                )
                .unwrap_or_else(|e| panic!("{ctx}: import failed: {e}"));
            assert_record(
                &db,
                &full,
                &bytes,
                Some(rev),
                &format!("{ctx} (full reads)"),
            );

            let short = format!("src/short/{name}").into_bytes();
            let rev = db
                .import(
                    &short,
                    &mut MemSource::short_reads(bytes.clone(), i as u64),
                    &ImportOptions::default(),
                )
                .unwrap_or_else(|e| panic!("{ctx}: import with short reads failed: {e}"));
            assert_record(
                &db,
                &short,
                &bytes,
                Some(rev),
                &format!("{ctx} (short reads)"),
            );

            let ended = format!("src/end/{name}").into_bytes();
            let mut src = EndThenGarbage {
                inner: MemSource::new(bytes.clone()),
                ended: false,
            };
            let rev = db
                .import(&ended, &mut src, &ImportOptions::default())
                .unwrap_or_else(|e| panic!("{ctx}: import failed: {e}"));
            assert_record(
                &db,
                &ended,
                &bytes,
                Some(rev),
                &format!("{ctx} (0 ends the input)"),
            );

            let put = format!("src/put/{name}").into_bytes();
            db.put(&put, &bytes, Expect::Any).unwrap();
            assert_eq!(
                db.get(&put).unwrap(),
                db.get(&full).unwrap(),
                "{ctx}: put and import agree"
            );
        }
        verify_ok(&db, true, &format!("{mode:?}"));
        let gc = gc_and_check_clean(&mut db, &format!("{mode:?}"));
        assert_eq!(
            gc,
            GcReport::default(),
            "{mode:?}: completed imports leave nothing to collect"
        );
    }
}

#[test]
fn imported_record_survives_source_file_deletion() {
    let dir = temp_dir("babeldb-ingest-");
    let src = dir.path().join("source.txt");
    let db_path = dir.path().join("ingest.redb");
    let content = pattern_bytes(Pattern::Mixed, 7 * BS + 13, 42);
    std::fs::write(&src, &content).unwrap();
    let rev = {
        let db = open_redb(&db_path, ingest_config(Mode::Adaptive));
        let rev = db
            .import_file(b"doc", &src, &ImportOptions::default())
            .unwrap();
        std::fs::remove_file(&src).unwrap();
        assert!(!src.exists());
        assert_record(&db, b"doc", &content, Some(rev), "source deleted");
        db.clear_cache();
        assert_record(
            &db,
            b"doc",
            &content,
            Some(rev),
            "source deleted, cold cache",
        );
        rev
    };
    let db = open_redb(&db_path, ingest_config(Mode::Adaptive));
    assert_record(
        &db,
        b"doc",
        &content,
        Some(rev),
        "source deleted, after reopen",
    );
    let sources = db.sources().unwrap();
    assert_eq!(
        sources.len(),
        1,
        "the descriptor outlives the file: {sources:?}"
    );
    let li = sources[0].1.last_import.as_ref().expect("last_import");
    assert_eq!((li.revision, li.bytes), (rev, content.len() as u64));
    verify_ok(&db, true, "source deleted");
}

#[test]
fn import_file_records_its_source_descriptor() {
    let dir = temp_dir("babeldb-ingest-");
    let db = mem_db(ingest_config(Mode::Adaptive));
    let path = dir.path().join("feed.log");
    let v1 = pattern_bytes(Pattern::Text, 4 * BS + 11, 1);
    std::fs::write(&path, &v1).unwrap();
    let t0 = unix_ms();
    let r1 = db
        .import_file(b"feed", &path, &ImportOptions::default())
        .unwrap();
    let t1 = unix_ms();
    let sources = db.sources().unwrap();
    assert_eq!(
        sources.len(),
        1,
        "import_file registers one LOCAL_FILE source: {sources:?}"
    );
    let (sid, desc) = sources[0].clone();
    assert_eq!(desc.kind, source_kind::LOCAL_FILE);
    assert_eq!(desc.adapter_version, LOCAL_FILE_ADAPTER_VERSION);
    assert!(
        desc.location.contains("feed.log"),
        "location {:?}",
        desc.location
    );
    let li = desc
        .last_import
        .clone()
        .expect("last_import after a confirmed import");
    assert_eq!(
        li.revision, r1,
        "last_import.revision is the returned revision"
    );
    assert_eq!(li.bytes, v1.len() as u64, "last_import.bytes");
    assert_eq!(
        li.digest,
        blake3_of(&v1),
        "last_import.digest is BLAKE3 of the file"
    );
    assert!(
        li.unix_ms + 5_000 >= t0 && li.unix_ms <= t1 + 5_000,
        "last_import.unix_ms {} outside [{t0}, {t1}]",
        li.unix_ms
    );
    let insp = db.inspect(b"feed").unwrap().expect("inspect");
    assert_eq!(
        insp.source,
        Some((sid, desc.clone())),
        "the record references its source"
    );

    // The file changes: importing the same location reuses the descriptor.
    let v2 = pattern_bytes(Pattern::Text, 5 * BS + 3, 2);
    std::fs::write(&path, &v2).unwrap();
    let r2 = db
        .import_file(b"feed", &path, &ImportOptions::default())
        .unwrap();
    assert!(r2 > r1);
    assert_record(&db, b"feed", &v2, Some(r2), "re-import");
    let sources = db.sources().unwrap();
    assert_eq!(
        sources.len(),
        1,
        "same location, same descriptor: {sources:?}"
    );
    assert_eq!(sources[0].0, sid);
    let li = sources[0].1.last_import.clone().expect("last_import");
    assert_eq!(
        (li.revision, li.bytes, li.digest),
        (r2, v2.len() as u64, blake3_of(&v2))
    );

    // Same location into another key: still one descriptor, updated.
    let r3 = db
        .import_file(b"feed-copy", &path, &ImportOptions::default())
        .unwrap();
    let sources = db.sources().unwrap();
    assert_eq!(sources.len(), 1, "{sources:?}");
    assert_eq!(
        sources[0].1.last_import.as_ref().map(|l| l.revision),
        Some(r3)
    );
    assert_eq!(db.stats().unwrap().sources, 1);
    verify_ok(&db, true, "source descriptor");
}

#[test]
fn import_with_registered_source_id_updates_it() {
    let db = mem_db(ingest_config(Mode::Adaptive));
    let desc = SourceDescriptor {
        kind: source_kind::EXTERNAL,
        location: "https://example.invalid/archive.bin".into(),
        adapter_version: 7,
        last_import: None,
    };
    let id = db.register_source(desc.clone()).unwrap();
    assert!(
        db.sources().unwrap().contains(&(id, desc.clone())),
        "registered descriptor listed"
    );
    let data = pattern_bytes(Pattern::Mixed, 6 * BS + 1, 9);
    let opts = ImportOptions {
        expect: Expect::Any,
        source_id: Some(id),
    };
    let rev = db
        .import(b"ext", &mut MemSource::new(data.clone()), &opts)
        .unwrap();
    assert_record(&db, b"ext", &data, Some(rev), "import with source_id");
    let sources = db.sources().unwrap();
    let (_, got) = sources
        .iter()
        .find(|(sid, _)| *sid == id)
        .expect("source still listed");
    assert_eq!(
        (got.kind, &got.location, got.adapter_version),
        (desc.kind, &desc.location, 7)
    );
    let li = got.last_import.as_ref().expect("last_import updated");
    assert_eq!(
        (li.revision, li.bytes, li.digest),
        (rev, data.len() as u64, blake3_of(&data))
    );
    let insp = db.inspect(b"ext").unwrap().expect("inspect");
    assert_eq!(insp.source.map(|(sid, _)| sid), Some(id));
    verify_ok(&db, true, "registered source");
}

struct FailedImport {
    key: Vec<u8>,
    objects_before: u64,
    ctx: String,
}

/// Run an import whose source fails after `ok_reads` reads: it must fail and
/// publish nothing (pending imports are allowed until gc).
fn failing_import_start<S: Store>(
    db: &mut Db<S>,
    ok_reads: usize,
    previous: Option<&[u8]>,
) -> FailedImport {
    let key = format!("fail/{ok_reads}/{}", previous.is_some()).into_bytes();
    let ctx = format!(
        "failing source after {ok_reads} reads, previous value: {}",
        previous.is_some()
    );
    if let Some(p) = previous {
        db.put(&key, p, Expect::Any).unwrap();
    }
    gc_and_check_clean(db, &ctx);
    let objects_before = db.stats().unwrap().objects;
    let head_before = db.head(&key).unwrap();
    let mut src = FailingSource {
        inner: MemSource::new(pattern_bytes(Pattern::Random, 64 * BS, ok_reads as u64)),
        ok_reads,
        reads: 0,
    };
    let err = db
        .import(&key, &mut src, &ImportOptions::default())
        .expect_err("an import whose source fails must fail");
    assert!(
        matches!(err, Error::Io(_)),
        "{ctx}: the source error is reported: {err:?}"
    );
    assert_eq!(
        db.get(&key).unwrap().as_deref(),
        previous,
        "{ctx}: value after the failure"
    );
    assert_eq!(
        db.head(&key).unwrap(),
        head_before,
        "{ctx}: revision after the failure"
    );
    verify_ok(
        db,
        true,
        &format!("{ctx}: before gc (pending imports allowed)"),
    );
    FailedImport {
        key,
        objects_before,
        ctx,
    }
}

/// After gc nothing the failed import prepared may remain.
fn failing_import_finish<S: Store>(db: &mut Db<S>, failed: &FailedImport, previous: Option<&[u8]>) {
    let ctx = &failed.ctx;
    assert_eq!(
        db.get(&failed.key).unwrap().as_deref(),
        previous,
        "{ctx}: value before gc"
    );
    gc_and_check_clean(db, ctx);
    assert_eq!(
        db.stats().unwrap().objects,
        failed.objects_before,
        "{ctx}: objects prepared by the failed import must be collected"
    );
    assert_eq!(
        db.get(&failed.key).unwrap().as_deref(),
        previous,
        "{ctx}: value after gc"
    );
}

#[test]
fn failing_source_publishes_nothing_mem() {
    let mut db = mem_db(ingest_config(Mode::Adaptive));
    let previous = pattern_bytes(Pattern::Text, 3 * BS + 5, 77);
    for ok_reads in [0, 1, 7, 40] {
        for prev in [None, Some(previous.as_slice())] {
            let failed = failing_import_start(&mut db, ok_reads, prev);
            failing_import_finish(&mut db, &failed, prev);
        }
    }
}

#[test]
fn failing_source_publishes_nothing_redb_across_reopen() {
    let dir = temp_dir("babeldb-ingest-");
    let path = dir.path().join("failing.redb");
    let cfg = ingest_config(Mode::Adaptive);
    let previous = pattern_bytes(Pattern::Text, 3 * BS + 5, 77);
    for ok_reads in [0, 7, 40] {
        for prev in [None, Some(previous.as_slice())] {
            let failed = {
                let mut db = open_redb(&path, cfg.clone());
                failing_import_start(&mut db, ok_reads, prev)
            };
            // Closed and reopened between the failure and gc.
            let mut db = open_redb(&path, cfg.clone());
            failing_import_finish(&mut db, &failed, prev);
        }
    }
}

#[test]
fn import_expectations_conflict_without_side_effects() {
    let dir = temp_dir("babeldb-ingest-");
    let mut db = mem_db(ingest_config(Mode::Adaptive));
    let old = pattern_bytes(Pattern::Mixed, 4 * BS + 9, 5);
    let new = pattern_bytes(Pattern::Random, 9 * BS + 1, 6);
    let path = dir.path().join("new.bin");
    std::fs::write(&path, &new).unwrap();
    let r0 = db.put(b"imp/x", &old, Expect::Any).unwrap();
    let objects0 = db.stats().unwrap().objects;
    let unchanged = |db: &Db<babeldb::MemStore>, what: &str| {
        assert_eq!(
            db.get_with_revision(b"imp/x").unwrap(),
            Some((r0, old.clone())),
            "{what}"
        );
    };
    for (what, expect) in [
        ("Absent", Expect::Absent),
        ("wrong revision", Expect::Revision(r0 + 1000)),
    ] {
        let opts = ImportOptions {
            expect,
            source_id: None,
        };
        let err = db
            .import(b"imp/x", &mut MemSource::new(new.clone()), &opts)
            .expect_err("conflicting import");
        assert!(is_conflict(&err), "import with {what}: {err:?}");
        unchanged(&db, &format!("import with {what}"));
        let err = db
            .import_file(b"imp/x", &path, &opts)
            .expect_err("conflicting import_file");
        assert!(is_conflict(&err), "import_file with {what}: {err:?}");
        unchanged(&db, &format!("import_file with {what}"));
    }
    for (_, desc) in db.sources().unwrap() {
        assert!(
            desc.last_import.is_none(),
            "a conflicting import must not record last_import: {desc:?}"
        );
    }
    gc_and_check_clean(&mut db, "after conflicting imports");
    assert_eq!(
        db.stats().unwrap().objects,
        objects0,
        "conflicting imports leave no object behind"
    );
    unchanged(&db, "after gc");
    let opts = ImportOptions {
        expect: Expect::Revision(r0),
        source_id: None,
    };
    let r1 = db
        .import(b"imp/x", &mut MemSource::new(new.clone()), &opts)
        .unwrap();
    assert!(r1 > r0);
    assert_record(
        &db,
        b"imp/x",
        &new,
        Some(r1),
        "import with the current revision",
    );
    let opts = ImportOptions {
        expect: Expect::Absent,
        source_id: None,
    };
    let r2 = db.import_file(b"imp/fresh", &path, &opts).unwrap();
    assert_record(
        &db,
        b"imp/fresh",
        &new,
        Some(r2),
        "import_file with Absent on a new key",
    );
    verify_ok(&db, true, "expectations");
}

fn unit_objects<S: Store>(db: &Db<S>, key: &[u8]) -> Vec<(Option<u64>, Option<u64>)> {
    db.inspect(key)
        .unwrap()
        .unwrap_or_else(|| panic!("inspect({})", key_str(key)))
        .units
        .iter()
        .map(|u| (u.object_id, u.refcount))
        .collect()
}

#[test]
fn importing_the_same_file_twice_shares_objects() {
    let dir = temp_dir("babeldb-ingest-");
    let path = dir.path().join("dup.bin");
    let blocks = 20;
    let content = pattern_bytes(Pattern::Random, blocks * BS, 99);
    std::fs::write(&path, &content).unwrap();
    for mode in [Mode::Adaptive, Mode::BabelPure] {
        let mut db = mem_db(ingest_config(mode));
        let ctx = format!("{mode:?}");
        let r1 = db
            .import_file(b"dup/a", &path, &ImportOptions::default())
            .unwrap();
        let o1 = db.stats().unwrap().objects;
        assert_eq!(o1, blocks as u64, "{ctx}: one object per distinct block");
        let r2 = db
            .import_file(b"dup/b", &path, &ImportOptions::default())
            .unwrap();
        assert_record(&db, b"dup/a", &content, Some(r1), &ctx);
        assert_record(&db, b"dup/b", &content, Some(r2), &ctx);
        let o2 = db.stats().unwrap().objects;
        let a = unit_objects(&db, b"dup/a");
        let b = unit_objects(&db, b"dup/b");
        match mode {
            Mode::Adaptive => {
                assert_eq!(
                    o2, o1,
                    "{ctx}: importing identical content again must reuse every object"
                );
                let ids_a: Vec<_> = a.iter().map(|u| u.0).collect();
                let ids_b: Vec<_> = b.iter().map(|u| u.0).collect();
                assert_eq!(
                    ids_a, ids_b,
                    "{ctx}: both records reference the same objects"
                );
                assert!(
                    b.iter().all(|u| u.1 == Some(2)),
                    "{ctx}: refcount 2 per shared object: {b:?}"
                );
            }
            Mode::BabelPure => {
                assert_eq!(o2, 2 * o1, "{ctx}: BabelPure never deduplicates");
                assert!(b.iter().all(|u| u.1 == Some(1)), "{ctx}: refcount 1: {b:?}");
            }
        }
        assert!(db.delete(b"dup/a", Expect::Any).unwrap());
        assert_eq!(
            db.stats().unwrap().objects,
            o2 - if mode == Mode::Adaptive { 0 } else { o1 }
        );
        assert_record(
            &db,
            b"dup/b",
            &content,
            Some(r2),
            &format!("{ctx} after deleting the twin"),
        );
        assert!(db.delete(b"dup/b", Expect::Any).unwrap());
        assert_eq!(
            db.stats().unwrap().objects,
            0,
            "{ctx}: all objects released"
        );
        let gc = gc_and_check_clean(&mut db, &ctx);
        assert_eq!(gc, GcReport::default(), "{ctx}: nothing left for gc");
    }
}

#[test]
fn put_and_import_of_the_same_content_share_objects() {
    let db = mem_db(ingest_config(Mode::Adaptive));
    let content = pattern_bytes(Pattern::Mixed, 12 * BS + 100, 5);
    db.put(b"p", &content, Expect::Any).unwrap();
    let objects = db.stats().unwrap().objects;
    let rev = db
        .import(
            b"i",
            &mut MemSource::new(content.clone()),
            &ImportOptions::default(),
        )
        .unwrap();
    assert_record(&db, b"i", &content, Some(rev), "import after put");
    assert_eq!(
        db.stats().unwrap().objects,
        objects,
        "import of stored content must reuse its objects"
    );
    let ids = |key: &[u8]| {
        unit_objects(&db, key)
            .into_iter()
            .map(|u| u.0)
            .collect::<Vec<_>>()
    };
    assert_eq!(ids(b"p"), ids(b"i"), "same blocks, same objects");
    verify_ok(&db, true, "put + import");
}

/// After a successful import the provisional (pending) references must have
/// become exact record references: gc finds nothing, and overwriting then
/// deleting the record releases every object.
fn import_then_release<S: Store>(db: &mut Db<S>, ctx: &str) {
    let content = pattern_bytes(Pattern::Random, 12 * BS, 12);
    let rev = db
        .import(
            b"rel",
            &mut MemSource::new(content.clone()),
            &ImportOptions::default(),
        )
        .unwrap();
    assert_record(db, b"rel", &content, Some(rev), ctx);
    assert_eq!(
        db.stats().unwrap().objects,
        12,
        "{ctx}: one object per block"
    );
    let rep = verify_ok(db, true, ctx);
    assert_eq!(
        (rep.pending_imports, rep.orphan_objects),
        (0, 0),
        "{ctx}: after a confirmed import"
    );
    let gc = gc_and_check_clean(db, ctx);
    assert_eq!(
        gc,
        GcReport::default(),
        "{ctx}: a confirmed import leaves nothing to collect"
    );
    let replacement = pattern_bytes(Pattern::Random, 5 * BS, 13);
    db.put(b"rel", &replacement, Expect::Any).unwrap();
    assert_eq!(
        db.stats().unwrap().objects,
        5,
        "{ctx}: overwriting releases the imported objects"
    );
    assert!(db.delete(b"rel", Expect::Any).unwrap());
    let st = db.stats().unwrap();
    assert_eq!(
        (st.objects, st.hash_candidates),
        (0, 0),
        "{ctx}: delete releases everything"
    );
    gc_and_check_clean(db, ctx);
}

#[test]
fn imported_objects_are_released_mem() {
    import_then_release(&mut mem_db(ingest_config(Mode::Adaptive)), "mem");
}

#[test]
fn imported_objects_are_released_redb() {
    let dir = temp_dir("babeldb-ingest-");
    let mut db = open_redb(
        &dir.path().join("release.redb"),
        ingest_config(Mode::Adaptive),
    );
    import_then_release(&mut db, "redb");
}

#[test]
fn import_spanning_many_batches_is_exact() {
    let dir = temp_dir("babeldb-ingest-");
    // 256 KiB with 4 KiB import batches: dozens of intermediate transactions.
    let content = pattern_bytes(Pattern::Mixed, 256 * 1024 + 3, 2024);
    let path = dir.path().join("big.bin");
    std::fs::write(&path, &content).unwrap();
    let mut mem = mem_db(ingest_config(Mode::Adaptive));
    let rev = mem
        .import_file(b"big", &path, &ImportOptions::default())
        .unwrap();
    assert_record(&mem, b"big", &content, Some(rev), "mem");
    gc_and_check_clean(&mut mem, "mem");
    let db_path = dir.path().join("big.redb");
    let rev = {
        let db = open_redb(&db_path, ingest_config(Mode::Adaptive));
        db.import_file(b"big", &path, &ImportOptions::default())
            .unwrap()
    };
    let mut db = open_redb(&db_path, ingest_config(Mode::Adaptive));
    assert_record(&db, b"big", &content, Some(rev), "redb after reopen");
    let gc = gc_and_check_clean(&mut db, "redb");
    assert_eq!(
        gc,
        GcReport::default(),
        "redb: a completed import leaves nothing to collect"
    );
}

#[test]
fn import_of_a_missing_file_fails_cleanly() {
    let dir = temp_dir("babeldb-ingest-");
    let mut db = mem_db(ingest_config(Mode::Adaptive));
    let missing = dir.path().join("does-not-exist.bin");
    let err = db
        .import_file(b"missing", &missing, &ImportOptions::default())
        .expect_err("importing a missing file");
    assert!(matches!(err, Error::Io(_)), "missing file: {err:?}");
    assert_eq!(db.get(b"missing").unwrap(), None);
    for (_, desc) in db.sources().unwrap() {
        assert!(desc.last_import.is_none(), "no import happened: {desc:?}");
    }
    gc_and_check_clean(&mut db, "missing file");
    assert_eq!(db.stats().unwrap().objects, 0);
}

#[test]
#[ignore = "slow: 48 MiB import with the default configuration (16 KiB blocks); run with `cargo test --test ingest -- --ignored`"]
fn import_large_file_with_default_config() {
    let dir = temp_dir("babeldb-ingest-large-");
    let len = 48 << 20;
    let mut content = Vec::with_capacity(len);
    let mut i = 0u64;
    while content.len() < len {
        let pattern = Pattern::ALL[(i % 6) as usize];
        content.extend_from_slice(&pattern_bytes(pattern, 1 << 20, i));
        i += 1;
    }
    let path = dir.path().join("large.bin");
    std::fs::write(&path, &content).unwrap();
    let db_path = dir.path().join("large.redb");
    let cfg = Config {
        import_batch_bytes: 1 << 20,
        ..Config::adaptive()
    };
    let rev = {
        let db = open_redb(&db_path, cfg.clone());
        db.import_file(b"large", &path, &ImportOptions::default())
            .unwrap()
    };
    std::fs::remove_file(&path).unwrap();
    let mut db = open_redb(&db_path, cfg);
    assert_record(&db, b"large", &content, Some(rev), "large import");
    let li = db.sources().unwrap()[0]
        .1
        .last_import
        .clone()
        .expect("last_import");
    assert_eq!((li.bytes, li.digest), (len as u64, blake3_of(&content)));
    gc_and_check_clean(&mut db, "large import");
}
