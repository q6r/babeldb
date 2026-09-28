//! Streaming import and maintenance (verify, gc, compact, training install)
//! on `Db<MemStore>` in Adaptive mode (block_size 512, inline_max 64).
//! Values are checked by rebuilding them from their manifests and objects
//! (`ops::load_manifest` + `ops::decode_envelope`), not through `Db::get`.

use babeldb::config::{CodecPolicy, Config, Mode};
use babeldb::engine::ops::{self, ParamCache};
use babeldb::engine::{Db, Expect};
use babeldb::error::{Error, Result};
use babeldb::format::{
    self, ChunkRef, CodecTag, Manifest, ManifestBody, Param, SourceDescriptor, id_key, meta_key,
    param_kind, source_kind,
};
use babeldb::generator::Generator;
use babeldb::hash;
use babeldb::ingest::{ByteSource, ImportOptions, ReaderSource};
use babeldb::maintenance::{CompactReport, GcReport, VerifyReport};
use babeldb::planner::{Planner, TrainOptions};
use babeldb::source;
use babeldb::store::mem::MemStore;
use babeldb::store::{Durability, ReadTxn, Store, Table, WriteTxn};

const BLOCK: usize = 512;
const INLINE: usize = 64;

fn cfg() -> Config {
    Config {
        block_size: BLOCK as u32,
        inline_max: INLINE as u32,
        ..Config::adaptive()
    }
}

fn open(cfg: Config) -> Db<MemStore> {
    Db::with_store(MemStore::new(), cfg).unwrap()
}

/// Deterministic bytes without repeated blocks (xorshift64).
fn pattern(len: usize, seed: u64) -> Vec<u8> {
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

fn import_bytes<S: Store>(
    db: &Db<S>,
    key: &[u8],
    data: &[u8],
    opts: &ImportOptions,
) -> Result<u64> {
    let mut src: &[u8] = data;
    db.import(key, &mut src, opts)
}

fn opts(expect: Expect) -> ImportOptions {
    ImportOptions {
        expect,
        ..ImportOptions::default()
    }
}

fn manifest<S: Store>(db: &Db<S>, key: &[u8]) -> Option<Manifest> {
    ops::load_manifest(&db.store().begin_read().unwrap(), key).unwrap()
}

/// Rebuild the current value of `key` from its manifest and objects,
/// checking the length and digest of every unit.
fn rebuild<S: Store>(db: &Db<S>, key: &[u8]) -> Option<Vec<u8>> {
    let r = db.store().begin_read().unwrap();
    let m = ops::load_manifest(&r, key).unwrap()?;
    let params = ParamCache::new(3);
    let value = match &m.body {
        ManifestBody::Inline(env) => ops::decode_envelope(env, &params, true, None).unwrap(),
        ManifestBody::Chunks(refs) => {
            let mut out = Vec::new();
            let mut start = 0;
            for c in refs {
                let env = r
                    .get(Table::Objects, &id_key(c.object_id))
                    .unwrap()
                    .expect("referenced object exists");
                params.ensure_for_envelope(&r, &env).unwrap();
                let bytes = ops::decode_envelope(&env, &params, true, Some(c.object_id)).unwrap();
                assert_eq!(bytes.len() as u64, c.logical_end - start, "chunk length");
                start = c.logical_end;
                out.extend_from_slice(&bytes);
            }
            out
        }
        ManifestBody::Tombstone => return None,
        ManifestBody::Generated { .. } => panic!("unexpected generated record"),
    };
    assert_eq!(value.len() as u64, m.logical_len);
    Some(value)
}

fn table_len<S: Store>(db: &Db<S>, table: Table) -> u64 {
    db.store().begin_read().unwrap().len(table).unwrap()
}

fn refcount<S: Store>(db: &Db<S>, id: u64) -> u64 {
    ops::get_refcount(&db.store().begin_read().unwrap(), id).unwrap()
}

fn meta_u64<S: Store>(db: &Db<S>, name: &str) -> Option<u64> {
    ops::get_meta_u64(&db.store().begin_read().unwrap(), name).unwrap()
}

/// Deep verification finds nothing at all: no problem, orphan or pending import.
fn assert_clean<S: Store>(db: &Db<S>) -> VerifyReport {
    let rep = db.verify(true).unwrap();
    assert!(
        rep.ok() && rep.orphan_objects == 0 && rep.pending_imports == 0 && rep.issues.is_empty(),
        "{rep:#?}"
    );
    rep
}

/// Write one manifest directly (tombstones, generated records, corruption).
fn put_manifest<S: Store>(db: &Db<S>, key: &[u8], logical_len: u64, body: ManifestBody) -> u64 {
    let mut w = db.store().begin_write().unwrap();
    let revision = ops::alloc_revision(&mut w).unwrap();
    ops::put_manifest(
        &mut w,
        key,
        &Manifest {
            revision,
            logical_len,
            source_id: None,
            body,
        },
    )
    .unwrap();
    w.commit(Durability::Immediate).unwrap();
    revision
}

/// In-memory source with short reads and an injected failure.
struct TestSource {
    data: Vec<u8>,
    pos: usize,
    max_read: usize,
    fail_at: Option<usize>,
}

impl TestSource {
    fn new(data: Vec<u8>) -> Self {
        TestSource {
            data,
            pos: 0,
            max_read: usize::MAX,
            fail_at: None,
        }
    }

    fn max_read(mut self, n: usize) -> Self {
        self.max_read = n;
        self
    }

    /// Serve the bytes before `at`, then fail.
    fn fail_at(mut self, at: usize) -> Self {
        self.fail_at = Some(at);
        self
    }
}

impl ByteSource for TestSource {
    fn read_block(&mut self, buf: &mut [u8]) -> Result<usize> {
        let end = self
            .fail_at
            .map_or(self.data.len(), |f| f.min(self.data.len()));
        if self.fail_at.is_some() && self.pos >= end {
            return Err(std::io::Error::other("injected source failure").into());
        }
        let n = buf.len().min(self.max_read).min(end - self.pos);
        buf[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

/// Source that observes the database while the import runs.
struct ProbeSource<'a> {
    db: &'a Db<MemStore>,
    data: Vec<u8>,
    pos: usize,
    verify_at: usize,
    objects_seen: Vec<u64>,
    report: Option<VerifyReport>,
}

impl ByteSource for ProbeSource<'_> {
    fn read_block(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.objects_seen.push(table_len(self.db, Table::Objects));
        if self.pos == self.verify_at && self.report.is_none() {
            self.report = Some(self.db.verify(true)?);
        }
        let n = buf.len().min(self.data.len() - self.pos);
        buf[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

#[test]
fn import_roundtrips_every_size_class() {
    let db = open(cfg());
    let sizes = [
        0,
        1,
        INLINE - 1,
        INLINE,
        INLINE + 1,
        BLOCK - 1,
        BLOCK,
        BLOCK + 1,
        3 * BLOCK,
        3 * BLOCK + 7,
    ];
    let mut objects = 0;
    for (i, &len) in sizes.iter().enumerate() {
        let key = format!("size/{len}");
        let data = pattern(len, i as u64 + 1);
        let rev = import_bytes(&db, key.as_bytes(), &data, &ImportOptions::default()).unwrap();
        let m = manifest(&db, key.as_bytes()).unwrap();
        assert_eq!(
            (m.revision, m.logical_len, m.source_id),
            (rev, len as u64, None)
        );
        match &m.body {
            ManifestBody::Inline(_) => assert!(len <= INLINE, "{len} bytes must be chunked"),
            ManifestBody::Chunks(refs) => {
                assert!(len > INLINE, "{len} bytes must be inline");
                let ends: Vec<u64> = refs.iter().map(|c| c.logical_end).collect();
                let fixed: Vec<u64> = (1..=len.div_ceil(BLOCK))
                    .map(|j| (j * BLOCK).min(len) as u64)
                    .collect();
                assert_eq!(ends, fixed, "fixed-size blocks, the last one shorter");
                objects += refs.len() as u64;
            }
            other => panic!("unexpected manifest {other:?}"),
        }
        assert_eq!(rebuild(&db, key.as_bytes()).unwrap(), data, "{len} bytes");
    }
    let deep = assert_clean(&db);
    assert_eq!(deep.records_checked, sizes.len() as u64);
    assert_eq!(
        (deep.objects_checked, deep.objects_decoded),
        (objects, objects)
    );
    let shallow = db.verify(false).unwrap();
    assert!(shallow.ok() && shallow.objects_decoded == 0, "{shallow:#?}");
}

#[test]
fn import_batches_and_short_reads_keep_fixed_blocks() {
    let data = pattern(10 * BLOCK + 123, 7);
    for batch in [1, 1500, 1 << 20] {
        let db = open(Config {
            import_batch_bytes: batch,
            ..cfg()
        });
        let mut src = TestSource::new(data.clone()).max_read(97);
        let rev = db
            .import(b"big", &mut src, &ImportOptions::default())
            .unwrap();
        let m = manifest(&db, b"big").unwrap();
        assert_eq!((m.revision, m.object_ids().len()), (rev, 11));
        assert_eq!(rebuild(&db, b"big").unwrap(), data);
        assert_eq!(table_len(&db, Table::PendingImports), 0);
        assert_eq!(
            meta_u64(&db, meta_key::NEXT_IMPORT_ID),
            Some(2),
            "one import id per chunked import"
        );
        assert_clean(&db);
    }
    // Any `std::io::Read` works through `ReaderSource`.
    let db = open(cfg());
    let mut reader = ReaderSource::new(std::io::Cursor::new(data.clone()));
    db.import(b"reader", &mut reader, &ImportOptions::default())
        .unwrap();
    assert_eq!(rebuild(&db, b"reader").unwrap(), data);
}

#[test]
fn batches_commit_during_the_import_and_verify_counts_pending_references() {
    let data = pattern(8 * BLOCK, 4);
    for (batch, seen) in [(1, vec![0, 1, 2, 3, 4, 5, 6, 7, 8]), (1 << 20, vec![0; 9])] {
        let db = open(Config {
            import_batch_bytes: batch,
            ..cfg()
        });
        let mut src = ProbeSource {
            db: &db,
            data: data.clone(),
            pos: 0,
            verify_at: 4 * BLOCK,
            objects_seen: Vec::new(),
            report: None,
        };
        db.import(b"k", &mut src, &ImportOptions::default())
            .unwrap();
        assert_eq!(
            src.objects_seen, seen,
            "objects committed before each read (batch {batch})"
        );
        let mid = src.report.take().unwrap();
        assert!(mid.ok() && mid.orphan_objects == 0, "{mid:#?}");
        let committed = seen[4];
        let counts = (
            mid.pending_imports,
            mid.objects_checked,
            mid.objects_decoded,
        );
        assert_eq!(counts, (u64::from(committed > 0), committed, committed));
        assert_eq!(rebuild(&db, b"k").unwrap(), data);
        assert_clean(&db);
    }
}

#[test]
fn import_preserves_every_byte_and_dedupes_verified_blocks() {
    let db = open(Config {
        import_batch_bytes: 1,
        ..cfg()
    });
    let all_bytes: Vec<u8> = (0..=255u8).cycle().take(5 * 256 + 3).collect();
    let crlf = b"line one\r\nline two\r\n\r\n\n\r\x00end".repeat(40);
    let invalid_utf8 = [
        0xffu8, 0xfe, 0xc0, 0x80, 0xed, 0xa0, 0x80, 0xf4, 0x90, 0x80, 0x80,
    ]
    .repeat(70);
    let zeros = vec![0u8; 8 * BLOCK];
    let cases: [(&[u8], &Vec<u8>); 4] = [
        (b"all-bytes", &all_bytes),
        (b"crlf", &crlf),
        (b"invalid-utf8", &invalid_utf8),
        (b"zeros", &zeros),
    ];
    for (key, data) in cases {
        import_bytes(&db, key, data, &ImportOptions::default()).unwrap();
        assert_eq!(
            &rebuild(&db, key).unwrap(),
            data,
            "{}",
            String::from_utf8_lossy(key)
        );
    }
    // The 8 zero blocks (one batch each) share one byte-verified object.
    let ids = manifest(&db, b"zeros").unwrap().object_ids();
    assert_eq!(ids, vec![ids[0]; 8]);
    assert_eq!(refcount(&db, ids[0]), 8);
    assert_clean(&db);
}

#[test]
fn imported_file_stays_readable_after_the_source_is_deleted() {
    let db = open(cfg());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("input.bin");
    let data = pattern(3 * BLOCK + 17, 11);
    std::fs::write(&path, &data).unwrap();
    let location = source::local_file_location(&path).unwrap();
    let rev = db
        .import_file(b"file", &path, &ImportOptions::default())
        .unwrap();
    std::fs::remove_file(&path).unwrap();
    assert_eq!(rebuild(&db, b"file").unwrap(), data);

    let sid = manifest(&db, b"file")
        .unwrap()
        .source_id
        .expect("source attached");
    let desc = source::load_source(&db.store().begin_read().unwrap(), sid)
        .unwrap()
        .unwrap();
    assert_eq!(
        (desc.kind, desc.location.as_str()),
        (source_kind::LOCAL_FILE, location.as_str())
    );
    let last = desc.last_import.unwrap();
    assert_eq!(
        (last.revision, last.bytes, last.digest),
        (rev, data.len() as u64, hash::digest(&data))
    );
    assert!(last.unix_ms > 0);

    // The same path again (now an inline value): the descriptor is reused and updated.
    let small = pattern(40, 12);
    std::fs::write(&path, &small).unwrap();
    let rev2 = db
        .import_file(b"file2", &path, &ImportOptions::default())
        .unwrap();
    assert_eq!(manifest(&db, b"file2").unwrap().source_id, Some(sid));
    let r = db.store().begin_read().unwrap();
    assert_eq!(source::list_sources(&r).unwrap().len(), 1);
    let last = source::load_source(&r, sid)
        .unwrap()
        .unwrap()
        .last_import
        .unwrap();
    assert_eq!(
        (last.revision, last.bytes, last.digest),
        (rev2, 40, hash::digest(&small))
    );
    drop(r);
    assert_eq!(rebuild(&db, b"file2").unwrap(), small);

    // A missing file publishes nothing and registers no source.
    let err = db
        .import_file(
            b"none",
            &dir.path().join("missing.bin"),
            &ImportOptions::default(),
        )
        .unwrap_err();
    assert!(matches!(err, Error::Io(_)), "{err}");
    assert!(manifest(&db, b"none").is_none());
    assert_eq!(table_len(&db, Table::Sources), 1);
    assert_clean(&db);
}

#[test]
fn import_updates_an_explicit_source() {
    let db = open(cfg());
    let feed = SourceDescriptor {
        kind: source_kind::EXTERNAL,
        location: "https://example.invalid/feed".into(),
        adapter_version: 3,
        last_import: None,
    };
    let sid = {
        let mut w = db.store().begin_write().unwrap();
        let id = source::insert_source(&mut w, &feed).unwrap();
        w.commit(Durability::Immediate).unwrap();
        id
    };
    let data = pattern(2 * BLOCK, 5);
    let rev = import_bytes(
        &db,
        b"ext",
        &data,
        &ImportOptions {
            source_id: Some(sid),
            ..ImportOptions::default()
        },
    )
    .unwrap();
    assert_eq!(manifest(&db, b"ext").unwrap().source_id, Some(sid));
    let desc = source::load_source(&db.store().begin_read().unwrap(), sid)
        .unwrap()
        .unwrap();
    assert_eq!(
        (desc.kind, desc.adapter_version, desc.location.as_str()),
        (feed.kind, 3, feed.location.as_str())
    );
    let last = desc.last_import.unwrap();
    assert_eq!(
        (last.revision, last.bytes, last.digest),
        (rev, data.len() as u64, hash::digest(&data))
    );

    let unknown = ImportOptions {
        source_id: Some(sid + 100),
        ..ImportOptions::default()
    };
    let err = import_bytes(&db, b"other", &data, &unknown).unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)), "{err}");
    assert!(manifest(&db, b"other").is_none());
    assert_clean(&db);
}

#[test]
fn failed_import_publishes_nothing_and_releases_its_objects() {
    let db = open(Config {
        import_batch_bytes: 1,
        ..cfg()
    });
    let old = pattern(2 * BLOCK, 1);
    import_bytes(&db, b"k", &old, &ImportOptions::default()).unwrap();
    let next_object = meta_u64(&db, meta_key::NEXT_OBJECT_ID).unwrap();

    // Fails after 3 full blocks, i.e. after 3 committed batches.
    let mut src = TestSource::new(pattern(6 * BLOCK, 2)).fail_at(3 * BLOCK + 10);
    let err = db
        .import(b"k", &mut src, &ImportOptions::default())
        .unwrap_err();
    assert!(matches!(err, Error::Io(_)), "{err}");
    assert_eq!(
        meta_u64(&db, meta_key::NEXT_OBJECT_ID),
        Some(next_object + 3),
        "3 objects were stored"
    );
    assert_eq!(rebuild(&db, b"k").unwrap(), old);
    assert_eq!(
        (
            table_len(&db, Table::Objects),
            table_len(&db, Table::Refcounts)
        ),
        (2, 2)
    );
    assert_eq!(table_len(&db, Table::PendingImports), 0);
    assert_clean(&db);

    // Same for a new key, failing inside a block.
    let mut src = TestSource::new(pattern(6 * BLOCK, 3)).fail_at(BLOCK + 100);
    assert!(
        db.import(b"new", &mut src, &ImportOptions::default())
            .is_err()
    );
    assert!(manifest(&db, b"new").is_none());
    assert_clean(&db);

    // A source that reports more bytes than the buffer holds is rejected.
    struct Liar;
    impl ByteSource for Liar {
        fn read_block(&mut self, buf: &mut [u8]) -> Result<usize> {
            Ok(buf.len() + 1)
        }
    }
    let err = db
        .import(b"liar", &mut Liar, &ImportOptions::default())
        .unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)), "{err}");
    assert!(manifest(&db, b"liar").is_none());
    assert_clean(&db);
}

#[test]
fn import_enforces_key_and_value_limits() {
    let db = open(Config {
        max_key_len: 8,
        max_value_len: 3 * BLOCK as u64,
        import_batch_bytes: 1,
        ..cfg()
    });
    let small = pattern(10, 0);
    let err = import_bytes(&db, b"", &small, &ImportOptions::default()).unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)), "{err}");
    let err = import_bytes(&db, b"123456789", &small, &ImportOptions::default()).unwrap_err();
    assert!(matches!(err, Error::LimitExceeded(_)), "{err}");
    let exact = pattern(3 * BLOCK, 1);
    import_bytes(&db, b"12345678", &exact, &ImportOptions::default()).unwrap();
    assert_eq!(rebuild(&db, b"12345678").unwrap(), exact);
    let err = import_bytes(
        &db,
        b"big",
        &pattern(3 * BLOCK + 1, 2),
        &ImportOptions::default(),
    )
    .unwrap_err();
    assert!(matches!(err, Error::LimitExceeded(_)), "{err}");
    assert!(manifest(&db, b"big").is_none());
    assert_clean(&db);
}

#[test]
fn import_checks_expectations_at_publication() {
    let db = open(Config {
        import_batch_bytes: 1,
        ..cfg()
    });
    let a = pattern(2 * BLOCK, 1);
    let r1 = import_bytes(&db, b"k", &a, &opts(Expect::Absent)).unwrap();

    let err = import_bytes(&db, b"k", &pattern(3 * BLOCK, 2), &opts(Expect::Absent)).unwrap_err();
    assert!(
        matches!(err, Error::RevisionConflict { actual: Some(r), .. } if r == r1),
        "{err}"
    );
    let err = import_bytes(&db, b"k", &a, &opts(Expect::Revision(r1 + 1000))).unwrap_err();
    assert!(matches!(err, Error::RevisionConflict { .. }), "{err}");
    assert_eq!(rebuild(&db, b"k").unwrap(), a);
    assert_clean(&db); // the objects of the rejected imports were released

    let b = pattern(BLOCK + 3, 3);
    let r2 = import_bytes(&db, b"k", &b, &opts(Expect::Revision(r1))).unwrap();
    assert!(r2 > r1);
    assert_eq!(rebuild(&db, b"k").unwrap(), b);
    let err = import_bytes(&db, b"missing", &b, &opts(Expect::Revision(r2))).unwrap_err();
    assert!(
        matches!(err, Error::RevisionConflict { actual: None, .. }),
        "{err}"
    );
    import_bytes(&db, b"k", &a, &opts(Expect::Any)).unwrap();
    assert_eq!(rebuild(&db, b"k").unwrap(), a);

    // A tombstone counts as absent.
    put_manifest(&db, b"gone", 0, ManifestBody::Tombstone);
    import_bytes(&db, b"gone", &a, &opts(Expect::Absent)).unwrap();
    assert_eq!(rebuild(&db, b"gone").unwrap(), a);
    assert_clean(&db);
}

#[test]
fn replacing_a_record_releases_or_retains_its_objects() {
    let a = pattern(4 * BLOCK, 1);
    let db = open(cfg());
    import_bytes(&db, b"k", &a, &ImportOptions::default()).unwrap();
    // Same bytes again: the new import shares the objects, then the old manifest releases them.
    import_bytes(&db, b"k", &a, &ImportOptions::default()).unwrap();
    assert_eq!(table_len(&db, Table::Objects), 4);
    for id in manifest(&db, b"k").unwrap().object_ids() {
        assert_eq!(refcount(&db, id), 1);
    }
    import_bytes(&db, b"k", &pattern(2 * BLOCK, 2), &ImportOptions::default()).unwrap();
    assert_eq!(table_len(&db, Table::Objects), 2);
    assert_clean(&db);

    // With history the previous manifest keeps its references.
    let db = open(Config {
        keep_history: true,
        ..cfg()
    });
    let r1 = import_bytes(&db, b"k", &a, &ImportOptions::default()).unwrap();
    let b = pattern(3 * BLOCK, 3);
    import_bytes(&db, b"k", &b, &ImportOptions::default()).unwrap();
    let r = db.store().begin_read().unwrap();
    let old = Manifest::decode(
        &r.get(Table::History, &format::history_key(b"k", r1))
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!((old.revision, old.logical_len), (r1, a.len() as u64));
    for id in old.object_ids() {
        assert_eq!(ops::get_refcount(&r, id).unwrap(), 1);
    }
    drop(r);
    assert_eq!(table_len(&db, Table::Objects), 7);
    assert_eq!(rebuild(&db, b"k").unwrap(), b);
    assert_eq!(assert_clean(&db).history_checked, 1);
}

#[test]
fn simulated_crash_is_reported_by_verify_and_collected_by_gc() {
    let mut db = open(cfg());
    let published = pattern(2 * BLOCK, 1);
    import_bytes(&db, b"k", &published, &ImportOptions::default()).unwrap();
    let shared = manifest(&db, b"k").unwrap().object_ids()[0];

    // An import that crashed after one batch: two new objects plus a
    // deduplicated reference to an object of "k".
    let planner = Planner::new(Mode::Adaptive, CodecPolicy::raw_only());
    let params = ParamCache::new(3);
    {
        let mut w = db.store().begin_write().unwrap();
        let import_id = ops::alloc_id(&mut w, meta_key::NEXT_IMPORT_ID, "import id").unwrap();
        let mut ids = Vec::new();
        for block in [
            pattern(BLOCK, 50),
            pattern(BLOCK, 51),
            published[..BLOCK].to_vec(),
        ] {
            let unit = ops::prepare_unit(&planner, &block);
            ids.push(
                ops::store_unit(&mut w, &unit, &block, true, &params)
                    .unwrap()
                    .0,
            );
        }
        assert_eq!(ids[2], shared);
        w.put(
            Table::PendingImports,
            &id_key(import_id),
            &format::encode_id_list(&ids),
        )
        .unwrap();
        w.commit(Durability::Deferred).unwrap();
    }
    let rep = db.verify(true).unwrap();
    assert!(rep.ok() && rep.orphan_objects == 0, "{rep:#?}");
    assert_eq!(rep.pending_imports, 1);
    assert_eq!(refcount(&db, shared), 2);

    let gc = db.gc().unwrap();
    assert_eq!(
        gc,
        GcReport {
            abandoned_imports: 1,
            objects_removed: 2,
            ..GcReport::default()
        }
    );
    assert_eq!(refcount(&db, shared), 1);
    assert_eq!(table_len(&db, Table::Objects), 2);
    assert_eq!(rebuild(&db, b"k").unwrap(), published);
    assert_clean(&db);
    assert_eq!(db.gc().unwrap(), GcReport::default(), "gc is idempotent");
}

#[test]
fn gc_repairs_refcount_drift_and_removes_garbage() {
    let mut db = open(cfg());
    let a = pattern(3 * BLOCK, 1);
    import_bytes(&db, b"a", &a, &ImportOptions::default()).unwrap();
    let ids = manifest(&db, b"a").unwrap().object_ids();
    let planner = Planner::new(Mode::Adaptive, CodecPolicy::raw_only());
    let (unused_param, active_param, used_param);
    {
        let mut w = db.store().begin_write().unwrap();
        // Drift: one count too high, one row missing.
        w.put(Table::Refcounts, &id_key(ids[0]), &format::encode_u64(99))
            .unwrap();
        w.remove(Table::Refcounts, &id_key(ids[1])).unwrap();
        // Garbage: an orphan with row and candidate, an orphan without row,
        // a row without object, a candidate pointing to a missing object
        // (the stale id a released object leaves).
        let orphan = pattern(BLOCK, 9);
        ops::store_unit(
            &mut w,
            &ops::prepare_unit(&planner, &orphan),
            &orphan,
            true,
            &ParamCache::new(3),
        )
        .unwrap();
        let loose = ops::alloc_id(&mut w, meta_key::NEXT_OBJECT_ID, "object id").unwrap();
        w.put(
            Table::Objects,
            &id_key(loose),
            &ops::prepare_unit(&planner, b"loose object").envelope,
        )
        .unwrap();
        let stale = ops::alloc_id(&mut w, meta_key::NEXT_OBJECT_ID, "object id").unwrap();
        w.put(Table::Refcounts, &id_key(stale), &format::encode_u64(1))
            .unwrap();
        let ghost = ops::alloc_id(&mut w, meta_key::NEXT_OBJECT_ID, "object id").unwrap();
        ops::add_candidate(&mut w, &hash::digest(b"ghost"), 5, ghost).unwrap();
        // A live object also listed under another digest (corruption).
        ops::add_candidate(&mut w, &hash::digest(b"other"), 5, ids[2]).unwrap();
        // Params: unused, active in meta, and needed by a referenced object.
        let mut add_param = |kind: u8| {
            let id = ops::alloc_id(&mut w, meta_key::NEXT_PARAM_ID, "param id").unwrap();
            let param = Param {
                kind,
                version: 1,
                bytes: b"param bytes".to_vec(),
            };
            w.put(Table::Params, &id_key(id), &param.encode()).unwrap();
            id
        };
        unused_param = add_param(param_kind::ZSTD_DICT);
        active_param = add_param(param_kind::TEMPLATE);
        used_param = add_param(param_kind::ZSTD_DICT);
        ops::put_meta_u64(&mut w, meta_key::ACTIVE_TEMPLATE, active_param).unwrap();
        // A referenced ZstdV1 object with that dictionary (never decoded here).
        let zid = ops::alloc_id(&mut w, meta_key::NEXT_OBJECT_ID, "object id").unwrap();
        let env = format::write_envelope(
            CodecTag::ZSTD_V1,
            used_param,
            3,
            &hash::digest(b"xyz"),
            b"opaque zstd body",
        );
        w.put(Table::Objects, &id_key(zid), &env).unwrap();
        ops::incref(&mut w, zid).unwrap();
        let revision = ops::alloc_revision(&mut w).unwrap();
        let body = ManifestBody::Chunks(vec![ChunkRef {
            logical_end: 3,
            object_id: zid,
        }]);
        ops::put_manifest(
            &mut w,
            b"z",
            &Manifest {
                revision,
                logical_len: 3,
                source_id: None,
                body,
            },
        )
        .unwrap();
        w.commit(Durability::Immediate).unwrap();
    }
    let rep = db.verify(false).unwrap();
    let counts = (
        rep.refcount_mismatches,
        rep.orphan_objects,
        rep.dangling_candidates,
        rep.stale_candidates,
        rep.missing_params,
        rep.format_errors,
    );
    assert_eq!(counts, (2, 3, 1, 1, 0, 0), "{rep:#?}");
    assert!(!rep.ok());

    let gc = db.gc().unwrap();
    let expected = GcReport {
        objects_removed: 2,
        candidates_removed: 1,
        stale_candidates_removed: 1,
        params_removed: 1,
        refcounts_fixed: 3,
        ..GcReport::default()
    };
    assert_eq!(gc, expected);
    let rep = db.verify(false).unwrap();
    assert!(
        rep.ok() && rep.orphan_objects == 0 && rep.issues.is_empty(),
        "{rep:#?}"
    );
    for &id in &ids {
        assert_eq!(refcount(&db, id), 1);
    }
    let r = db.store().begin_read().unwrap();
    assert!(
        r.get(Table::Params, &id_key(unused_param))
            .unwrap()
            .is_none()
    );
    assert!(
        r.get(Table::Params, &id_key(active_param))
            .unwrap()
            .is_some()
    );
    assert!(r.get(Table::Params, &id_key(used_param)).unwrap().is_some());
    drop(r);
    assert_eq!(rebuild(&db, b"a").unwrap(), a);
}

#[test]
fn verify_reports_corruption_and_gc_refuses_unknown_references() {
    let mut db = open(cfg());
    import_bytes(&db, b"a", &pattern(3 * BLOCK, 1), &ImportOptions::default()).unwrap();
    import_bytes(
        &db,
        b"small",
        b"tiny inline value",
        &ImportOptions::default(),
    )
    .unwrap();
    let ids = manifest(&db, b"a").unwrap().object_ids();

    // Flip one body byte: only deep verification decodes it.
    {
        let mut w = db.store().begin_write().unwrap();
        let mut env = w.get(Table::Objects, &id_key(ids[0])).unwrap().unwrap();
        *env.last_mut().unwrap() ^= 1;
        w.put(Table::Objects, &id_key(ids[0]), &env).unwrap();
        w.commit(Durability::Immediate).unwrap();
    }
    let shallow = db.verify(false).unwrap();
    assert!(shallow.ok(), "{shallow:#?}");
    let deep = db.verify(true).unwrap();
    assert_eq!(
        (deep.digest_failures, deep.objects_decoded),
        (1, 2),
        "{deep:#?}"
    );
    assert!(!deep.ok());
    assert!(
        deep.issues
            .iter()
            .any(|i| i.starts_with(&format!("object {} ", ids[0]))),
        "{deep:#?}"
    );

    // A missing object is attributed to its record; its candidate dangles.
    {
        let mut w = db.store().begin_write().unwrap();
        w.remove(Table::Objects, &id_key(ids[1])).unwrap();
        w.commit(Durability::Immediate).unwrap();
    }
    let rep = db.verify(false).unwrap();
    assert_eq!(
        (
            rep.missing_objects,
            rep.dangling_candidates,
            rep.refcount_mismatches
        ),
        (1, 1, 0),
        "{rep:#?}"
    );
    assert!(
        rep.issues.iter().any(|i| i.starts_with("record \"a\"")),
        "{rep:#?}"
    );

    // An undecodable manifest, and an inline envelope whose dictionary is missing.
    {
        let mut w = db.store().begin_write().unwrap();
        w.put(Table::Records, b"broken", b"\x01garbage").unwrap();
        w.commit(Durability::Immediate).unwrap();
    }
    let env = format::write_envelope(CodecTag::ZSTD_V1, 4242, 3, &hash::digest(b"xyz"), b"body");
    put_manifest(&db, b"needs-dict", 3, ManifestBody::Inline(env));
    let rep = db.verify(true).unwrap();
    assert_eq!(rep.missing_params, 1, "{rep:#?}");
    // The broken manifest, and next_param_id not above the referenced param 4242.
    assert_eq!(rep.format_errors, 2, "{rep:#?}");
    assert!(
        rep.issues
            .iter()
            .any(|i| i.starts_with("record \"broken\"")),
        "{rep:#?}"
    );
    assert!(
        rep.issues
            .iter()
            .any(|i| i.starts_with("param 4242 is missing")),
        "{rep:#?}"
    );

    // gc cannot know what the broken manifest references: it changes nothing.
    let objects = table_len(&db, Table::Objects);
    let err = db.gc().unwrap_err();
    assert!(matches!(err, Error::Format(_)), "{err}");
    assert_eq!(table_len(&db, Table::Objects), objects);
}

/// Output byte `i` is `i as u8`; params: output length (u64 LE).
struct Ramp;

impl Generator for Ramp {
    fn id(&self) -> u16 {
        900
    }

    fn version(&self) -> u16 {
        1
    }

    fn name(&self) -> &'static str {
        "test-ramp"
    }

    fn output_len(&self, params: &[u8]) -> Result<u64> {
        format::decode_u64(params)
    }

    fn generate(&self, _params: &[u8], offset: u64, out: &mut [u8]) -> Result<()> {
        for (i, b) in out.iter_mut().enumerate() {
            *b = (offset + i as u64) as u8;
        }
        Ok(())
    }
}

#[test]
fn deep_verify_regenerates_generated_records() {
    let mut db = open(cfg());
    db.register_generator(Box::new(Ramp));
    let len = 200_000u64; // several 64 KiB regeneration chunks
    let bytes: Vec<u8> = (0..len).map(|i| i as u8).collect();
    let generated = |generator_id: u16, digest: [u8; 32]| ManifestBody::Generated {
        generator_id,
        generator_version: 1,
        params: len.to_le_bytes().to_vec(),
        digest,
    };
    put_manifest(&db, b"good", len, generated(900, hash::digest(&bytes)));
    assert_clean(&db);
    put_manifest(&db, b"bad-digest", len, generated(900, [0; 32]));
    put_manifest(&db, b"unknown", len, generated(901, hash::digest(&bytes)));
    let shallow = db.verify(false).unwrap();
    assert_eq!(
        (shallow.unknown_generators, shallow.digest_failures),
        (1, 0),
        "{shallow:#?}"
    );
    let deep = db.verify(true).unwrap();
    assert_eq!(
        (deep.unknown_generators, deep.digest_failures),
        (1, 1),
        "{deep:#?}"
    );
    assert!(!deep.ok());
}

#[test]
fn compact_reports_an_unsupported_backend() {
    let mut db = open(cfg());
    import_bytes(&db, b"k", &pattern(2 * BLOCK, 1), &ImportOptions::default()).unwrap();
    let rep = db.compact().unwrap();
    let expected = CompactReport {
        supported: false,
        allocated_before: Some(0),
        allocated_after: Some(0),
        ..CompactReport::default()
    };
    assert_eq!(rep, expected);
    assert_clean(&db);
}

#[test]
fn training_is_refused_in_babel_pure_mode() {
    let db = open(Config {
        mode: Mode::BabelPure,
        dedupe: false,
        ..cfg()
    });
    let samples = vec![b"sample value".to_vec(); 16];
    let err = db
        .train_dictionary(&samples, &TrainOptions::default())
        .unwrap_err();
    assert!(matches!(err, Error::Unsupported(_)), "{err}");
    let err = db
        .train_template(&samples, &TrainOptions::default())
        .unwrap_err();
    assert!(matches!(err, Error::Unsupported(_)), "{err}");
    assert_eq!(table_len(&db, Table::Params), 0);
}

// ---------------------------------------------------------------------------
// After merge (need the engine core, RedbStore, codecs and training)
// ---------------------------------------------------------------------------

fn chat_samples() -> Vec<Vec<u8>> {
    (0..400u64)
        .map(|i| {
            let id = 1_100_000_000_000_000_000u64 + i * 4096;
            format!(
                r#"{{"id":"{id}","channel":"general","author":"user{}","text":"message number {i} of the test corpus","edited":false}}"#,
                i % 7
            )
            .into_bytes()
        })
        .collect()
}

#[test]
fn trained_dictionary_is_installed_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dict.redb");
    let samples = chat_samples();
    let train = TrainOptions {
        require_gain: false,
        ..TrainOptions::default()
    };
    let dict_id = {
        let db = Db::open(&path, cfg()).unwrap();
        let rep = db.train_dictionary(&samples, &train).unwrap();
        assert!(rep.installed, "{rep:?}");
        let id = rep.param_id.unwrap();
        assert_eq!(meta_u64(&db, meta_key::ACTIVE_ZSTD_DICT), Some(id));
        let stored = db
            .store()
            .begin_read()
            .unwrap()
            .get(Table::Params, &id_key(id))
            .unwrap()
            .unwrap();
        assert_eq!(Param::decode(&stored).unwrap().kind, param_kind::ZSTD_DICT);
        for (i, s) in samples.iter().enumerate().take(40) {
            import_bytes(
                &db,
                format!("msg/{i}").as_bytes(),
                s,
                &ImportOptions::default(),
            )
            .unwrap();
        }
        assert_clean(&db);
        id
    };
    let mut db = Db::open(&path, cfg()).unwrap();
    assert_eq!(meta_u64(&db, meta_key::ACTIVE_ZSTD_DICT), Some(dict_id));
    for (i, s) in samples.iter().enumerate().take(40) {
        assert_eq!(&rebuild(&db, format!("msg/{i}").as_bytes()).unwrap(), s);
    }
    assert_eq!(
        db.gc().unwrap().params_removed,
        0,
        "the active dictionary is kept"
    );
    assert_clean(&db);
}

#[test]
fn trained_template_is_installed_next_to_the_dictionary() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("template.redb");
    let samples = chat_samples();
    let train = TrainOptions {
        require_gain: false,
        ..TrainOptions::default()
    };
    {
        let db = Db::open(&path, cfg()).unwrap();
        let dict = db.train_dictionary(&samples, &train).unwrap();
        let template = db.train_template(&samples, &train).unwrap();
        assert!(
            dict.installed && template.installed,
            "{dict:?} {template:?}"
        );
        assert_eq!(meta_u64(&db, meta_key::ACTIVE_ZSTD_DICT), dict.param_id);
        assert_eq!(meta_u64(&db, meta_key::ACTIVE_TEMPLATE), template.param_id);
        for (i, s) in samples.iter().enumerate().take(40) {
            import_bytes(
                &db,
                format!("msg/{i}").as_bytes(),
                s,
                &ImportOptions::default(),
            )
            .unwrap();
        }
        assert_clean(&db);
    }
    let db = Db::open(&path, cfg()).unwrap();
    for (i, s) in samples.iter().enumerate().take(40) {
        assert_eq!(&rebuild(&db, format!("msg/{i}").as_bytes()).unwrap(), s);
    }
    assert_clean(&db);
}

#[test]
fn imported_values_read_back_through_get() {
    let db = open(Config {
        import_batch_bytes: 1,
        ..cfg()
    });
    for (i, len) in [0, INLINE, 5 * BLOCK + 9].into_iter().enumerate() {
        let data = pattern(len, i as u64);
        let key = format!("k{i}");
        import_bytes(&db, key.as_bytes(), &data, &ImportOptions::default()).unwrap();
        assert_eq!(db.get(key.as_bytes()).unwrap().unwrap(), data);
        let (off, n) = (len / 3, len / 2);
        let range = db
            .get_range(key.as_bytes(), off as u64, n as u64)
            .unwrap()
            .unwrap();
        assert_eq!(range, data[off..off + n]);
    }
}

#[test]
fn imported_file_survives_reopen_on_redb() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("input.bin");
    let data = pattern(20 * BLOCK + 5, 3);
    std::fs::write(&file, &data).unwrap();
    let path = dir.path().join("db.redb");
    let rev = {
        let db = Db::open(
            &path,
            Config {
                import_batch_bytes: 4 * BLOCK,
                ..cfg()
            },
        )
        .unwrap();
        db.import_file(b"file", &file, &ImportOptions::default())
            .unwrap()
    };
    std::fs::remove_file(&file).unwrap();
    let mut db = Db::open(&path, cfg()).unwrap();
    assert_eq!(manifest(&db, b"file").unwrap().revision, rev);
    assert_eq!(rebuild(&db, b"file").unwrap(), data);
    assert_clean(&db);
    let rep = db.compact().unwrap();
    assert!(rep.apparent_before > 0 && rep.apparent_after > 0, "{rep:?}");
}

#[test]
fn babel_pure_import_roundtrips() {
    let db = open(Config {
        mode: Mode::BabelPure,
        dedupe: false,
        ..cfg()
    });
    for (i, len) in [0, 1, INLINE, 3 * BLOCK + 1].into_iter().enumerate() {
        let data = pattern(len, i as u64);
        let key = format!("k{i}");
        import_bytes(&db, key.as_bytes(), &data, &ImportOptions::default()).unwrap();
        assert_eq!(rebuild(&db, key.as_bytes()).unwrap(), data);
    }
    assert_clean(&db);
}
