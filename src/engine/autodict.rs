//! Automatic Zstd dictionary for small values (`config::AutoDictionary`).
//!
//! `put`, `write_batch` and `write_batch_each` (imports do not) feed their
//! small values (stored inline, of `MIN_COMPRESS_LEN..=max_sample_len` bytes)
//! to [`AutoDict::observe`] before preparing them. Once `samples` values were collected, a dictionary is
//! trained and evaluated on held-out samples with the planner's training
//! (`planner::train_zstd_dictionary`: amortized projected net gain), on a
//! background thread by default. The next write operation (or
//! [`Db::finish_auto_dictionary`]) installs it through the regular param
//! mechanism (`Db::install_param`: new immutable `ZSTD_DICT` param +
//! `meta.active_zstd_dict` in one durable commit, then a new planner), always
//! outside any write transaction, and only when the gain is positive.
//!
//! Nothing here touches the format: the dictionary is an ordinary param and
//! values written before it keep their representation. One attempt per open
//! database; nothing happens when a dictionary is already active (installed
//! automatically or by `train_dictionary`, in this session or before).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::JoinHandle;

use super::Db;
use crate::codec::zstd::MIN_DICT_LEN;
use crate::config::{AutoDictionary, Config, Mode};
use crate::error::{Error, Result};
use crate::format::param_kind;
use crate::planner::{self, MIN_COMPRESS_LEN, TrainOptions, TrainReport};
use crate::store::Store;

/// Where automatic dictionary training stands (`Db::auto_dictionary_status`).
#[derive(Clone, Debug, PartialEq)]
pub enum AutoDictStatus {
    /// Not applicable: BabelPure, a policy without Zstd or dictionaries,
    /// `samples == 0`, or a dictionary was already active when the database
    /// was opened.
    Off,
    /// Collecting samples.
    Collecting { samples: usize, needed: usize },
    /// Samples complete; training (or waiting for the write that trains).
    Training,
    /// Installed as the active dictionary (`param_id` in the report).
    Installed(TrainReport),
    /// Evaluated and not installed: no projected net gain.
    Rejected(TrainReport),
    /// Training or installation failed (the dictionary is only an
    /// optimization: writes are never failed because of it).
    Failed(String),
    /// Another dictionary became active first (e.g. `train_dictionary`).
    Superseded,
}

type TrainResult = Result<(Option<Vec<u8>>, TrainReport)>;

enum State {
    Collecting { samples: Vec<Vec<u8>>, bytes: usize },
    /// Samples complete, to be trained by the next write (no background thread).
    Ready(Vec<Vec<u8>>),
    Training(JoinHandle<TrainResult>),
    /// A write took the samples or the thread's result and is finishing.
    Busy,
    Done(AutoDictStatus),
}

/// Per-database automatic dictionary state.
pub(crate) struct AutoDict {
    cfg: AutoDictionary,
    zstd_level: i32,
    /// Fast path of `observe`: samples are still being collected.
    collecting: AtomicBool,
    /// Fast path of `poll`: samples are complete and a result may be waiting.
    pending: AtomicBool,
    state: Mutex<State>,
}

/// A trained dictionary that passed its evaluation, to be installed.
pub(crate) struct Trained {
    bytes: Vec<u8>,
    report: TrainReport,
}

impl AutoDict {
    /// State for a database of `mode` whose active dictionary is `has_dict`.
    pub(crate) fn new(cfg: &Config, mode: Mode, has_dict: bool) -> AutoDict {
        let on = cfg.auto_dictionary_applies(mode) && !has_dict;
        let state = if on {
            State::Collecting { samples: Vec::new(), bytes: 0 }
        } else {
            State::Done(AutoDictStatus::Off)
        };
        AutoDict {
            cfg: cfg.auto_dictionary.clone(),
            zstd_level: cfg.codecs.zstd_level,
            collecting: AtomicBool::new(on),
            pending: AtomicBool::new(false),
            state: Mutex::new(state),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn is_collecting(&self) -> bool {
        self.collecting.load(Ordering::Relaxed)
    }

    /// Collect the small values among `values` (`inline_max` of the
    /// database). `superseded`: a dictionary is active already.
    pub(crate) fn observe(&self, values: &[&[u8]], inline_max: usize, superseded: bool) {
        if !self.is_collecting() {
            return;
        }
        let hi = self.cfg.max_sample_len.min(inline_max);
        let is_small = |len: usize| (MIN_COMPRESS_LEN..=hi).contains(&len);
        if !superseded && !values.iter().any(|v| is_small(v.len())) {
            return;
        }
        let mut st = self.lock();
        if superseded {
            if matches!(*st, State::Collecting { .. }) {
                *st = State::Done(AutoDictStatus::Superseded);
            }
            self.collecting.store(false, Ordering::Relaxed);
            return;
        }
        let State::Collecting { samples, bytes } = &mut *st else {
            return;
        };
        for v in values.iter().filter(|v| is_small(v.len())) {
            if samples.len() >= self.cfg.samples {
                break;
            }
            samples.push(v.to_vec());
            *bytes += v.len();
        }
        if samples.len() < self.cfg.samples {
            return;
        }
        self.collecting.store(false, Ordering::Relaxed);
        let samples = std::mem::take(samples);
        let opts = self.options(samples.len(), *bytes);
        *st = if self.cfg.background {
            let level = self.zstd_level;
            let samples = Arc::new(samples);
            let for_thread = Arc::clone(&samples);
            // The thread owns its inputs and never touches the database.
            let spawned = std::thread::Builder::new()
                .name("babeldb-autodict".into())
                .spawn(move || planner::train_zstd_dictionary(&for_thread, level, &opts));
            match spawned {
                Ok(handle) => State::Training(handle),
                // No thread available: the next write trains in place.
                Err(_) => State::Ready(Arc::try_unwrap(samples).unwrap_or_else(|s| (*s).clone())),
            }
        } else {
            State::Ready(samples)
        };
        self.pending.store(true, Ordering::Release);
    }

    fn options(&self, samples: usize, sample_bytes: usize) -> TrainOptions {
        TrainOptions {
            expected_uses: if self.cfg.expected_uses == 0 { samples as u64 } else { self.cfg.expected_uses },
            validation_fraction: self.cfg.validation_fraction,
            max_dict_bytes: self.cfg.max_dict_bytes.min(sample_bytes / 16).max(MIN_DICT_LEN),
            require_gain: true,
        }
    }

    /// A trained dictionary to install, if one is ready. With `wait`, a
    /// running training is waited for; without, only a finished one is taken.
    /// Samples of the synchronous mode are trained here, by the caller.
    pub(crate) fn poll(&self, wait: bool) -> Option<Trained> {
        if !self.pending.load(Ordering::Acquire) {
            return None;
        }
        let mut st = self.lock();
        let result = match std::mem::replace(&mut *st, State::Busy) {
            State::Training(handle) if wait || handle.is_finished() => {
                drop(st);
                handle
                    .join()
                    .unwrap_or_else(|_| Err(Error::Backend("dictionary training panicked".into())))
            }
            State::Ready(samples) => {
                drop(st);
                let total = samples.iter().map(Vec::len).sum();
                planner::train_zstd_dictionary(&samples, self.zstd_level, &self.options(samples.len(), total))
            }
            other => {
                *st = other;
                return None;
            }
        };
        self.pending.store(false, Ordering::Release);
        match result {
            Ok((Some(bytes), report)) => Some(Trained { bytes, report }),
            Ok((None, report)) => {
                self.finish(AutoDictStatus::Rejected(report));
                None
            }
            Err(e) => {
                self.finish(AutoDictStatus::Failed(e.to_string()));
                None
            }
        }
    }

    pub(crate) fn finish(&self, status: AutoDictStatus) {
        *self.lock() = State::Done(status);
    }

    pub(crate) fn status(&self) -> AutoDictStatus {
        match &*self.lock() {
            State::Collecting { samples, .. } => {
                AutoDictStatus::Collecting { samples: samples.len(), needed: self.cfg.samples }
            }
            State::Ready(_) | State::Training(_) | State::Busy => AutoDictStatus::Training,
            State::Done(status) => status.clone(),
        }
    }
}

impl<S: Store> Db<S> {
    /// Run by every write operation before it prepares `values`, outside any
    /// write transaction: sample them, and install a dictionary whose
    /// training finished. Failures are recorded in the status, never
    /// returned: the dictionary only changes how later values are stored.
    pub(crate) fn auto_dictionary_step(&self, values: &[&[u8]]) {
        if self.auto_dict.is_collecting() {
            let superseded = self.planner().dictionary().is_some();
            self.auto_dict.observe(values, self.inline_max as usize, superseded);
        }
        if let Some(trained) = self.auto_dict.poll(false) {
            self.install_trained(trained);
        }
    }

    fn install_trained(&self, trained: Trained) {
        let Trained { bytes, mut report } = trained;
        if self.planner().dictionary().is_some() {
            self.auto_dict.finish(AutoDictStatus::Superseded);
            return;
        }
        match self.install_param(param_kind::ZSTD_DICT, bytes) {
            Ok(id) => {
                report.installed = true;
                report.param_id = Some(id);
                self.auto_dict.finish(AutoDictStatus::Installed(report));
            }
            Err(e) => self.auto_dict.finish(AutoDictStatus::Failed(e.to_string())),
        }
    }

    /// Where automatic dictionary training stands.
    pub fn auto_dictionary_status(&self) -> AutoDictStatus {
        self.auto_dict.status()
    }

    /// Wait for a dictionary being trained in the background and install it
    /// now instead of at the next write. Returns the resulting status; while
    /// samples are still being collected nothing changes.
    pub fn finish_auto_dictionary(&self) -> AutoDictStatus {
        if let Some(trained) = self.auto_dict.poll(true) {
            self.install_trained(trained);
        }
        self.auto_dict.status()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CodecPolicy, Config};
    use crate::engine::{BatchOp, Expect, ScanOptions};
    use crate::format::meta_key;
    use crate::planner::TrainOptions;
    use crate::store::mem::MemStore;
    use crate::store::ReadTxn;

    const WORDS: [&str; 16] = [
        "deploy", "worked", "thanks", "see", "you", "tomorrow", "prod", "is", "slow", "why", "lol", "ship", "it",
        "hello", "anyone", "around",
    ];

    /// Chat-like JSON messages, ~150..300 bytes.
    fn message(i: u64) -> Vec<u8> {
        let mut s = i.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        let text: Vec<&str> = (0..(6 + i % 20))
            .map(|_| {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                WORDS[(s % 16) as usize]
            })
            .collect();
        format!(
            r#"{{"id":"{}","channel_id":"1180000000000000123","author":{{"id":"{}","username":"user{}"}},"content":"{}","pinned":false}}"#,
            1_190_000_000_000_000_000u64 + i * 4_194_304,
            1_000_000_000_000_000_000u64 + (i % 29) * 1_234_567,
            i % 29,
            text.join(" ")
        )
        .into_bytes()
    }

    fn cfg(samples: usize, background: bool) -> Config {
        Config {
            auto_dictionary: AutoDictionary { samples, background, ..AutoDictionary::default() },
            ..Config::adaptive()
        }
    }

    fn put_batch<S: Store>(db: &Db<S>, range: std::ops::Range<u64>) {
        let keys: Vec<Vec<u8>> = range.clone().map(|i| format!("m/{i:08}").into_bytes()).collect();
        let values: Vec<Vec<u8>> = range.map(message).collect();
        let ops: Vec<BatchOp<'_>> = keys
            .iter()
            .zip(&values)
            .map(|(k, v)| BatchOp::Put { key: k, value: v, expect: Expect::Any })
            .collect();
        db.write_batch(&ops).unwrap();
    }

    fn active_dict<S: Store>(db: &Db<S>) -> Option<u64> {
        let r = db.store.begin_read().unwrap();
        crate::engine::ops::get_meta_u64(&r, meta_key::ACTIVE_ZSTD_DICT).unwrap()
    }

    /// Every message reads back exactly: through a scan first (right after a
    /// reopen its dependency is not prepared yet), then point reads.
    fn check_all<S: Store>(db: &Db<S>, n: u64) {
        let items = db.scan(&ScanOptions::prefix(b"m/").limit(n as usize).with_values(true)).unwrap();
        assert_eq!(items.len() as u64, n);
        for (i, item) in items.iter().enumerate() {
            assert_eq!(item.value.as_deref(), Some(&message(i as u64)[..]), "scan item {i}");
        }
        for i in 0..n {
            assert_eq!(db.get(format!("m/{i:08}").as_bytes()).unwrap().unwrap(), message(i), "message {i}");
        }
        let report = db.verify(true).unwrap();
        assert!(report.ok(), "{report:#?}");
    }

    #[test]
    fn installs_a_dictionary_after_the_samples_and_keeps_old_values_readable() {
        let db = Db::with_store(MemStore::new(), cfg(300, false)).unwrap();
        assert_eq!(db.auto_dictionary_status(), AutoDictStatus::Collecting { samples: 0, needed: 300 });
        put_batch(&db, 0..150);
        assert_eq!(db.auto_dictionary_status(), AutoDictStatus::Collecting { samples: 150, needed: 300 });
        assert_eq!(active_dict(&db), None);
        // The batch that completes the samples trains, installs, and uses it.
        put_batch(&db, 150..400);
        let AutoDictStatus::Installed(report) = db.auto_dictionary_status() else {
            panic!("{:?}", db.auto_dictionary_status());
        };
        assert!(report.installed && report.projected_net_gain > 0, "{report:?}");
        assert!(report.validation_bytes_with < report.validation_bytes_without, "{report:?}");
        let id = report.param_id.unwrap();
        assert_eq!(active_dict(&db), Some(id));
        assert_eq!(db.planner().dictionary().map(|d| d.id), Some(id));
        put_batch(&db, 400..450);
        let before = db.inspect(b"m/00000010").unwrap().unwrap();
        assert!(before.units.iter().all(|u| u.aux_id == 0), "{before:?}");
        let after = db.inspect(b"m/00000420").unwrap().unwrap();
        assert!(after.units.iter().all(|u| u.codec == "ZstdV1" && u.aux_id == id), "{after:?}");
        check_all(&db, 450);
        let s = db.stats().unwrap();
        assert_eq!(s.params, 1);
        // Planner counters stay cumulative across the planner switch.
        assert_eq!(s.planner.chosen.iter().map(|(_, n)| n).sum::<u64>(), 450);
    }

    #[test]
    fn background_training_is_installed_by_a_later_write_or_on_demand() {
        let db = Db::with_store(MemStore::new(), cfg(200, true)).unwrap();
        put_batch(&db, 0..200);
        assert!(matches!(db.auto_dictionary_status(), AutoDictStatus::Training | AutoDictStatus::Installed(_)));
        let status = db.finish_auto_dictionary();
        let AutoDictStatus::Installed(report) = status else { panic!("{status:?}") };
        let id = report.param_id.unwrap();
        // Idempotent afterwards.
        assert_eq!(db.finish_auto_dictionary(), AutoDictStatus::Installed(report));
        put_batch(&db, 200..230);
        assert!(db.inspect(b"m/00000225").unwrap().unwrap().units.iter().all(|u| u.aux_id == id));
        check_all(&db, 230);
    }

    #[test]
    fn uncompressible_values_install_nothing() {
        let db = Db::with_store(MemStore::new(), cfg(200, false)).unwrap();
        let keys: Vec<Vec<u8>> = (0..220u64).map(|i| format!("r/{i:05}").into_bytes()).collect();
        let values: Vec<Vec<u8>> = (0..220u64)
            .map(|i| (0..(100 + i % 50)).map(|j| crate::datasets::splitmix64(i * 1000 + j) as u8).collect())
            .collect();
        for (k, v) in keys.iter().zip(&values) {
            db.put(k, v, Expect::Any).unwrap();
        }
        let status = db.auto_dictionary_status();
        assert!(matches!(status, AutoDictStatus::Rejected(_) | AutoDictStatus::Failed(_)), "{status:?}");
        assert_eq!(active_dict(&db), None);
        assert_eq!(db.stats().unwrap().params, 0);
        for (k, v) in keys.iter().zip(&values) {
            assert_eq!(&db.get(k).unwrap().unwrap(), v);
        }
    }

    #[test]
    fn off_when_not_applicable_or_already_active() {
        let small = AutoDictionary { samples: 10, background: false, ..AutoDictionary::default() };
        for c in [
            Config { auto_dictionary: small.clone(), ..Config::babel_pure() },
            Config { auto_dictionary: small.clone(), ..Config::raw_only() },
            Config {
                auto_dictionary: small.clone(),
                codecs: CodecPolicy { zstd_dictionary: false, ..CodecPolicy::default() },
                ..Config::adaptive()
            },
            Config { auto_dictionary: AutoDictionary::disabled(), ..Config::adaptive() },
        ] {
            let db = Db::with_store(MemStore::new(), c.clone()).unwrap();
            assert_eq!(db.auto_dictionary_status(), AutoDictStatus::Off, "{c:?}");
            put_batch(&db, 0..20);
            assert_eq!(db.auto_dictionary_status(), AutoDictStatus::Off, "{c:?}");
            assert_eq!(active_dict(&db), None);
        }
        // A dictionary trained by hand first: the automatic one steps aside.
        let db = Db::with_store(MemStore::new(), cfg(100, false)).unwrap();
        put_batch(&db, 0..50);
        let samples: Vec<Vec<u8>> = (0..300).map(message).collect();
        let manual = db.train_dictionary(&samples, &TrainOptions { require_gain: false, ..TrainOptions::default() }).unwrap();
        put_batch(&db, 50..200);
        assert_eq!(db.auto_dictionary_status(), AutoDictStatus::Superseded);
        assert_eq!(active_dict(&db), manual.param_id);
        check_all(&db, 200);
    }

    #[test]
    fn reopened_database_keeps_the_dictionary_and_does_not_retrain() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auto.redb");
        let id = {
            let db = Db::open(&path, cfg(200, false)).unwrap();
            put_batch(&db, 0..260);
            let AutoDictStatus::Installed(report) = db.auto_dictionary_status() else {
                panic!("{:?}", db.auto_dictionary_status())
            };
            report.param_id.unwrap()
        };
        let db = Db::open(&path, cfg(200, false)).unwrap();
        assert_eq!(db.auto_dictionary_status(), AutoDictStatus::Off);
        assert_eq!(active_dict(&db), Some(id));
        put_batch(&db, 260..300);
        check_all(&db, 300);
        let r = db.store.begin_read().unwrap();
        assert_eq!(r.len(crate::store::Table::Params).unwrap(), 1);
    }
}
