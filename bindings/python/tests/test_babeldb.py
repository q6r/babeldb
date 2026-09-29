"""Tests of the babeldb Python binding (run against the installed wheel)."""

import gc
import os
import subprocess
import sys
import threading

import pytest

import babeldb


@pytest.fixture
def db(tmp_path):
    handle = babeldb.open(tmp_path / "db")
    yield handle
    handle.close()


def keys_of(items):
    return [k for k, _ in items]


def test_roundtrip_str_and_bytes(db):
    rev = db.put("hello", "world")
    assert isinstance(rev, int) and rev > 0
    assert db.get("hello") == b"world"
    assert db.get(b"hello") == b"world"
    db.put(b"\x00\xffbin", bytearray(b"\x01\x02"))
    assert db.get(memoryview(b"\x00\xffbin")) == b"\x01\x02"
    db.put(bytearray(b"mv"), memoryview(b"view"))
    assert db.get("mv") == b"view"
    db.put("ção", "ünïcode ✓")
    assert db.get("ção".encode("utf-8")) == "ünïcode ✓".encode("utf-8")
    db.put("empty-value", b"")
    assert db.get("empty-value") == b""
    assert db.get("missing") is None
    assert db.put("hello", b"again") > rev
    assert db.get("hello") == b"again"


def test_argument_errors(db):
    with pytest.raises(TypeError):
        db.put(1, b"x")
    with pytest.raises(TypeError):
        db.put("k", 1.5)
    with pytest.raises(TypeError):
        db.get(None)
    with pytest.raises(babeldb.InvalidArgumentError):
        db.put(b"", b"x")
    with pytest.raises(babeldb.InvalidArgumentError):
        db.put(b"k" * 4097, b"x")
    db.put(b"k" * 4096, b"max key length")
    assert db.get(b"k" * 4096) == b"max key length"
    assert issubclass(babeldb.InvalidArgumentError, babeldb.BabelError)


def test_large_values(db):
    random_mib = os.urandom(1 << 20)
    db.put("random", random_mib)
    assert db.get("random") == random_mib
    text_mib = (b"babeldb large value " * 60000)[: 1 << 20]
    db.put("text", text_mib)
    assert db.get("text") == text_mib
    assert db.scan("random") == [(b"random", random_mib)]


def test_compare_and_set(db):
    r1 = db.put("k", "v1", if_absent=True)
    with pytest.raises(babeldb.ConflictError):
        db.put("k", "v2", if_absent=True)
    r2 = db.put("k", "v2", if_revision=r1)
    assert r2 > r1
    with pytest.raises(babeldb.ConflictError):
        db.put("k", "v3", if_revision=r1)
    assert db.get("k") == b"v2"
    with pytest.raises(babeldb.ConflictError):
        db.put("never-written", "v", if_revision=r2)
    with pytest.raises(babeldb.InvalidArgumentError):
        db.put("k", "v", if_absent=True, if_revision=r2)
    assert issubclass(babeldb.ConflictError, babeldb.BabelError)


def test_delete(db):
    db.put("k", "v")
    assert db.delete("k") is True
    assert db.get("k") is None
    assert db.delete("k") is False
    rev = db.put("k", "v")
    with pytest.raises(babeldb.ConflictError):
        db.delete("k", if_revision=rev + 1000)
    assert db.get("k") == b"v"
    assert db.delete("k", if_revision=rev) is True
    with pytest.raises(babeldb.ConflictError):
        db.delete("k", if_revision=rev)
    # A deleted key can be created again with if_absent.
    db.put("k", "again", if_absent=True)
    assert db.get("k") == b"again"


@pytest.fixture
def filled(db):
    for i in range(10):
        db.put(f"a/{i}", f"v{i}")
    db.put("b/0", "b0")
    db.put("0", "zero")
    return db


def test_scan_prefix_reverse_limit(filled):
    db = filled
    items = db.scan("a/")
    assert keys_of(items) == [f"a/{i}".encode() for i in range(10)]
    assert items[3] == (b"a/3", b"v3")
    assert keys_of(db.scan("a/", reverse=True, limit=3)) == [b"a/9", b"a/8", b"a/7"]
    assert keys_of(db.scan(b"a/", limit=2)) == [b"a/0", b"a/1"]
    assert db.scan("zzz") == []
    assert len(db.scan()) == 12
    assert db.scan(limit=2) == [(b"0", b"zero"), (b"a/0", b"v0")]
    assert keys_of(db.scan(reverse=True, limit=1)) == [b"b/0"]


def test_scan_range(filled):
    db = filled
    assert keys_of(db.scan(start="a/2", end="a/5")) == [b"a/2", b"a/3", b"a/4"]
    assert keys_of(db.scan(start="a/2", end="a/5", reverse=True)) == [b"a/4", b"a/3", b"a/2"]
    assert keys_of(db.scan(start="a/8")) == [b"a/8", b"a/9", b"b/0"]
    assert keys_of(db.scan(end="a/1")) == [b"0", b"a/0"]
    assert keys_of(db.scan(start=b"a/5", limit=2)) == [b"a/5", b"a/6"]
    assert db.scan(start="b", end="a") == []
    assert db.scan(start="a/3", end="a/3") == []
    with pytest.raises(babeldb.InvalidArgumentError):
        db.scan("a/", start="a/1")
    with pytest.raises(babeldb.InvalidArgumentError):
        db.scan("a/", end="a/5")
    with pytest.raises(babeldb.InvalidArgumentError):
        db.scan(limit=-1)


def test_keys(filled):
    db = filled
    assert db.keys("a/", limit=3) == [b"a/0", b"a/1", b"a/2"]
    assert db.keys(reverse=True, limit=2) == [b"b/0", b"a/9"]
    assert db.keys(start="a/8", end="b/1") == [b"a/8", b"a/9", b"b/0"]
    assert len(db.keys()) == 12
    assert all(isinstance(k, bytes) for k in db.keys())


def test_batch(db):
    db.put("x", "old")
    x_rev = db.put("x", "old2")
    res = db.batch([("put", "a", "1"), ("put", b"b", b"2"), ("delete", "x"), ("delete", "nope")])
    assert len(res) == 4
    assert isinstance(res[0], int) and isinstance(res[1], int) and res[1] > res[0]
    assert res[2] == x_rev
    assert res[3] is None
    assert db.get("a") == b"1" and db.get("b") == b"2" and db.get("x") is None
    # In order: a later op sees the earlier ones on the same key.
    db.batch([["put", "seq", "1"], ("delete", "seq"), ("put", "seq", "3")])
    assert db.get("seq") == b"3"
    assert db.batch([]) == []
    assert db.batch(op for op in [("put", "gen", "g")])[0] > 0


def test_batch_is_all_or_nothing(db):
    db.put("keep", "original")
    # An invalid operation (empty key) aborts the whole batch.
    with pytest.raises(babeldb.InvalidArgumentError):
        db.batch([("put", "keep", "changed"), ("put", "new", "v"), ("put", b"", "bad")])
    assert db.get("keep") == b"original"
    assert db.get("new") is None
    # A malformed operation is refused before anything is written.
    with pytest.raises(babeldb.InvalidArgumentError):
        db.batch([("put", "new", "v"), ("frob", "x")])
    with pytest.raises(babeldb.InvalidArgumentError):
        db.batch([("put", "new", "v"), ("put", "only-a-key")])
    with pytest.raises(babeldb.InvalidArgumentError):
        db.batch([("put", "new", "v"), "put"])
    with pytest.raises(TypeError):
        db.batch([("put", "new", "v"), ("put", 1, "v")])
    with pytest.raises(TypeError):
        db.batch(42)
    assert db.get("new") is None
    assert db.keys() == [b"keep"]


def test_reopen_persistence(tmp_path):
    path = tmp_path / "db"
    with babeldb.open(path) as db:
        revs = [db.put(f"k{i:03}", f"v{i}") for i in range(100)]
        db.batch([("put", "batched", "yes"), ("delete", "k000")])
        db.put("big", b"\xab" * (1 << 20))
    with babeldb.open(str(path)) as db:
        assert db.get("k042") == b"v42"
        assert db.get("k000") is None
        assert db.get("batched") == b"yes"
        assert db.get("big") == b"\xab" * (1 << 20)
        assert len(db.keys("k")) == 99
        assert db.put("k001", "new") > max(revs)


def test_buffered_mode_persists_after_sync_and_close(tmp_path):
    path = tmp_path / "db"
    db = babeldb.open(path, durability="buffered")
    assert db.durability == "buffered"
    for i in range(50):
        db.put(f"k{i}", "v")
    assert db.get("k7") == b"v"  # visible as soon as put returns
    db.sync()
    db.put("after-sync", "v")
    db.close()  # makes everything durable
    with babeldb.Db(path) as db:
        assert db.durability == "immediate"
        assert len(db.keys("k")) == 50
        assert db.get("after-sync") == b"v"


def test_invalid_durability(tmp_path):
    with pytest.raises(babeldb.InvalidArgumentError):
        babeldb.open(tmp_path / "db", durability="sometimes")


def test_close_semantics(tmp_path):
    db = babeldb.open(tmp_path / "db")
    db.put("k", "v")
    assert not db.closed
    db.close()
    assert db.closed
    db.close()  # idempotent
    calls = [
        lambda: db.get("k"),
        lambda: db.put("k", "v"),
        lambda: db.delete("k"),
        lambda: db.scan(),
        lambda: db.keys(),
        lambda: db.batch([]),
        lambda: db.sync(),
    ]
    for call in calls:
        with pytest.raises(babeldb.ClosedError):
            call()
    with pytest.raises(babeldb.ClosedError):
        with db:
            pass
    assert "closed" in repr(db)
    assert issubclass(babeldb.ClosedError, babeldb.BabelError)


def test_context_manager_closes(tmp_path):
    with babeldb.open(tmp_path / "db") as db:
        db.put("k", "v")
        assert not db.closed
    assert db.closed
    with pytest.raises(ZeroDivisionError):
        with babeldb.open(tmp_path / "db") as db2:
            1 / 0
    assert db2.closed


def test_one_db_per_directory(tmp_path):
    path = tmp_path / "db"
    with babeldb.open(path) as db:
        with pytest.raises(babeldb.BabelError, match="already open"):
            babeldb.open(path)
        with pytest.raises(babeldb.BabelError, match="already open"):
            babeldb.Db(str(path) + os.sep)
        db.put("k", "v")  # the first handle still works
    with babeldb.open(path) as db:  # closed: can be opened again
        assert db.get("k") == b"v"


def test_properties_and_repr(db, tmp_path):
    assert db.durability == "immediate"
    assert os.path.samefile(db.path, tmp_path / "db")
    assert os.path.isabs(db.path)
    assert "babeldb.Db" in repr(db)
    assert babeldb.__version__ == "0.1.0"
    assert isinstance(db, babeldb.Db)


def test_eight_threads_writing(tmp_path):
    n_threads, per_thread = 8, 250
    with babeldb.open(tmp_path / "db") as db:
        barrier = threading.Barrier(n_threads)
        revs = [[] for _ in range(n_threads)]
        errors = []

        def worker(t):
            try:
                barrier.wait()
                for i in range(per_thread):
                    key = f"t{t}/{i:04}"
                    revs[t].append(db.put(key, f"{t}:{i}".encode() * 8))
                    if i % 25 == 0:
                        assert db.get(key) == f"{t}:{i}".encode() * 8
                        db.scan(f"t{t}/", reverse=True, limit=5)
            except BaseException as e:  # noqa: BLE001 - reported below
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(n_threads)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        assert not errors, errors
        every = [r for per in revs for r in per]
        assert len(set(every)) == n_threads * per_thread  # unique revisions
        for per in revs:
            assert per == sorted(per)  # each thread sees its revisions grow
        assert len(db.keys()) == n_threads * per_thread
        for t in range(n_threads):
            assert db.keys(f"t{t}/", reverse=True, limit=1) == [f"t{t}/{per_thread - 1:04}".encode()]


def test_threads_and_close(tmp_path):
    db = babeldb.open(tmp_path / "db")
    stop = threading.Event()
    outcomes = []

    def writer(t):
        i = 0
        try:
            while not stop.is_set():
                db.put(f"{t}/{i}", "v")
                i += 1
        except babeldb.ClosedError:
            outcomes.append("closed")
        except BaseException as e:  # noqa: BLE001 - reported below
            outcomes.append(e)

    threads = [threading.Thread(target=writer, args=(t,)) for t in range(4)]
    for th in threads:
        th.start()
    db.put("main", "v")
    db.close()  # waits for the calls in flight; later calls raise ClosedError
    stop.set()
    for th in threads:
        th.join()
    assert all(o in ("closed", None) for o in outcomes), outcomes
    with babeldb.open(tmp_path / "db") as again:
        assert again.get("main") == b"v"


def test_message_key_helpers(db):
    key = babeldb.message_key(42, 7)
    assert key == (42).to_bytes(8, "big") + (7).to_bytes(8, "big")
    assert len(key) == 16
    assert babeldb.parse_message_key(key) == (42, 7)
    assert babeldb.parse_message_key(bytearray(key)) == (42, 7)
    assert babeldb.parse_message_key(key[:15]) is None
    assert babeldb.parse_message_key(b"") is None
    assert babeldb.channel_prefix(42) == (42).to_bytes(8, "big")
    assert key.startswith(babeldb.channel_prefix(42))
    assert babeldb.message_key(1, 2**64 - 1) < babeldb.message_key(2, 0)
    assert babeldb.parse_message_key(babeldb.message_key(2**64 - 1, 0)) == (2**64 - 1, 0)
    with pytest.raises(OverflowError):
        babeldb.message_key(-1, 0)
    with pytest.raises(OverflowError):
        babeldb.channel_prefix(2**64)
    # Chat usage: the newest messages of a channel.
    for message_id in range(1, 21):
        db.put(babeldb.message_key(42, message_id), f"message {message_id}")
    db.put(babeldb.message_key(43, 1), "another channel")
    latest = db.scan(babeldb.channel_prefix(42), reverse=True, limit=5)
    assert [babeldb.parse_message_key(k)[1] for k, _ in latest] == [20, 19, 18, 17, 16]
    assert latest[0][1] == b"message 20"
    older = db.scan(
        start=babeldb.channel_prefix(42), end=babeldb.message_key(42, 16), reverse=True, limit=3
    )
    assert [babeldb.parse_message_key(k)[1] for k, _ in older] == [15, 14, 13]


def test_garbage_collected_db_is_closed(tmp_path):
    path = tmp_path / "db"
    db = babeldb.open(path, durability="buffered")
    db.put("k", "v")
    db = None  # last reference: the Db is closed when it is destroyed
    gc.collect()
    with babeldb.open(path) as again:  # the directory was released
        assert again.get("k") == b"v"


def test_exit_without_close_makes_buffered_writes_durable(tmp_path):
    path = tmp_path / "db"
    lines = [
        "import babeldb",
        f"db = babeldb.open({str(path)!r}, durability='buffered')",
        "for i in range(100):",
        "    db.put(f'k{i}', 'v')",
        "keep = db  # still open at exit: closed by atexit",
    ]
    subprocess.run([sys.executable, "-c", chr(10).join(lines)], check=True, timeout=120)
    with babeldb.open(path) as db:
        assert len(db.keys("k")) == 100
