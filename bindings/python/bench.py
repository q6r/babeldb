"""Tiny babeldb benchmark: puts/s and gets/s with 512-byte values, 1 and 8 threads.

Each phase writes (then reads, in random order) ``--ops`` records of 512 random
(incompressible) bytes, split between the threads, in a fresh database.

Usage: python bench.py [--ops N] [--durability immediate|buffered] [--dir DIR]
"""

import argparse
import os
import random
import shutil
import tempfile
import threading
import time

import babeldb

VALUE_SIZE = 512


def timed(n_threads, work):
    """Run ``work(t)`` on ``n_threads`` threads started together; seconds taken."""
    ready = threading.Barrier(n_threads + 1)
    errors = []

    def body(t):
        ready.wait()
        try:
            work(t)
        except BaseException as e:  # noqa: BLE001 - re-raised below
            errors.append(e)

    threads = [threading.Thread(target=body, args=(t,)) for t in range(n_threads)]
    for th in threads:
        th.start()
    ready.wait()
    start = time.perf_counter()
    for th in threads:
        th.join()
    elapsed = time.perf_counter() - start
    if errors:
        raise errors[0]
    return elapsed


def bench(root, n_threads, ops, durability):
    path = os.path.join(root, f"bench-{durability}-{n_threads}t")
    values = [os.urandom(VALUE_SIZE) for _ in range(256)]
    per_thread = ops // n_threads
    total = per_thread * n_threads
    with babeldb.open(path, durability=durability) as db:

        def put(t):
            for i in range(per_thread):
                db.put(b"k%02d-%08d" % (t, i), values[i & 255])

        put_s = timed(n_threads, put)
        db.sync()

        def get(t):
            rng = random.Random(t)
            for _ in range(per_thread):
                key = b"k%02d-%08d" % (rng.randrange(n_threads), rng.randrange(per_thread))
                if db.get(key) is None:
                    raise AssertionError(f"missing {key!r}")

        get_s = timed(n_threads, get)
    return total / put_s, total / get_s


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ops", type=int, default=20000, help="records per phase (default 20000)")
    parser.add_argument("--durability", choices=["immediate", "buffered"], default="immediate")
    parser.add_argument("--dir", help="where to create the databases (default: a temporary directory)")
    args = parser.parse_args()

    root = args.dir or tempfile.mkdtemp(prefix="babeldb-bench-")
    os.makedirs(root, exist_ok=True)
    try:
        print(
            f"babeldb {babeldb.__version__}: {VALUE_SIZE} B values, {args.ops} ops per phase, "
            f"durability={args.durability}"
        )
        print(f"{'threads':>7} {'puts/s':>12} {'gets/s':>12}")
        for n_threads in (1, 8):
            puts, gets = bench(root, n_threads, args.ops, args.durability)
            print(f"{n_threads:>7} {puts:>12,.0f} {gets:>12,.0f}")
    finally:
        if not args.dir:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
