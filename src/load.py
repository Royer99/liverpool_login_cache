"""Multiprocess streaming loader: generate and insert in one pass, no JSONL.

Each worker owns a contiguous global-index range for sessions and for index
documents, checkpoints after every batch, and resumes from the checkpoint on
restart. Duplicate-key errors on resume overlap are expected and ignored.

Standalone usage:
    python src/load.py                  # full load per .env
    python src/load.py --drop           # drop collection first
    python src/load.py --create-type-index
"""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import os
import sys
import time
from pathlib import Path
from typing import Any, Callable, Iterator

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import Config, load_config  # noqa: E402
from generate import (  # noqa: E402
    build_hash_doc,
    build_stream_doc,
    iter_index_docs,
    iter_session_docs,
)
from model import ensure_indexes  # noqa: E402

BYTE_BUDGET = 10 * 1024 * 1024  # per insert_many payload
MB = 1024 * 1024


def _estimate_size(doc: dict[str, Any]) -> int:
    """Cheap BSON-size estimate; only used to bound batch payloads."""
    if doc["type"] == "zset":
        return 120 + 78 * len(doc["value"])
    return 400 + len(doc["value"].get("padding", "")) + 2600


def _mongo_collection(cfg: Config) -> Any:
    from pymongo import MongoClient
    client: Any = MongoClient(cfg.mongodb_uri, maxPoolSize=4, retryWrites=True,
                              w=cfg.write_concern)
    return client[cfg.mongodb_db][cfg.mongodb_collection]


def _checkpoint_path(base: str, worker: int) -> Path:
    return Path(f"{base}.w{worker}")


def _read_checkpoint(base: str, worker: int) -> dict[str, Any] | None:
    p = _checkpoint_path(base, worker)
    if p.exists():
        return json.loads(p.read_text())
    return None


def _write_checkpoint(base: str, worker: int, state: dict[str, Any]) -> None:
    p = _checkpoint_path(base, worker)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(state))
    os.replace(tmp, p)


def _insert_batch(coll: Any, batch: list[dict[str, Any]]) -> int:
    """insert_many(ordered=False), swallowing duplicate-key errors from resume."""
    from pymongo.errors import BulkWriteError
    try:
        coll.insert_many(batch, ordered=False)
        return len(batch)
    except BulkWriteError as e:
        dupes = [w for w in e.details.get("writeErrors", []) if w.get("code") == 11000]
        if len(dupes) != len(e.details.get("writeErrors", [])):
            raise
        return len(batch) - len(dupes)


def load_range(cfg: Config, worker: int, sess_range: tuple[int, int],
               idx_range: tuple[int, int], checkpoint_base: str,
               progress: Callable[[int, int], None],
               coll: Any = None) -> int:
    """Generate and insert one worker's ranges, checkpointing per batch.

    *progress(worker, delta)* is called after each batch. *coll* is injectable
    for tests. Returns documents inserted this run.
    """
    coll = coll if coll is not None else _mongo_collection(cfg)
    state = _read_checkpoint(checkpoint_base, worker)
    if state is not None:
        if state["session_range"] != list(sess_range) or state["index_range"] != list(idx_range):
            raise SystemExit(
                f"worker {worker}: checkpoint ranges {state['session_range']}/"
                f"{state['index_range']} do not match assigned {sess_range}/{idx_range}. "
                "GENERATE_WORKERS changed mid-load; restore it or delete the "
                f"{checkpoint_base}.w* files to restart."
            )
    else:
        state = {"session_range": list(sess_range), "index_range": list(idx_range),
                 "session_next": sess_range[0], "index_next": idx_range[0]}

    inserted = 0

    def flush(batch: list[dict[str, Any]], kind: str, next_i: int) -> None:
        nonlocal inserted
        if not batch:
            return
        inserted += _insert_batch(coll, batch)
        state[kind] = next_i
        _write_checkpoint(checkpoint_base, worker, state)
        progress(worker, len(batch))
        batch.clear()

    for kind, rng_end, stream in (
        ("session_next", sess_range[1],
         iter_session_docs(cfg, state["session_next"], sess_range[1])),
        ("index_next", idx_range[1],
         iter_index_docs(cfg, state["index_next"], idx_range[1])),
    ):
        batch: list[dict[str, Any]] = []
        size = 0
        i = state[kind]
        for doc in stream:
            batch.append(doc)
            size += _estimate_size(doc)
            i += 1
            if len(batch) >= cfg.load_batch_size or size >= BYTE_BUDGET:
                flush(batch, kind, i)
                size = 0
        flush(batch, kind, rng_end)

    if worker == 0:
        inserted += _insert_batch(coll, [build_stream_doc(cfg), build_hash_doc(cfg)])
        progress(worker, 2)
    return inserted


def _worker_entry(worker: int, sess_range: tuple[int, int], idx_range: tuple[int, int],
                  queue: Any) -> None:
    cfg = load_config(quiet=True)
    try:
        load_range(cfg, worker, sess_range, idx_range, cfg.load_checkpoint_path,
                   progress=lambda w, n: queue.put((w, n)))
        queue.put((worker, -1))  # done sentinel
    except BaseException as e:
        queue.put((worker, -2))
        raise SystemExit(f"worker {worker} failed: {e}")


def _split(total: int, workers: int, w: int) -> tuple[int, int]:
    per = total // workers
    start = w * per
    end = total if w == workers - 1 else start + per
    return start, end


def run_load(cfg: Config, workers: int) -> None:
    """Spawn *workers* processes and stream progress until completion."""
    ctx = mp.get_context("spawn")
    queue: Any = ctx.Queue()
    procs = []
    for w in range(workers):
        p = ctx.Process(target=_worker_entry, args=(
            w, _split(cfg.session_doc_count, workers, w),
            _split(cfg.index_doc_count, workers, w), queue), daemon=True)
        p.start()
        procs.append(p)

    total_target = cfg.total_document_count
    per_worker = [0] * workers
    done = 0
    failed = False
    start_t = time.monotonic()
    last_print = 0.0
    while done < workers:
        w, n = queue.get()
        if n == -1:
            done += 1
        elif n == -2:
            done += 1
            failed = True
        else:
            per_worker[w] += n
        now = time.monotonic()
        if now - last_print >= 2 or done == workers:
            total = sum(per_worker)
            rate = total / max(now - start_t, 1e-9)
            eta = (total_target - total) / rate if rate > 0 else float("inf")
            counts = " ".join(f"w{i}:{c}" for i, c in enumerate(per_worker))
            sys.stdout.write(
                f"\r{total:>10,}/{total_target:,} docs  {rate:8.0f} docs/s  "
                f"elapsed {now - start_t:6.0f}s  ETA {eta:6.0f}s  [{counts}]   ")
            sys.stdout.flush()
            last_print = now
    print()
    for p in procs:
        p.join()
    if failed or any(p.exitcode not in (0, None) for p in procs):
        raise SystemExit("load failed in at least one worker; rerun to resume from checkpoints")


def print_final_stats(cfg: Config, scan_large: bool = True) -> None:
    """Print collstats and flag documents above 1 MB (large zset docs)."""
    coll = _mongo_collection(cfg)
    db = coll.database
    stats = db.command("collStats", cfg.mongodb_collection)
    print(f"count:       {stats['count']:,}")
    print(f"avgObjSize:  {stats['avgObjSize']:,} B")
    print(f"dataSize:    {stats['size'] / MB:,.1f} MB")
    if stats["storageSize"] < stats["size"] * 0.01:
        print("storageSize: not yet reported by WiredTiger (fresh load); "
              "run src/verify.py in a minute for the compression ratio")
    else:
        print(f"storageSize: {stats['storageSize'] / MB:,.1f} MB "
              f"(compression {stats['size'] / max(stats['storageSize'], 1):.2f}x)")
    for name, sz in stats.get("indexSizes", {}).items():
        print(f"index {name}: {sz / MB:,.1f} MB")
    if scan_large:
        print("scanning zset docs for BSON size > 1 MB ...")
        big = list(coll.aggregate([
            {"$match": {"type": "zset"}},
            {"$match": {"$expr": {"$gt": [{"$bsonSize": "$$ROOT"}, MB]}}},
            {"$project": {"bytes": {"$bsonSize": "$$ROOT"},
                          "members": {"$size": "$value"}}},
            {"$sort": {"bytes": -1}}, {"$limit": 10},
        ]))
        for d in big:
            print(f"  >1MB: {d['_id'][:60]}...  {d['bytes'] / MB:.2f} MB, {d['members']} members")
        if not big:
            print("  none")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--workers", type=int, help="override GENERATE_WORKERS")
    p.add_argument("--drop", action="store_true", help="drop the collection first")
    p.add_argument("--create-type-index", action="store_true",
                   help="create {type:1} after the load")
    p.add_argument("--skip-scan", action="store_true",
                   help="skip the >1MB document scan at the end")
    args = p.parse_args()
    cfg = load_config()
    workers = args.workers or cfg.generate_workers

    if args.drop:
        _mongo_collection(cfg).drop()
        for f in Path(".").glob(Path(cfg.load_checkpoint_path).name + ".w*"):
            f.unlink()
        print("dropped collection and checkpoints")

    t0 = time.monotonic()
    run_load(cfg, workers)
    print(f"load complete in {time.monotonic() - t0:,.0f}s")

    coll = _mongo_collection(cfg)
    created = ensure_indexes(coll, enable_ttl_index=cfg.enable_ttl_index,
                             create_type_index=args.create_type_index)
    if created:
        print(f"created indexes AFTER load: {created}")
    print_final_stats(cfg, scan_large=not args.skip_scan)


if __name__ == "__main__":
    main()
