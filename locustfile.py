"""Locust harness replaying the Redis access patterns against MongoDB.

Operation mapping (Redis -> MongoDB):
  GET             -> find_one by _id                       (name: get_session)
  ZRANGE          -> find_one by _id with $slice tail      (name: zrange_index)
  MGET            -> find {_id: {$in: [...]}}              (name: mget_sessions)
  SETEX/HSET      -> update_one $set inside value          (name: update_session)
  SETEX (new)     -> insert_one full session doc           (name: insert_session)
  ZADD            -> update_one $push with $slice          (name: push_index)

Locust monkey-patches gevent before this module is imported, so importing
pymongo here is safe. ONE MongoClient per worker process, at module scope,
with connect=False — never per User or per task.
"""

from __future__ import annotations

import random
import sys
import time
import uuid as uuidlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from locust import User, constant_throughput, events, task

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

import bson  # noqa: E402  (ships with pymongo)
from pymongo import MongoClient  # noqa: E402
from pymongo.errors import DuplicateKeyError  # noqa: E402

from config import load_config  # noqa: E402
from generate import build_session_doc  # noqa: E402
from model import (  # noqa: E402
    DEFAULT_SESSION_PREFIX,
    EPOCH_ANCHOR_MS,
    index_key,
    sample_session_index,
    session_key,
)

CFG = load_config()
CLIENT: MongoClient = MongoClient(
    CFG.mongodb_uri,
    maxPoolSize=CFG.max_pool_size,
    readPreference=CFG.read_preference,
    w=CFG.write_concern,
    connect=False,
    appname="locust-session-bench",
)
COLL = CLIENT[CFG.mongodb_db][CFG.mongodb_collection]

# Total op rate implied by the read target; each user (reader or writer)
# carries an equal share so reads land at ~TARGET_READ_RPS.
_TOTAL_RPS = CFG.target_read_rps / max(1.0 - CFG.write_ratio, 0.01)
_PER_USER_RPS = _TOTAL_RPS / CFG.locust_users

_MISS = {"mget_requested": 0, "mget_returned": 0, "update_matched": 0, "update_sent": 0}


@events.init.add_listener
def _assert_pool(environment: Any, **kw: Any) -> None:
    # Undersized pools queue in the driver and get reported as DB latency.
    assert CFG.max_pool_size >= CFG.locust_users, (
        f"MONGODB_MAX_POOL_SIZE={CFG.max_pool_size} must be >= per-process users "
        f"(worst case LOCUST_USERS={CFG.locust_users}); raise the pool or add workers"
    )


@events.test_stop.add_listener
def _report_misses(environment: Any, **kw: Any) -> None:
    req, ret = _MISS["mget_requested"], _MISS["mget_returned"]
    if req:
        print(f"[drift] mget_sessions: requested {req}, found {ret} "
              f"({100 * (req - ret) / req:.1f}% misses — expected, the export's "
              "index zsets drift the same way)")
    if _MISS["update_sent"]:
        print(f"[writes] update_session matched {_MISS['update_matched']}/{_MISS['update_sent']}")


def _fire(name: str, start: float, response_length: int = 0,
          exc: Exception | None = None) -> None:
    events.request.fire(
        request_type="mongo", name=name,
        response_time=(time.perf_counter() - start) * 1000,
        response_length=response_length, exception=exc,
    )


def _timed(name: str, fn: Any) -> Any:
    start = time.perf_counter()
    try:
        result = fn()
    except Exception as e:  # timeouts, transient network errors -> Locust failures
        _fire(name, start, exc=e)
        return None
    _fire(name, start, response_length=_blen(result))
    return result


def _blen(result: Any) -> int:
    if isinstance(result, dict):
        return len(bson.encode(result))
    if isinstance(result, list):
        return sum(len(bson.encode(d)) for d in result)
    return 0


class SessionReader(User):
    """Point reads and two-step index reads, recency-skewed."""

    weight = max(1, round((1.0 - CFG.write_ratio) * 100))
    wait_time = constant_throughput(_PER_USER_RPS)

    def __init__(self, environment: Any) -> None:
        super().__init__(environment)
        self.rng = random.Random()

    def _sample_key(self) -> str:
        i = sample_session_index(self.rng, CFG.session_doc_count,
                                 CFG.hot_set_ratio, CFG.hot_access_share)
        return session_key(CFG.random_seed, i)

    @task(max(1, round((1.0 - CFG.index_read_ratio) * 10)))
    def get_session(self) -> None:
        """GET -> _id point read. The SLO applies to this operation."""
        key = self._sample_key()
        _timed("get_session", lambda: COLL.find_one({"_id": key}))

    @task(max(1, round(CFG.index_read_ratio * 10)))
    def index_read(self) -> None:
        """ZRANGE tail + MGET, reported as two legs plus the round trip."""
        j = self.rng.randrange(CFG.index_doc_count)
        rt_start = time.perf_counter()
        idx = _timed("zrange_index", lambda: COLL.find_one(
            {"_id": index_key(CFG.random_seed, j)},
            # $slice tail only: whole arrays run to ~1.2 MB and the transfer
            # alone would blow the budget.
            {"value": {"$slice": -CFG.zrange_limit}},
        ))
        if not idx:
            return
        keys = [DEFAULT_SESSION_PREFIX + m["member"] for m in idx["value"]]
        docs = _timed("mget_sessions", lambda: list(COLL.find({"_id": {"$in": keys}})))
        if docs is not None:
            _MISS["mget_requested"] += len(keys)
            _MISS["mget_returned"] += len(docs)
            _fire("index_round_trip", rt_start,
                  response_length=_blen(idx) + _blen(docs))


class SessionWriter(User):
    """Concurrent write load at WRITE_RATIO of total ops."""

    weight = max(1, round(CFG.write_ratio * 100))
    wait_time = constant_throughput(_PER_USER_RPS)

    def __init__(self, environment: Any) -> None:
        super().__init__(environment)
        self.rng = random.Random()

    @task(6)
    def update_session(self) -> None:
        """SETEX refresh -> $set a field inside value on an existing session."""
        i = sample_session_index(self.rng, CFG.session_doc_count,
                                 CFG.hot_set_ratio, CFG.hot_access_share)
        now = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        res = _timed("update_session", lambda: COLL.update_one(
            {"_id": session_key(CFG.random_seed, i)},
            {"$set": {"value.lastUsedDate": now}},
        ))
        if res is not None:
            _MISS["update_sent"] += 1
            _MISS["update_matched"] += res.matched_count

    @task(2)
    def insert_session(self) -> None:
        """New login -> insert a full session doc (indices above the read range)."""
        i = self.rng.randrange(CFG.session_doc_count * 10, CFG.session_doc_count * 11)
        doc = build_session_doc(CFG, i)

        def do() -> dict[str, Any]:
            try:
                COLL.insert_one(doc)
            except DuplicateKeyError:
                pass  # same high index drawn twice; the doc exists, fine
            return doc

        _timed("insert_session", do)

    @task(2)
    def push_index(self) -> None:
        """ZADD -> $push with $slice cap. Rewrites the doc; inherent to the
        zset shape and legitimately slower than a point read."""
        j = self.rng.randrange(CFG.index_doc_count)
        member = {"member": str(uuidlib.uuid4()),
                  "score": EPOCH_ANCHOR_MS + self.rng.randrange(10**9)}
        _timed("push_index", lambda: COLL.update_one(
            {"_id": index_key(CFG.random_seed, j)},
            {"$push": {"value": {"$each": [member],
                                 "$slice": -CFG.max_members_per_index_doc}}},
        ))
