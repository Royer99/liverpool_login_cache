"""Checkpoint/resume correctness for the loader, with a fake collection
standing in for MongoDB (dup-key semantics included)."""

import json

import pytest
from pymongo.errors import BulkWriteError

from conftest import make_cfg
from load import _checkpoint_path, load_range


class FakeCollection:
    """Records inserts, rejects duplicates with code 11000, and can crash
    AFTER recording a batch — the worst case: insert succeeded, checkpoint
    was never written, resume re-sends the batch."""

    def __init__(self):
        self.seen: set[str] = set()
        self.calls = 0
        self.crash_on_call: int | None = None

    def insert_many(self, docs, ordered=False):
        self.calls += 1
        if self.calls == self.crash_on_call:
            self.crash_on_call = None
            for d in docs:
                self.seen.add(d["_id"])
            raise ConnectionError("simulated crash after write, before checkpoint")
        errors = []
        for idx, d in enumerate(docs):
            if d["_id"] in self.seen:
                errors.append({"index": idx, "code": 11000, "errmsg": "dup"})
            else:
                self.seen.add(d["_id"])
        if errors:
            raise BulkWriteError({"writeErrors": errors})


def _expected_ids(cfg, sess_range, idx_range, with_singletons):
    from model import HASH_KEY, STREAM_KEY, index_key, session_key
    ids = {session_key(cfg.random_seed, i) for i in range(*sess_range)}
    ids |= {index_key(cfg.random_seed, j) for j in range(*idx_range)}
    if with_singletons:
        ids |= {STREAM_KEY, HASH_KEY}
    return ids


def test_interrupt_and_resume_yields_exact_dataset(tmp_path):
    cfg = make_cfg(total_document_count=400, load_batch_size=20)
    base = str(tmp_path / "ckpt.json")
    sess_range, idx_range = (0, cfg.session_doc_count), (0, cfg.index_doc_count)
    coll = FakeCollection()
    coll.crash_on_call = 5

    with pytest.raises(ConnectionError):
        load_range(cfg, 0, sess_range, idx_range, base,
                   progress=lambda w, n: None, coll=coll)
    assert _checkpoint_path(base, 0).exists()
    partial = json.loads(_checkpoint_path(base, 0).read_text())
    assert partial["session_next"] < sess_range[1]  # genuinely interrupted

    # Resume against the same "database": dup errors swallowed, rest loaded.
    load_range(cfg, 0, sess_range, idx_range, base,
               progress=lambda w, n: None, coll=coll)
    assert coll.seen == _expected_ids(cfg, sess_range, idx_range, with_singletons=True)

    final = json.loads(_checkpoint_path(base, 0).read_text())
    assert final["session_next"] == sess_range[1]
    assert final["index_next"] == idx_range[1]


def test_resume_refuses_changed_ranges(tmp_path):
    cfg = make_cfg(total_document_count=200, load_batch_size=20)
    base = str(tmp_path / "ckpt.json")
    coll = FakeCollection()
    load_range(cfg, 1, (0, 50), (0, 10), base, progress=lambda w, n: None, coll=coll)
    with pytest.raises(SystemExit, match="GENERATE_WORKERS changed"):
        load_range(cfg, 1, (0, 60), (0, 10), base, progress=lambda w, n: None, coll=coll)


def test_worker_zero_owns_singletons(tmp_path):
    cfg = make_cfg(total_document_count=100, load_batch_size=10)
    base = str(tmp_path / "ckpt.json")
    coll = FakeCollection()
    load_range(cfg, 1, (0, 10), (0, 5), base, progress=lambda w, n: None, coll=coll)
    assert coll.seen == _expected_ids(cfg, (0, 10), (0, 5), with_singletons=False)


def test_progress_reports_every_batch(tmp_path):
    cfg = make_cfg(total_document_count=200, load_batch_size=25)
    base = str(tmp_path / "ckpt.json")
    seen = []
    load_range(cfg, 2, (0, 100), (0, 20), base,
               progress=lambda w, n: seen.append((w, n)), coll=FakeCollection())
    assert sum(n for _, n in seen) == 120
    assert all(w == 2 for w, _ in seen)
