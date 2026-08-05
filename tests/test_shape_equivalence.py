"""Generated documents must be shape-identical to the (hand-written) export
fixture: same key sets, same value types, recursively."""

import json
from pathlib import Path

import pytest

from generate import (
    _to_jsonable,
    build_hash_doc,
    build_index_doc,
    build_session_doc,
    build_stream_doc,
)

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "export_shaped.json").read_text())


def shape(o):
    if isinstance(o, dict):
        return {k: shape(v) for k, v in o.items()}
    if isinstance(o, list):
        return [shape(o[0])] if o else []
    return type(o).__name__


def _pick_full_session(cfg):
    """Find a generated session doc carrying the optional fields
    (ttlSeconds, padding, profile) so the comparison covers the full key set."""
    for i in range(200):
        d = build_session_doc(cfg, i)
        if "ttlSeconds" in d and "padding" in d["value"] and "profile" in d["value"]:
            return d
    pytest.fail("no session doc with ttlSeconds+padding+profile in first 200")


def test_session_shape_matches_export(cfg):
    gen = shape(_to_jsonable(_pick_full_session(cfg)))
    fix = shape(FIXTURE["session"])
    assert gen == fix


def test_zset_shape_matches_export(cfg):
    assert shape(build_index_doc(cfg, 3)) == shape(FIXTURE["zset"])


def test_stream_shape_matches_export(cfg):
    assert shape(build_stream_doc(cfg)) == shape(FIXTURE["stream"])


def test_hash_shape_matches_export(cfg):
    assert shape(build_hash_doc(cfg)) == shape(FIXTURE["hash"])


def test_optional_fields_are_the_only_variance(cfg):
    """Across many docs, the only keys allowed to vary are the optional ones."""
    key_sets = {frozenset(build_session_doc(cfg, i)) for i in range(300)}
    assert all(ks | {"ttlSeconds"} == set(FIXTURE["session"]) for ks in key_sets)
    value_key_sets = {frozenset(build_session_doc(cfg, i)["value"]) for i in range(300)}
    full = set(FIXTURE["session"]["value"])
    assert all(vk | {"padding", "profile"} == full for vk in value_key_sets)
