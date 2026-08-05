import random
import re

from conftest import make_cfg
from model import (
    DEFAULT_SESSION_PREFIX,
    SESSION_NAMESPACES,
    ghost_session_uuid,
    index_key,
    sample_session_index,
    session_key,
    session_uuid,
)

UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


def test_session_key_deterministic():
    assert session_key(42, 123) == session_key(42, 123)
    assert session_key(42, 123) != session_key(42, 124)
    assert session_key(43, 123) != session_key(42, 123)


def test_session_key_shape():
    templates = {t for t, _ in SESSION_NAMESPACES}
    for i in range(500):
        key = session_key(42, i)
        uuid = key.rsplit(":", 1)[1]
        assert UUID_RE.match(uuid), key
        assert key.replace(uuid, "{u}") in templates, key
        assert uuid == session_uuid(42, i)


def test_namespace_distribution_matches_export():
    total = sum(w for _, w in SESSION_NAMESPACES)
    n = 30_000
    hits = sum(1 for i in range(n)
               if session_key(42, i).startswith(DEFAULT_SESSION_PREFIX))
    expected = 2407 / total
    assert abs(hits / n - expected) < 0.02


def test_ghost_uuid_disjoint_derivation():
    for i in range(200):
        assert ghost_session_uuid(42, i) != session_uuid(42, i)
        assert UUID_RE.match(ghost_session_uuid(42, i))


def test_index_key_shape():
    key = index_key(42, 7)
    assert re.match(
        r"^AUTH0_DEFAULT_INJECTOR:idx:user:[A-Za-z0-9]{32}@clients:sessions$", key)
    assert index_key(42, 7) == index_key(42, 7)
    assert index_key(42, 8) != key


def test_generator_and_harness_agree_on_keys(cfg):
    """The harness must be able to derive any generated _id in O(1)."""
    from generate import build_index_doc, build_session_doc
    for i in (0, 1, 137, 859):
        assert build_session_doc(cfg, i)["_id"] == session_key(cfg.random_seed, i)
    for j in (0, 5, 137):
        assert build_index_doc(cfg, j)["_id"] == index_key(cfg.random_seed, j)


def test_hot_set_sampling_distribution():
    rng = random.Random(1)
    n, hot_ratio, hot_share = 100_000, 0.05, 0.80
    boundary = 100_000 - int(n * hot_ratio)
    draws = [sample_session_index(rng, n, hot_ratio, hot_share) for _ in range(20_000)]
    in_hot = sum(1 for d in draws if d >= boundary) / len(draws)
    assert abs(in_hot - hot_share) < 0.02
    assert all(0 <= d < n for d in draws)
