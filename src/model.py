"""Document envelope and deterministic key derivation.

This module is the single source of truth for the Redis-export document shape
and for key derivation. Both the generator and the Locust harness import it,
so any random index can be turned into a valid ``_id`` in O(1) with no lookup
table.

Everything here was confirmed against the QA RDB dumps (Redis 7 / RDB v10):

- Session strings are ``keyv`` envelopes ``{"value": "<json string>",
  "expires": <epoch_ms>}``; the inner payload is double-encoded JSON. In
  MongoDB the inner payload is stored parsed, under ``value``.
- Session keys span nine brand injector namespaces, and ~6% of them carry no
  ``keyv::`` prefix. Weights below are the observed per-namespace counts.
- Per-user zset index keys all live in ``AUTH0_DEFAULT_INJECTOR`` and score
  members with epoch **milliseconds** (integers, not seconds).
"""

from __future__ import annotations

import hashlib
import random
from datetime import datetime, timezone
from typing import Any

# Fixed anchor for deterministic timestamps: generation must be byte-identical
# under a given seed, so wall-clock time never enters document content.
EPOCH_ANCHOR_MS = int(datetime(2026, 8, 1, tzinfo=timezone.utc).timestamp() * 1000)

# (prefix template, observed count in shard 1 of the QA export).
# ``{u}`` is the session uuid. Order matters for deterministic selection.
SESSION_NAMESPACES: list[tuple[str, int]] = [
    ("keyv::AUTH0_DEFAULT_INJECTOR:session:{u}", 2407),
    ("keyv::AUTH0_SUBURBIA_INJECTOR:session:{u}", 818),
    ("keyv::AUTH0_LIVESTORE_INJECTOR:session:{u}", 289),
    ("AUTH0_DEFAULT_INJECTOR:session:{u}", 200),
    ("keyv::AUTH0_DOCKERS_INJECTOR:session:{u}", 193),
    ("keyv::AUTH0_GAP_INJECTOR:session:{u}", 120),
    ("keyv::AUTH0_TOYS_R_INJECTOR:session:{u}", 109),
    ("keyv::AUTH0_WEST_ELM_INJECTOR:session:{u}", 97),
    ("keyv::AUTH0_POTTERY_INJECTOR:session:{u}", 81),
    ("keyv::AUTH0_WILLIAM_SONOMA_INJECTOR:session:{u}", 72),
    ("AUTH0_SUBURBIA_INJECTOR:session:{u}", 71),
]
_NS_TOTAL = sum(w for _, w in SESSION_NAMESPACES)

INDEX_KEY_TEMPLATE = "AUTH0_DEFAULT_INJECTOR:idx:user:{client_id}@clients:sessions"
# The two-step read path composes full session keys from bare zset members;
# all idx zsets live in the DEFAULT injector namespace, so live members must
# reference sessions under this prefix.
DEFAULT_SESSION_PREFIX = "keyv::AUTH0_DEFAULT_INJECTOR:session:"
STREAM_KEY = "bull:token-refresh:events"
HASH_KEY = "bull:token-refresh:meta"

# Top-level document fields, used by shape tests and verify.py.
SESSION_DOC_FIELDS = {"_id", "shard", "type", "value", "expiresAt", "ttlSeconds"}
INDEX_DOC_FIELDS = {"_id", "shard", "type", "value"}

_CLIENT_ID_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"


def _digest(*parts: object, size: int = 20) -> bytes:
    return hashlib.blake2b(":".join(str(p) for p in parts).encode(), digest_size=size).digest()


def session_uuid(seed: int, i: int) -> str:
    """Deterministic UUID4-shaped session id for index *i* under *seed*."""
    h = _digest(seed, i, size=16).hex()
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def ghost_session_uuid(seed: int, i: int) -> str:
    """UUID for a drifted index member whose session document does not exist.

    Uses a salted derivation so it can never collide with `session_uuid`
    output for any live index.
    """
    h = _digest("ghost", seed, i, size=16).hex()
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def session_key(seed: int, i: int) -> str:
    """Full Redis key (``_id``) for session index *i* under *seed*.

    The injector namespace (and presence of the ``keyv::`` prefix) is derived
    from the same hash, so the ``_id`` distribution matches the multi-banner
    mix observed in the export. O(1), no lookup table: the harness calls this
    with a random index to build a guaranteed-existing key.
    """
    u = session_uuid(seed, i)
    sel = int.from_bytes(_digest("ns", seed, i, size=4), "big") % _NS_TOTAL
    for template, weight in SESSION_NAMESPACES:
        if sel < weight:
            return template.format(u=u)
        sel -= weight
    raise AssertionError("unreachable")


def client_id(seed: int, j: int) -> str:
    """Deterministic Auth0-style 32-char client id for principal *j*."""
    # 24 bytes = 192 bits >= 32 chars x log2(62): no degenerate 'A' tail.
    d = _digest("client", seed, j, size=24)
    n = int.from_bytes(d, "big")
    out = []
    for _ in range(32):
        n, r = divmod(n, len(_CLIENT_ID_ALPHABET))
        out.append(_CLIENT_ID_ALPHABET[r])
    return "".join(out)


def index_key(seed: int, j: int) -> str:
    """Full Redis key (``_id``) for the index document of principal *j*."""
    return INDEX_KEY_TEMPLATE.format(client_id=client_id(seed, j))


def shard_name(i: int, shard_count: int) -> str:
    """Round-robin source-shard label (``node1``..``nodeN``)."""
    return f"node{(i % shard_count) + 1}"


def doc_rng(seed: int, kind: str, i: int) -> random.Random:
    """Per-document RNG derived from (seed, kind, index) only.

    Guarantees document content is independent of generation order and worker
    count.
    """
    return random.Random(int.from_bytes(_digest(kind, seed, i, size=8), "big"))


def session_created_ms(seed: int, i: int, session_count: int, ttl_days: int,
                       expired_ratio: float) -> int:
    """Deterministic creation time for session *i*, older for lower indices.

    Creation times spread over a window sized so that exactly
    ``expired_ratio`` of sessions have ``created + ttl`` in the past relative
    to EPOCH_ANCHOR_MS. The newest sessions are the highest indices, which is
    what makes tail-sampling equal recency-sampling.
    """
    ttl_ms = ttl_days * 86_400_000
    window_ms = ttl_ms / (1.0 - expired_ratio) if expired_ratio < 1.0 else ttl_ms * 10
    frac = (i + 1) / session_count
    jitter = doc_rng(seed, "ts", i).uniform(0, window_ms / session_count)
    return int(EPOCH_ANCHOR_MS - window_ms * (1.0 - frac) - jitter)


def sample_session_index(rng: random.Random, session_count: int,
                         hot_set_ratio: float, hot_access_share: float) -> int:
    """Draw a session index under the recency-skewed access distribution.

    ``hot_access_share`` of draws land uniformly in the most recent
    ``hot_set_ratio`` of indices (the tail); the rest are uniform over the
    cold range.
    """
    hot = max(1, int(session_count * hot_set_ratio))
    boundary = session_count - hot
    if rng.random() < hot_access_share or boundary == 0:
        return rng.randrange(boundary, session_count)
    return rng.randrange(0, boundary)


def ensure_indexes(coll: Any, enable_ttl_index: bool, create_type_index: bool,
                   create_query_indexes: bool = True) -> list[str]:
    """Create the (few) optional secondary indexes; returns names created.

    The hot read path (point read by ``_id``) needs none: ``_id`` is the
    Redis key and its unique index comes for free. The query indexes serve
    the ADDITIONAL customer-requested lookups (by email, by access token,
    by device type + recency) and never touch the point-read plan. All are
    partial: session-only fields would otherwise index 2.2M zset docs as
    null. Call this only AFTER a bulk load.
    """
    created: list[str] = []
    if create_type_index:
        created.append(coll.create_index([("type", 1)], name="type_1"))
    if create_query_indexes:
        created.append(coll.create_index(
            [("value.profile.email", 1)], name="email_1",
            partialFilterExpression={"value.profile.email": {"$exists": True}},
        ))
        created.append(coll.create_index(
            [("value.accessToken", 1)], name="accessToken_1",
            partialFilterExpression={"value.accessToken": {"$exists": True}},
        ))
        created.append(coll.create_index(
            [("value.deviceInfo.os", 1), ("value.lastUsedDate", -1)],
            name="device_lastUsed",
            partialFilterExpression={"value.deviceInfo.os": {"$exists": True}},
        ))
    if enable_ttl_index:
        # Off by default: the dataset carries past expiries on purpose and
        # would delete itself mid-benchmark.
        created.append(coll.create_index(
            [("expiresAt", 1)], name="ttl_expiresAt", expireAfterSeconds=0,
            partialFilterExpression={"type": "string"},
        ))
    return created
