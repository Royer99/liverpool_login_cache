"""Post-load verification and pre-flight checks.

Proves — rather than asserts — the access paths and environment:
counts by type, index inventory, explain() on both read paths, storage vs
data size, raw RTT against the latency budget, and hot-working-set size
against the cluster's WiredTiger cache.

Standalone usage:
    python src/verify.py                # everything
    python src/verify.py --preflight    # RTT + cache sizing only (fast)
"""

from __future__ import annotations

import argparse
import json
import random
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import Config, load_config  # noqa: E402
from generate import build_session_doc  # noqa: E402
from model import (  # noqa: E402
    EPOCH_ANCHOR_MS,
    INDEX_DOC_FIELDS,
    SESSION_DOC_FIELDS,
    sample_session_index,
    session_key,
    session_uuid,
)

MB = 1024 * 1024


def _client(cfg: Config):
    from pymongo import MongoClient
    return MongoClient(cfg.mongodb_uri, maxPoolSize=8)


def preflight(cfg: Config) -> None:
    """Measure raw RTT and compare hot-set estimate to the WT cache size."""
    client = _client(cfg)
    db = client[cfg.mongodb_db]

    rtts = []
    db.command("ping")  # connection + TLS warm-up, excluded
    for _ in range(30):
        t0 = time.perf_counter()
        db.command("ping")
        rtts.append((time.perf_counter() - t0) * 1000)
    p50 = statistics.median(rtts)
    p95 = sorted(rtts)[int(len(rtts) * 0.95)]
    print(f"raw RTT to cluster: p50={p50:.2f} ms  p95={p95:.2f} ms  "
          f"(read SLO {cfg.read_latency_slo_ms} ms)")
    if p50 > cfg.read_latency_slo_ms * 0.5:
        print(f"  WARNING: network alone consumes >50% of the {cfg.read_latency_slo_ms} ms "
              "budget. Is the load generator in the cluster's region?")

    try:
        stats = db.command("collStats", cfg.mongodb_collection)
        id_index = stats.get("indexSizes", {}).get("_id_", 0)
        hot_bytes = stats["size"] * cfg.hot_set_ratio + id_index
        print(f"hot-set estimate: {hot_bytes / MB:,.0f} MB "
              f"(dataSize {stats['size'] / MB:,.0f} MB x HOT_SET_RATIO "
              f"{cfg.hot_set_ratio} + _id index {id_index / MB:,.0f} MB)")
    except Exception:
        print("hot-set estimate: collection not loaded yet; run after src/load.py")
        hot_bytes = None
    try:
        wt = db.command("serverStatus")["wiredTiger"]["cache"]
        cache = int(wt["maximum bytes configured"])
        print(f"WiredTiger cache on primary: {cache / MB:,.0f} MB")
        if hot_bytes and hot_bytes > cache * 0.8:
            print("  WARNING: hot set does not fit comfortably in cache; p99 will be "
                  "NVMe-bound. Use a larger tier or lower HOT_SET_RATIO honestly.")
    except Exception as e:
        print(f"  (serverStatus unavailable on this user/tier: {e})")


def verify_counts(cfg: Config) -> None:
    """Compare per-type counts against the configured mix."""
    coll = _client(cfg)[cfg.mongodb_db][cfg.mongodb_collection]
    print("counts by type (expected -> actual):")
    expected = {"string": cfg.session_doc_count, "zset": cfg.index_doc_count,
                "stream": 1, "hash": 1}
    ok = True
    for t, exp in expected.items():
        n = coll.count_documents({"type": t})
        flag = "" if n == exp else "  <-- MISMATCH"
        ok &= n == exp
        print(f"  {t:7} {exp:>12,} -> {n:>12,}{flag}")
    print("indexes:", [i["name"] for i in coll.list_indexes()])
    if not ok:
        print("  (mismatches usually mean an interrupted load; rerun src/load.py to resume)")


def verify_shapes(cfg: Config) -> None:
    """Spot-check stored documents against the canonical field sets."""
    coll = _client(cfg)[cfg.mongodb_db][cfg.mongodb_collection]
    s = coll.find_one({"_id": session_key(cfg.random_seed, 0)})
    assert s, "session doc 0 missing"
    assert set(s) <= SESSION_DOC_FIELDS and {"_id", "shard", "type", "value", "expiresAt"} <= set(s), \
        f"unexpected session fields: {set(s)}"
    z = coll.find_one({"type": "zset"})
    assert z and set(z) == INDEX_DOC_FIELDS, f"unexpected zset fields: {z and set(z)}"
    assert isinstance(z["value"], list) and {"member", "score"} == set(z["value"][0])
    print("stored document shapes match the export envelope")


JWT_PAYLOAD_KEYS = {
    "https://liverpool.com.mx/anon_id", "https://liverpool.com.mx/gcp-migrated",
    "https://liverpool.com.mx/isReliable", "https://liverpool.com.mx/isSignUp",
    "https://liverpool.com.mx/is_anonymous", "https://liverpool.com.mx/phoneHash",
    "https://liverpool.com.mx/phone_number", "https://liverpool.com.mx/prn",
    "https://liverpool.com.mx/prnLegacy", "https://liverpool.com.mx/roles",
    "iss", "sub", "aud", "iat", "exp", "scope", "gty", "azp",
}


def verify_jwt(cfg: Config, sample_size: int = 200) -> None:
    """Decode accessToken JWTs from STORED documents and validate structure.

    Checks, per sampled session doc: 3 base64url segments; RS256 header with
    a FAKE- kid (proving no real token leaked in); the namespaced payload
    claim set observed in the export; a 342-char (2048-bit RSA-length)
    signature; and consistency between the raw JWT claims and the normalized
    decodeAccessToken object stored beside it.
    """
    import base64

    def b64(s: str) -> dict:
        return json.loads(base64.urlsafe_b64decode(s + "=" * (-len(s) % 4)))

    coll = _client(cfg)[cfg.mongodb_db][cfg.mongodb_collection]
    docs = list(coll.aggregate([{"$match": {"type": "string"}},
                                {"$sample": {"size": sample_size}}]))
    assert docs, "no session documents in the collection"
    kids = set()
    for d in docs:
        tok = d["value"]["accessToken"]
        parts = tok.split(".")
        assert len(parts) == 3, f"{d['_id']}: token is not a 3-segment JWT"
        header = b64(parts[0])
        assert header.get("alg") == "RS256" and header.get("typ") == "JWT", \
            f"{d['_id']}: bad JWT header {header}"
        assert str(header.get("kid", "")).startswith("FAKE-"), \
            f"{d['_id']}: kid not FAKE- marked — a real token may have leaked in"
        kids.add(header["kid"])
        payload = b64(parts[1])
        assert set(payload) == JWT_PAYLOAD_KEYS, \
            f"{d['_id']}: payload keys diverge: {set(payload) ^ JWT_PAYLOAD_KEYS}"
        assert len(parts[2]) == 342, f"{d['_id']}: signature length {len(parts[2])} != 342"
        dec = d["value"]["decodeAccessToken"]
        assert payload["sub"] == dec["sub"], f"{d['_id']}: sub mismatch jwt vs decode"
        assert payload["azp"] == dec["azp"], f"{d['_id']}: azp mismatch"
        assert payload["iat"] == dec["iat"] and payload["exp"] == dec["exp"], \
            f"{d['_id']}: iat/exp mismatch"
        assert payload["https://liverpool.com.mx/phoneHash"] == dec["phoneHash"]
        assert payload["https://liverpool.com.mx/gcp-migrated"] == dec["gcpMigrated"]
        assert payload["https://liverpool.com.mx/is_anonymous"] == dec["isAnonymous"]
    print(f"JWT check: {len(docs)} stored tokens decoded — RS256 header, "
          f"expected claim set, 342-char signature, claims consistent with "
          f"decodeAccessToken; signing kid(s): {sorted(kids)}")


def verify_explain(cfg: Config) -> None:
    """Print winning plans for both read paths; the point read must be IDHACK/EXPRESS."""
    coll = _client(cfg)[cfg.mongodb_db][cfg.mongodb_collection]
    rng = random.Random(1)
    i = sample_session_index(rng, cfg.session_doc_count, cfg.hot_set_ratio,
                             cfg.hot_access_share)
    key = session_key(cfg.random_seed, i)

    exp = coll.find({"_id": key}).explain()
    plan = exp["queryPlanner"]["winningPlan"]
    stage = plan.get("stage") or plan.get("queryPlan", {}).get("stage")
    stats = exp["executionStats"]
    print(f"point read winning plan: {stage}  "
          f"(examined docs={stats['totalDocsExamined']}, keys={stats['totalKeysExamined']}, "
          f"serverside {stats['executionTimeMillis']} ms)")
    assert stage in ("IDHACK", "EXPRESS_IXSCAN", "EXPRESS_CLUSTERED_IXSCAN"), \
        f"point read is not an _id express/idhack lookup: {json.dumps(plan)[:400]}"

    keys = [session_key(cfg.random_seed, rng.randrange(cfg.session_doc_count))
            for _ in range(cfg.zrange_limit)]
    exp = coll.find({"_id": {"$in": keys}}).explain()
    stats = exp["executionStats"]
    print(f"$in batch fetch: examined keys={stats['totalKeysExamined']}, "
          f"docs={stats['totalDocsExamined']}, returned={stats['nReturned']}, "
          f"serverside {stats['executionTimeMillis']} ms (uses _id index)")

    # Customer query indexes: derive real stored values client-side
    # (generation is deterministic) and prove each lookup is an IXSCAN.
    doc = None
    for i in range(50):
        doc = build_session_doc(cfg, i)
        if "profile" in doc["value"]:
            break
    assert doc and "profile" in doc["value"], "no non-anonymous session in first 50"

    def plan_of(e: dict) -> str:
        return json.dumps(e["queryPlanner"]["winningPlan"])

    exp = coll.find({"value.profile.email": doc["value"]["profile"]["email"]}) \
              .limit(20).explain()
    stats = exp["executionStats"]
    assert "email_1" in plan_of(exp), \
        f"email lookup not using email_1: {plan_of(exp)[:400]}"
    print(f"email lookup: uses email_1, returned={stats['nReturned']}, "
          f"keys={stats['totalKeysExamined']}, serverside {stats['executionTimeMillis']} ms")

    exp = coll.find({"value.accessToken": doc["value"]["accessToken"]}).limit(1).explain()
    stats = exp["executionStats"]
    assert "accessToken_1" in plan_of(exp), \
        f"token lookup not using accessToken_1: {plan_of(exp)[:400]}"
    print(f"token lookup: uses accessToken_1, returned={stats['nReturned']}, "
          f"keys={stats['totalKeysExamined']}, serverside {stats['executionTimeMillis']} ms")

    from datetime import datetime, timezone
    cutoff = datetime.fromtimestamp((EPOCH_ANCHOR_MS - 43_200_000) / 1000,
                                    tz=timezone.utc) \
        .isoformat(timespec="milliseconds").replace("+00:00", "Z")
    exp = coll.find({"value.deviceInfo.os": "iOS",
                     "value.lastUsedDate": {"$gte": cutoff}}) \
              .sort("value.lastUsedDate", -1).limit(cfg.zrange_limit).explain()
    stats = exp["executionStats"]
    p = plan_of(exp)
    assert "device_lastUsed" in p, f"device query not using device_lastUsed: {p[:400]}"
    assert '"SORT"' not in p, f"device query does an in-memory sort: {p[:400]}"
    print(f"device+recency: uses device_lastUsed (sort by index, no SORT stage), "
          f"returned={stats['nReturned']}, keys={stats['totalKeysExamined']}, "
          f"serverside {stats['executionTimeMillis']} ms")


def verify_storage(cfg: Config) -> None:
    """Report logical vs on-disk size so the compression ratio is a fact."""
    db = _client(cfg)[cfg.mongodb_db]
    st = db.command("collStats", cfg.mongodb_collection)
    if st["storageSize"] < st["size"] * 0.01:
        print(f"dataSize {st['size'] / MB:,.1f} MB | storageSize not yet reported by "
              "WiredTiger (fresh load) — rerun in a minute for the compression ratio")
        return
    print(f"dataSize {st['size'] / MB:,.1f} MB | storageSize {st['storageSize'] / MB:,.1f} MB "
          f"| compression {st['size'] / max(st['storageSize'], 1):.2f}x "
          f"| avgObjSize {st['avgObjSize']} B")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--preflight", action="store_true", help="RTT + cache sizing only")
    args = p.parse_args()
    cfg = load_config()
    preflight(cfg)
    if args.preflight:
        return
    print()
    verify_counts(cfg)
    verify_shapes(cfg)
    verify_jwt(cfg)
    verify_explain(cfg)
    verify_storage(cfg)


if __name__ == "__main__":
    main()
