"""Synthetic data generator emitting the exact QA-export document shape.

Every document is derived purely from ``(RANDOM_SEED, kind, index)``, so
output is byte-identical for a given seed regardless of worker count or
generation order. No real Auth0 tokens or customer data appear anywhere;
token-shaped strings are prefixed ``FAKE.`` on purpose.

Standalone usage:
    python src/generate.py --dry-run --limit 3
    python src/generate.py --jsonl out.jsonl --limit 10000
"""

from __future__ import annotations

import argparse
import base64
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import Config, load_config  # noqa: E402
from model import (  # noqa: E402
    DEFAULT_SESSION_PREFIX,
    EPOCH_ANCHOR_MS,
    HASH_KEY,
    STREAM_KEY,
    client_id,
    doc_rng,
    ghost_session_uuid,
    index_key,
    session_created_ms,
    session_key,
    session_uuid,
    shard_name,
)

_BROWSERS = ["Chrome", "Safari", "Firefox", "Edge", "Samsung Internet", "Chrome Mobile"]
_OS = [("iOS", ["17.5.1", "18.0", "18.1.1"]), ("Android", ["13", "14", "15"]),
       ("Mac OS X", ["14.5", "15.1"]), ("Windows", ["10", "11"])]
_MODELS = ["iPhone14,5", "iPhone15,3", "SM-G991B", "SM-A546E", "Pixel 8", "Moto G54",
           "iPad13,4", "Generic Desktop"]
_CONTEXTS = ["web", "app", "mweb"]
_SCOPES = "openid profile email offline_access read:current_user"


def _fake_ip(rng: Any) -> str:
    # RFC 5737 TEST-NET ranges only, so no real customer IPs can ever appear.
    net = rng.choice(["192.0.2", "198.51.100", "203.0.113"])
    return f"{net}.{rng.randrange(1, 255)}"


def _b64url(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode().rstrip("=")


def _signing_kid(seed: int) -> str:
    """One fake signing-key id per dataset (kids are per key, not per token)."""
    import hashlib
    return "FAKE-" + hashlib.blake2b(f"kid:{seed}".encode(), digest_size=8).hexdigest()


def _fake_jwt(rng: Any, payload: dict[str, Any], kid: str) -> str:
    """RS256-shaped JWT matching the export's token structure: real base64url
    header and payload, signature segment of random bytes (342 chars, like a
    2048-bit RSA signature) — structurally faithful, cryptographically
    invalid on purpose. The FAKE- kid marks it as synthetic."""
    header = {"alg": "RS256", "typ": "JWT", "kid": kid}
    return ".".join((
        _b64url(json.dumps(header, separators=(",", ":")).encode()),
        _b64url(json.dumps(payload, separators=(",", ":")).encode()),
        _b64url(rng.randbytes(256)),
    ))


def _injector_of(key: str) -> str:
    return key.split(":session:")[0].removeprefix("keyv::")


def _payload_size_target(cfg: Config, rng: Any) -> int:
    # Right-skewed like the export (p50 ~2.2 KB, mean ~3.4 KB, max ~5.6 KB).
    return int(cfg.payload_target_bytes * min(rng.lognormvariate(-0.1, 0.45), 2.2))


def build_session_doc(cfg: Config, i: int) -> dict[str, Any]:
    """Session (Redis string) document for index *i*. Mirrors the keyv envelope."""
    rng = doc_rng(cfg.random_seed, "sess", i)
    key = session_key(cfg.random_seed, i)
    uuid = session_uuid(cfg.random_seed, i)
    created_ms = session_created_ms(cfg.random_seed, i, cfg.session_doc_count,
                                    cfg.session_ttl_days, cfg.expired_session_ratio)
    expires_ms = created_ms + cfg.session_ttl_days * 86_400_000
    os_name, versions = rng.choice(_OS)
    anon = rng.random() < 0.3
    owner = client_id(cfg.random_seed, rng.randrange(max(cfg.index_doc_count, 1)))
    iat = created_ms // 1000
    sub = f"auth0|{rng.getrandbits(96):024x}"
    gcp_migrated = rng.random() < 0.5
    phone_hash = rng.getrandbits(128).to_bytes(16, "big").hex()
    aud = ["https://auth.qa.example-retail.mx/userinfo",
           "https://api.example-retail.mx/"]

    # Raw JWT payload: the namespaced claim set observed in the export's
    # tokens. decodeAccessToken below is the app's NORMALIZED decode of it —
    # the two field sets differ in the source data too.
    jwt_claims: dict[str, Any] = {
        "https://liverpool.com.mx/anon_id": uuid if anon else "",
        "https://liverpool.com.mx/gcp-migrated": gcp_migrated,
        "https://liverpool.com.mx/isReliable": rng.random() < 0.8,
        "https://liverpool.com.mx/isSignUp": rng.random() < 0.05,
        "https://liverpool.com.mx/is_anonymous": anon,
        "https://liverpool.com.mx/phoneHash": phone_hash,
        "https://liverpool.com.mx/phone_number": "" if anon else f"+5255{rng.randrange(10**8):08d}",
        "https://liverpool.com.mx/prn": rng.getrandbits(128).to_bytes(16, "big").hex(),
        "https://liverpool.com.mx/prnLegacy": f"{rng.randrange(10**9):09d}",
        "https://liverpool.com.mx/roles": [],
        "iss": "https://auth.qa.example-retail.mx/",
        "sub": sub,
        "aud": aud,
        "iat": iat,
        "exp": iat + 1800,
        "scope": _SCOPES,
        "gty": "password",
        "azp": owner,
    }

    value: dict[str, Any] = {
        "accessToken": _fake_jwt(rng, jwt_claims, _signing_kid(cfg.random_seed)),
        "scope": _SCOPES,
        "expiresIn": 1800,
        "tokenType": "Bearer",
        "injectorName": _injector_of(key),
        "lastUsedDate": datetime.fromtimestamp(
            (created_ms + rng.randrange(0, 3_600_000)) / 1000, tz=timezone.utc
        ).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "deviceInfo": {
            "model": rng.choice(_MODELS),
            "browser": rng.choice(_BROWSERS),
            "osVersion": rng.choice(versions),
            "os": os_name,
            "ip": _fake_ip(rng),
        },
        "context": rng.choice(_CONTEXTS),
        "atgContext": f"atg-{rng.randrange(10**8):08d}",
        "decodeAccessToken": {
            "https://liverpool.com.mx/anon_id": uuid if anon else "",
            "https://liverpool.com.mx/is_anonymous": anon,
            "iss": jwt_claims["iss"],
            "sub": sub,
            "aud": aud,
            "iat": iat,
            "exp": iat + 1800,
            "scope": _SCOPES,
            "gty": "password",
            "azp": owner,
            "requireProgressiveProfiling": rng.random() < 0.1,
            "roles": [],
            "gcpMigrated": gcp_migrated,
            "phoneHash": phone_hash,
            "isAnonymous": anon,
            "anonId": uuid if anon else "",
        },
    }
    base = len(json.dumps(value, separators=(",", ":")))
    pad = _payload_size_target(cfg, rng) - base - 20
    if pad > 0:
        # base64 of random bytes: incompressible-ish so storage figures are honest.
        value["padding"] = base64.b64encode(rng.randbytes(pad * 3 // 4)).decode()

    doc: dict[str, Any] = {
        "_id": key,
        "shard": shard_name(i, cfg.shard_count),
        "type": "string",
        "value": value,
        "expiresAt": datetime.fromtimestamp(expires_ms / 1000, tz=timezone.utc),
    }
    if rng.random() >= 0.01:  # ~1% of source keys were PERSIST'ed (no TTL)
        doc["ttlSeconds"] = cfg.session_ttl_days * 86_400
    return doc


def index_member_count(cfg: Config, j: int) -> int:
    """Skewed members-per-principal draw; principals 0 and 1 approach the max."""
    if j == 0:
        return cfg.max_members_per_index_doc
    if j == 1:
        return max(2, cfg.max_members_per_index_doc // 2)
    rng = doc_rng(cfg.random_seed, "idxn", j)
    a = cfg.sessions_per_user_skew
    scale = cfg.mean_members_per_index_doc * (a - 1.0) / a
    return min(cfg.max_members_per_index_doc, max(1, int(rng.paretovariate(a) * scale)))


def build_index_doc(cfg: Config, j: int) -> dict[str, Any]:
    """Per-user session index (Redis zset) document for principal *j*.

    ``INDEX_DOC_DRIFT_RATIO`` of members reference session ids with no
    backing document, exactly like the drifted zsets in the export. Scores
    are epoch milliseconds (as observed), ascending like ZRANGE output.
    """
    rng = doc_rng(cfg.random_seed, "idx", j)
    members = []
    window_ms = cfg.session_ttl_days * 86_400_000 * 4
    for k in range(index_member_count(cfg, j)):
        if rng.random() < cfg.index_doc_drift_ratio:
            member = ghost_session_uuid(cfg.random_seed, (j << 20) | k)
        else:
            # Live members must be DEFAULT-namespace sessions: the reader
            # composes DEFAULT_SESSION_PREFIX + member, as the real app does.
            for _ in range(64):
                si = rng.randrange(cfg.session_doc_count)
                if session_key(cfg.random_seed, si).startswith(DEFAULT_SESSION_PREFIX):
                    break
            member = session_uuid(cfg.random_seed, si)
        members.append({"member": member,
                        "score": EPOCH_ANCHOR_MS - rng.randrange(window_ms)})
    members.sort(key=lambda m: m["score"])
    return {
        "_id": index_key(cfg.random_seed, j),
        "shard": shard_name(j, cfg.shard_count),
        "type": "zset",
        "value": members,
    }


def build_stream_doc(cfg: Config) -> dict[str, Any]:
    """The single BullMQ stream document (small, fixed)."""
    rng = doc_rng(cfg.random_seed, "stream", 0)
    entries = []
    ms = EPOCH_ANCHOR_MS - 3_600_000
    for k in range(10):
        ms += rng.randrange(1000, 60_000)
        entries.append({
            "id": f"{ms}-0",
            "fields": {
                "event": rng.choice(["active", "waiting", "completed"]),
                "jobId": f"refresh-{ghost_session_uuid(cfg.random_seed, 10_000_000 + k)}",
                "prev": "waiting",
            },
        })
    return {"_id": STREAM_KEY, "shard": shard_name(0, cfg.shard_count),
            "type": "stream", "value": entries}


def build_hash_doc(cfg: Config) -> dict[str, Any]:
    """The single BullMQ meta hash document (fields observed in the export)."""
    return {"_id": HASH_KEY, "shard": shard_name(1, cfg.shard_count), "type": "hash",
            "value": {"opts.maxLenEvents": "10000", "version": "5.34.4"}}


def iter_session_docs(cfg: Config, start: int, end: int) -> Iterator[dict[str, Any]]:
    """Stream session documents for indices ``[start, end)``."""
    for i in range(start, end):
        yield build_session_doc(cfg, i)


def iter_index_docs(cfg: Config, start: int, end: int) -> Iterator[dict[str, Any]]:
    """Stream index documents for principals ``[start, end)``."""
    for j in range(start, end):
        yield build_index_doc(cfg, j)


def _to_jsonable(doc: dict[str, Any]) -> dict[str, Any]:
    out = dict(doc)
    if isinstance(out.get("expiresAt"), datetime):
        out["expiresAt"] = out["expiresAt"].isoformat()
    return out


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--dry-run", action="store_true",
                   help="print sample documents as JSON and exit")
    p.add_argument("--limit", type=int, default=5,
                   help="number of documents (per type for --dry-run)")
    p.add_argument("--jsonl", metavar="PATH",
                   help="write --limit docs (interleaved types) as JSONL to PATH; "
                        "never use this for the full dataset")
    args = p.parse_args()
    cfg = load_config(require_uri=False)

    if args.dry_run:
        for d in iter_session_docs(cfg, 0, min(args.limit, cfg.session_doc_count)):
            print(json.dumps(_to_jsonable(d), indent=2))
        for d in iter_index_docs(cfg, 2, 2 + min(args.limit, 3)):
            print(json.dumps(_to_jsonable(d), indent=2))
        print(json.dumps(_to_jsonable(build_stream_doc(cfg)), indent=2))
        print(json.dumps(_to_jsonable(build_hash_doc(cfg)), indent=2))
        return

    if args.jsonl:
        n_sess = min(args.limit, cfg.session_doc_count)
        n_idx = min(max(args.limit - n_sess, args.limit // 6), cfg.index_doc_count)
        with open(args.jsonl, "w") as f:
            for d in iter_session_docs(cfg, 0, n_sess):
                f.write(json.dumps(_to_jsonable(d), separators=(",", ":")) + "\n")
            for d in iter_index_docs(cfg, 0, n_idx):
                f.write(json.dumps(_to_jsonable(d), separators=(",", ":")) + "\n")
            f.write(json.dumps(_to_jsonable(build_stream_doc(cfg)), separators=(",", ":")) + "\n")
            f.write(json.dumps(_to_jsonable(build_hash_doc(cfg)), separators=(",", ":")) + "\n")
        print(f"wrote {n_sess} session + {n_idx} index docs (+stream/hash) to {args.jsonl}")
        return

    p.error("choose --dry-run or --jsonl (full-scale loading is src/load.py's job)")


if __name__ == "__main__":
    main()
