import json
from datetime import datetime, timezone

from conftest import make_cfg
from model import EPOCH_ANCHOR_MS, session_uuid
from generate import (
    build_index_doc,
    build_session_doc,
    index_member_count,
    iter_index_docs,
    iter_session_docs,
)

ANCHOR = datetime.fromtimestamp(EPOCH_ANCHOR_MS / 1000, tz=timezone.utc)


def test_documents_deterministic_under_seed(cfg):
    a = build_session_doc(cfg, 7)
    b = build_session_doc(make_cfg(), 7)
    assert a == b
    assert build_index_doc(cfg, 7) == build_index_doc(make_cfg(), 7)
    assert build_session_doc(make_cfg(random_seed=43), 7) != a


def test_worker_partitioning_yields_identical_dataset(cfg):
    """Splitting a range across N workers must not change any document."""
    whole = list(iter_session_docs(cfg, 0, 60))
    split = (list(iter_session_docs(cfg, 0, 17))
             + list(iter_session_docs(cfg, 17, 41))
             + list(iter_session_docs(cfg, 41, 60)))
    assert whole == split
    whole_idx = list(iter_index_docs(cfg, 0, 20))
    split_idx = list(iter_index_docs(cfg, 0, 9)) + list(iter_index_docs(cfg, 9, 20))
    assert whole_idx == split_idx


def test_access_token_is_rs256_shaped_jwt(cfg):
    """The export's tokens are 3-segment RS256 JWTs (header alg/typ/kid,
    namespaced payload claims, 342-char signature). Ours must match that
    structure while staying cryptographically invalid (random signature,
    FAKE- kid)."""
    import base64

    def b64(s: str) -> bytes:
        return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))

    doc = build_session_doc(cfg, 5)
    tok = doc["value"]["accessToken"]
    parts = tok.split(".")
    assert len(parts) == 3
    header = json.loads(b64(parts[0]))
    assert header["alg"] == "RS256" and header["typ"] == "JWT"
    assert header["kid"].startswith("FAKE-")
    payload = json.loads(b64(parts[1]))
    assert set(payload) == {
        "https://liverpool.com.mx/anon_id", "https://liverpool.com.mx/gcp-migrated",
        "https://liverpool.com.mx/isReliable", "https://liverpool.com.mx/isSignUp",
        "https://liverpool.com.mx/is_anonymous", "https://liverpool.com.mx/phoneHash",
        "https://liverpool.com.mx/phone_number", "https://liverpool.com.mx/prn",
        "https://liverpool.com.mx/prnLegacy", "https://liverpool.com.mx/roles",
        "iss", "sub", "aud", "iat", "exp", "scope", "gty", "azp",
    }  # observed JWT payload key set — distinct from decodeAccessToken's
    assert len(parts[2]) == 342  # 2048-bit RSA signature length, random bytes
    # consistency with the normalized decode stored next to it
    dec = doc["value"]["decodeAccessToken"]
    assert payload["sub"] == dec["sub"]
    assert payload["https://liverpool.com.mx/phoneHash"] == dec["phoneHash"]
    assert payload["https://liverpool.com.mx/gcp-migrated"] == dec["gcpMigrated"]


def test_profile_present_iff_not_anonymous(cfg):
    """profile (email + userinfo) is a documented addition to the export:
    every non-anonymous session carries it, anonymous ones never do, and the
    email is deterministic and confined to RFC 2606 reserved domains."""
    import re

    seen = {True: 0, False: 0}
    for i in range(300):
        doc = build_session_doc(cfg, i)
        anon = doc["value"]["decodeAccessToken"]["isAnonymous"]
        seen[anon] += 1
        if anon:
            assert "profile" not in doc["value"]
            continue
        prof = doc["value"]["profile"]
        assert set(prof) == {"email", "emailVerified", "firstName", "lastName",
                             "phoneNumber"}
        assert re.fullmatch(
            r"[a-z]+\.[a-z]+\d{3}@(example\.(com|net|org)|correo\.example\.mx)",
            prof["email"]), prof["email"]
        assert re.fullmatch(r"\+5255\d{8}", prof["phoneNumber"])
        assert isinstance(prof["emailVerified"], bool)
        assert prof == build_session_doc(cfg, i)["value"]["profile"]
    assert seen[True] > 0 and seen[False] > 0


def test_payload_size_near_target(cfg):
    sizes = [len(json.dumps(build_session_doc(cfg, i)["value"], default=str,
                            separators=(",", ":")))
             for i in range(300)]
    mean = sum(sizes) / len(sizes)
    assert cfg.payload_target_bytes * 0.75 < mean < cfg.payload_target_bytes * 1.35
    assert min(sizes) > 800  # claims alone; nothing degenerate


def test_expired_ratio_and_ttl_presence(cfg):
    docs = [build_session_doc(cfg, i) for i in range(cfg.session_doc_count)]
    expired = sum(1 for d in docs if d["expiresAt"] < ANCHOR) / len(docs)
    assert abs(expired - cfg.expired_session_ratio) < 0.04
    with_ttl = sum(1 for d in docs if "ttlSeconds" in d) / len(docs)
    assert with_ttl > 0.95  # ~1% of source keys had no TTL


def test_recency_ordering(cfg):
    """Higher index == newer session; tail sampling equals recency sampling."""
    old = build_session_doc(cfg, 0)["expiresAt"]
    new = build_session_doc(cfg, cfg.session_doc_count - 1)["expiresAt"]
    assert new > old


def test_member_count_distribution(cfg):
    assert index_member_count(cfg, 0) == cfg.max_members_per_index_doc
    assert index_member_count(cfg, 1) == cfg.max_members_per_index_doc // 2
    counts = [index_member_count(cfg, j) for j in range(2, 2000)]
    mean = sum(counts) / len(counts)
    # Heavy-tailed and capped, so the tolerance is loose on purpose.
    assert cfg.mean_members_per_index_doc * 0.4 < mean < cfg.mean_members_per_index_doc * 2
    assert max(counts) <= cfg.max_members_per_index_doc
    assert min(counts) >= 1


def test_index_drift_ratio(cfg):
    live_uuids = {session_uuid(cfg.random_seed, i) for i in range(cfg.session_doc_count)}
    total = ghosts = 0
    for j in range(2, 80):
        for m in build_index_doc(cfg, j)["value"]:
            total += 1
            ghosts += m["member"] not in live_uuids
    assert abs(ghosts / total - cfg.index_doc_drift_ratio) < 0.06


def test_index_docs_sorted_by_score(cfg):
    scores = [m["score"] for m in build_index_doc(cfg, 3)["value"]]
    assert scores == sorted(scores)
    assert all(isinstance(s, int) for s in scores)  # epoch ms, as observed


def test_type_mix_sums_to_total(cfg):
    assert cfg.session_doc_count + cfg.index_doc_count + 2 == cfg.total_document_count
    assert abs(cfg.session_doc_count / cfg.total_document_count - cfg.string_share) < 0.01
