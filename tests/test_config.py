import pytest

from config import ConfigError, load_config

NO_ENV = "/nonexistent/.env"  # keep the repo's real .env out of these tests


def _clean(monkeypatch, **env):
    for k in ("MONGODB_URI", "STRING_SHARE", "ZSET_SHARE", "SESSIONS_PER_USER_SKEW",
              "TOTAL_DOCUMENT_COUNT", "WRITE_RATIO", "LOAD_BATCH_SIZE",
              "MEAN_MEMBERS_PER_INDEX_DOC", "MAX_MEMBERS_PER_INDEX_DOC"):
        monkeypatch.delenv(k, raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)


def test_missing_uri_fails_fast(monkeypatch):
    _clean(monkeypatch)
    with pytest.raises(ConfigError, match="MONGODB_URI"):
        load_config(env_file=NO_ENV)


def test_uri_not_required_for_offline_tools(monkeypatch):
    _clean(monkeypatch)
    cfg = load_config(env_file=NO_ENV, require_uri=False)
    assert cfg.total_document_count == 16_000_000
    assert cfg.session_doc_count + cfg.index_doc_count + 2 == cfg.total_document_count


def test_shares_must_sum_to_one(monkeypatch):
    _clean(monkeypatch, MONGODB_URI="mongodb://x", STRING_SHARE="0.9", ZSET_SHARE="0.2")
    with pytest.raises(ConfigError, match="STRING_SHARE"):
        load_config(env_file=NO_ENV)


def test_ratio_bounds(monkeypatch):
    _clean(monkeypatch, MONGODB_URI="mongodb://x", WRITE_RATIO="1.5")
    with pytest.raises(ConfigError, match="WRITE_RATIO"):
        load_config(env_file=NO_ENV)


def test_pareto_shape_bound(monkeypatch):
    _clean(monkeypatch, MONGODB_URI="mongodb://x", SESSIONS_PER_USER_SKEW="1.0")
    with pytest.raises(ConfigError, match="SKEW"):
        load_config(env_file=NO_ENV)


def test_mean_cannot_exceed_max_members(monkeypatch):
    _clean(monkeypatch, MONGODB_URI="mongodb://x",
           MEAN_MEMBERS_PER_INDEX_DOC="100", MAX_MEMBERS_PER_INDEX_DOC="50")
    with pytest.raises(ConfigError, match="MEMBERS"):
        load_config(env_file=NO_ENV)


def test_positive_integer_knobs(monkeypatch):
    _clean(monkeypatch, MONGODB_URI="mongodb://x", LOAD_BATCH_SIZE="0")
    with pytest.raises(ConfigError, match="LOAD_BATCH_SIZE"):
        load_config(env_file=NO_ENV)
