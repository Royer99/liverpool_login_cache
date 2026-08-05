"""Environment-driven configuration. Loaded once, validated, fail fast."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


class ConfigError(ValueError):
    """Raised when the environment is missing or inconsistent."""


def _req(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v:
        raise ConfigError(f"{name} is required (set it in .env)")
    return v


def _int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def _float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


def _bool(name: str, default: bool) -> bool:
    return os.environ.get(name, str(default)).strip().lower() in ("1", "true", "yes")


@dataclass(frozen=True)
class Config:
    """All knobs for the generator, loader, and Locust harness."""

    mongodb_uri: str
    mongodb_db: str
    mongodb_collection: str
    max_pool_size: int
    read_preference: str
    write_concern: int

    shard_count: int
    total_document_count: int
    string_share: float
    zset_share: float
    synthetic_user_count: int
    sessions_per_user_skew: float
    max_members_per_index_doc: int
    mean_members_per_index_doc: int
    index_doc_drift_ratio: float
    payload_target_bytes: int
    session_ttl_days: int
    expired_session_ratio: float
    random_seed: int
    enable_ttl_index: bool

    generate_workers: int
    load_batch_size: int
    load_checkpoint_path: str

    hot_set_ratio: float
    hot_access_share: float

    locust_users: int
    locust_spawn_rate: int
    locust_run_time: str
    target_read_rps: int
    write_ratio: float
    index_read_ratio: float
    zrange_limit: int
    read_latency_slo_ms: float

    @property
    def session_doc_count(self) -> int:
        """Number of session (type=string) documents."""
        return round(self.total_document_count * self.string_share)

    @property
    def index_doc_count(self) -> int:
        """Number of per-user index (type=zset) documents. One per principal."""
        # -2 leaves room for the single stream and hash documents.
        return self.total_document_count - self.session_doc_count - 2

    @property
    def hot_set_size(self) -> int:
        """Number of sessions in the hot (recently created) set."""
        return max(1, int(self.session_doc_count * self.hot_set_ratio))


def load_config(env_file: str | os.PathLike[str] | None = None, require_uri: bool = True,
                quiet: bool = False) -> Config:
    """Load .env (or *env_file*), validate, and return an immutable Config.

    With require_uri=False the MongoDB URI may be absent — used by generator
    dry-runs and tests that never touch a cluster. *quiet* suppresses
    informational notes (loader worker processes).
    """
    load_dotenv(env_file or Path(".env"))

    cfg = Config(
        mongodb_uri=_req("MONGODB_URI") if require_uri else os.environ.get("MONGODB_URI", ""),
        mongodb_db=os.environ.get("MONGODB_DB", "liverpool_login"),
        mongodb_collection=os.environ.get("MONGODB_COLLECTION", "redis_dump"),
        max_pool_size=_int("MONGODB_MAX_POOL_SIZE", 200),
        read_preference=os.environ.get("MONGODB_READ_PREFERENCE", "primary"),
        write_concern=_int("MONGODB_WRITE_CONCERN", 1),
        shard_count=_int("SHARD_COUNT", 3),
        total_document_count=_int("TOTAL_DOCUMENT_COUNT", 16_000_000),
        string_share=_float("STRING_SHARE", 0.86),
        zset_share=_float("ZSET_SHARE", 0.14),
        synthetic_user_count=_int("SYNTHETIC_USER_COUNT", 800_000),
        sessions_per_user_skew=_float("SESSIONS_PER_USER_SKEW", 1.5),
        max_members_per_index_doc=_int("MAX_MEMBERS_PER_INDEX_DOC", 16_571),
        mean_members_per_index_doc=_int("MEAN_MEMBERS_PER_INDEX_DOC", 25),
        index_doc_drift_ratio=_float("INDEX_DOC_DRIFT_RATIO", 0.4),
        payload_target_bytes=_int("PAYLOAD_TARGET_BYTES", 3200),
        session_ttl_days=_int("SESSION_TTL_DAYS", 7),
        expired_session_ratio=_float("EXPIRED_SESSION_RATIO", 0.1),
        random_seed=_int("RANDOM_SEED", 42),
        enable_ttl_index=_bool("ENABLE_TTL_INDEX", False),
        generate_workers=_int("GENERATE_WORKERS", 8),
        load_batch_size=_int("LOAD_BATCH_SIZE", 1000),
        load_checkpoint_path=os.environ.get("LOAD_CHECKPOINT_PATH", "./.load_checkpoint.json"),
        hot_set_ratio=_float("HOT_SET_RATIO", 0.05),
        hot_access_share=_float("HOT_ACCESS_SHARE", 0.80),
        locust_users=_int("LOCUST_USERS", 200),
        locust_spawn_rate=_int("LOCUST_SPAWN_RATE", 20),
        locust_run_time=os.environ.get("LOCUST_RUN_TIME", "10m"),
        target_read_rps=_int("TARGET_READ_RPS", 2000),
        write_ratio=_float("WRITE_RATIO", 0.1),
        index_read_ratio=_float("INDEX_READ_RATIO", 0.2),
        zrange_limit=_int("ZRANGE_LIMIT", 50),
        read_latency_slo_ms=_float("READ_LATENCY_SLO_MS", 10),
    )
    _validate(cfg, quiet=quiet)
    return cfg


def _validate(cfg: Config, quiet: bool = False) -> None:
    err: list[str] = []
    if abs(cfg.string_share + cfg.zset_share - 1.0) > 1e-9:
        err.append(f"STRING_SHARE + ZSET_SHARE must equal 1.0, got {cfg.string_share + cfg.zset_share}")
    if cfg.total_document_count < 10:
        err.append("TOTAL_DOCUMENT_COUNT must be at least 10")
    for name in ("index_doc_drift_ratio", "expired_session_ratio", "hot_set_ratio",
                 "hot_access_share", "write_ratio", "index_read_ratio"):
        v = getattr(cfg, name)
        if not 0.0 <= v <= 1.0:
            err.append(f"{name.upper()} must be in [0, 1], got {v}")
    if cfg.sessions_per_user_skew <= 1.0:
        err.append("SESSIONS_PER_USER_SKEW must be > 1.0 (Pareto shape)")
    if cfg.mean_members_per_index_doc > cfg.max_members_per_index_doc:
        err.append("MEAN_MEMBERS_PER_INDEX_DOC exceeds MAX_MEMBERS_PER_INDEX_DOC")
    for name in ("shard_count", "generate_workers", "load_batch_size", "zrange_limit",
                 "locust_users", "payload_target_bytes", "max_pool_size"):
        if getattr(cfg, name) < 1:
            err.append(f"{name.upper()} must be >= 1")
    if err:
        raise ConfigError("; ".join(err))
    if not quiet and cfg.synthetic_user_count != cfg.index_doc_count:
        # Informational, not fatal: one index doc per principal, so the real
        # principal count is derived from ZSET_SHARE * TOTAL.
        print(
            f"[config] note: SYNTHETIC_USER_COUNT={cfg.synthetic_user_count} is "
            f"informational; principal count is derived as index_doc_count="
            f"{cfg.index_doc_count} (ZSET_SHARE * TOTAL - 2)."
        )
