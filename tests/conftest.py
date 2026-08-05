import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from config import Config  # noqa: E402


def make_cfg(**overrides) -> Config:
    """Small, fast Config for tests; same ratios as the real defaults."""
    base = dict(
        mongodb_uri="", mongodb_db="test", mongodb_collection="redis_dump",
        max_pool_size=200, read_preference="primary", write_concern=1,
        shard_count=3, total_document_count=1000, string_share=0.86,
        zset_share=0.14, synthetic_user_count=138, sessions_per_user_skew=1.5,
        max_members_per_index_doc=200, mean_members_per_index_doc=25,
        index_doc_drift_ratio=0.4, payload_target_bytes=3200,
        session_ttl_days=7, expired_session_ratio=0.1, random_seed=42,
        enable_ttl_index=False, generate_workers=4, load_batch_size=50,
        load_checkpoint_path="./.load_checkpoint.json", hot_set_ratio=0.05,
        hot_access_share=0.80, locust_users=200, locust_spawn_rate=20,
        locust_run_time="10m", target_read_rps=2000, write_ratio=0.1,
        index_read_ratio=0.2, zrange_limit=50, read_latency_slo_ms=10,
    )
    base.update(overrides)
    return Config(**base)


@pytest.fixture
def cfg() -> Config:
    return make_cfg()
