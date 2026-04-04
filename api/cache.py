"""
Redis cache layer.
All TTL constants defined here as named constants.
Failures are non-fatal — cache misses fall through to DB.
"""
import json
import os
from typing import Any, Optional

import redis
from loguru import logger

# ── TTL constants (seconds) ───────────────────────────────────────────────────
TTL_RANKING          = 600     # 10 min — changes only after pipeline reruns
TTL_STATS            = 300     # 5 min  — pipeline log changes rarely
TTL_MUNICIPIO_DETAIL = 3600    # 1 hour — municipality data is stable
TTL_SCORE            = 3600    # 1 hour — scores are pipeline output
TTL_INSIGHTS         = 86400   # 24 hours — AI generation is expensive
TTL_LONG             = 43200   # 12 hours — setores/PDVs mudam só após pipeline

_redis_client: Optional[redis.Redis] = None


def get_redis() -> redis.Redis:
    global _redis_client
    if _redis_client is None:
        url = os.getenv("REDIS_URL", "redis://redis:6379")
        _redis_client = redis.from_url(url, decode_responses=True)
        logger.info(f"Redis connected: {url}")
    return _redis_client


def cache_get(key: str) -> Optional[Any]:
    try:
        raw = get_redis().get(key)
        return json.loads(raw) if raw is not None else None
    except Exception as e:
        logger.warning(f"Cache GET failed [{key}]: {e}")
        return None


def cache_set(key: str, value: Any, ttl: int) -> None:
    try:
        get_redis().setex(key, ttl, json.dumps(value, default=str))
    except Exception as e:
        logger.warning(f"Cache SET failed [{key}]: {e}")


def cache_invalidate(pattern: str) -> int:
    """Delete all keys matching a glob pattern. Returns count deleted."""
    try:
        keys = get_redis().keys(pattern)
        if keys:
            return get_redis().delete(*keys)
        return 0
    except Exception as e:
        logger.warning(f"Cache invalidate failed [{pattern}]: {e}")
        return 0
