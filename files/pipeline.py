# pipeline.py — Aggregator 통합 버전
import asyncio
import logging
from datetime import datetime, timezone

log = logging.getLogger("wr.pipeline")

CACHE_TTL_FEED   = 600
CACHE_TTL_SEARCH = 3600

def now_iso():
    return datetime.now(timezone.utc).isoformat()


async def get_feed(force_refresh: bool = False, limit: int = 50) -> dict:
    try:
        import cache
        if not force_refresh:
            cached = await cache.get("feed:home")
            if cached:
                return {**cached, "cached": True}
    except Exception:
        pass

    try:
        from aggregator import Aggregator
        result = await Aggregator().run(limit=limit)
    except Exception as e:
        log.error("Aggregator 실패: %s", e)
        result = {"posts": [], "total": 0, "cached": False, "updated_at": now_iso(), "error": str(e)}

    try:
        import cache
        await cache.set("feed:home", result, CACHE_TTL_FEED)
    except Exception:
        pass

    return result


async def search(query: str, force_refresh: bool = False) -> dict:
    try:
        import cache
        if not force_refresh:
            cached = await cache.get(f"search:{query}")
            if cached:
                return {**cached, "cached": True}
    except Exception:
        pass

    try:
        from aggregator import Aggregator
        result = await Aggregator().search(query)
    except Exception as e:
        log.error("Search 실패: %s", e)
        result = {"query": query, "posts": [], "total": 0, "cached": False, "error": str(e)}

    try:
        import cache
        await cache.set(f"search:{query}", result, CACHE_TTL_SEARCH)
    except Exception:
        pass

    return result
