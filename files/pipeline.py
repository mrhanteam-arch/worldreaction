# pipeline.py — Aggregator 통합 버전
import asyncio
import logging
import os
from datetime import datetime, timezone

log = logging.getLogger("wr.pipeline")

CACHE_TTL_FEED   = 600
CACHE_TTL_SEARCH = 3600

def now_iso():
    return datetime.now(timezone.utc).isoformat()


async def get_feed(force_refresh: bool = False, limit: int = 50) -> dict:
    """
    홈 피드
    흐름: Naver 키워드 → Reddit/YouTube 검색 → JP/CN RSS → 균등 믹싱
    """
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
        log.error("Aggregator 실패, 폴백 실행: %s", e)
        result = await _fallback_feed(limit)

    try:
        import cache
        await cache.set("feed:home", result, CACHE_TTL_FEED)
    except Exception:
        pass

    return result


async def _fallback_feed(limit: int = 50) -> dict:
    """Aggregator 실패 시 기존 방식으로 폴백"""
    import aiohttp
    from aggregator import YouTubeCollector, RedditCollector

    yt      = YouTubeCollector()
    reddit  = RedditCollector()

    FALLBACK_QUERIES = [
        "korea trending news", "kpop world reaction",
        "south korea international", "korea technology",
    ]

    connector = aiohttp.TCPConnector(limit=10, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for q in FALLBACK_QUERIES:
            tasks.append(yt.search(session, q, limit=8))
            tasks.append(reddit.search(session, q, limit=8))
        results = await asyncio.gather(*tasks, return_exceptions=True)

    posts = []
    for r in results:
        if isinstance(r, list):
            posts.extend(r)

    # JP / CN
    try:
        from japan_collector import JapanCollector
        jp_posts = await JapanCollector().collect(limit=15)
    except Exception:
        jp_posts = []

    try:
        from china_collector import ChinaCollector
        cn_posts = await ChinaCollector().collect(limit=15)
    except Exception:
        cn_posts = []

    posts.extend(jp_posts)
    posts.extend(cn_posts)

    seen, unique = set(), []
    for p in posts:
        if p["id"] not in seen:
            seen.add(p["id"])
            unique.append(p)

    unique.sort(key=lambda p: p.get("score", 0), reverse=True)
    return {"posts": unique[:limit], "total": len(unique), "cached": False, "updated_at": now_iso()}


async def search(query: str, force_refresh: bool = False) -> dict:
    """검색 — 키워드로 Reddit + YouTube 동시 검색"""
    try:
        import cache
        if not force_refresh:
            cached = await cache.get(f"search:{query}")
            if cached:
                return {**cached, "cached": True}
    except Exception:
        pass

    import aiohttp
    from aggregator import RedditCollector, YouTubeCollector

    reddit  = RedditCollector()
    youtube = YouTubeCollector()

    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(
            reddit.search(session, query, limit=15),
            youtube.search(session, query, limit=10),
            return_exceptions=True
        )

    posts = []
    for r in results:
        if isinstance(r, list):
            posts.extend(r)

    # 검색어 관련성 필터
    q = query.lower()
    relevant = [p for p in posts if q in p.get("originalTitle", "").lower()]
    if not relevant:
        relevant = posts  # 관련 없으면 전체 반환

    # AI 재작성 (상위 8개)
    from aggregator import Aggregator
    agg = Aggregator()
    top = relevant[:8]
    await asyncio.gather(*[agg._rewrite(p) for p in top], return_exceptions=True)

    payload = {"query": query, "posts": relevant, "total": len(relevant), "cached": False}

    try:
        import cache
        await cache.set(f"search:{query}", payload, CACHE_TTL_SEARCH)
    except Exception:
        pass

    return payload
