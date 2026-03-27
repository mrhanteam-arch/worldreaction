# pipeline.py 연결 방법 — 기존 코드 최소 수정
# 아래 내용을 기존 pipeline.py에 추가하면 됩니다.

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1. 파일 상단 import에 추가
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

from japan_collector import JapanCollector
from china_collector import ChinaCollector

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2. get_feed() 함수 내부 tasks 리스트에 추가
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# 기존 코드 (변경 없음):
# tasks = [
#     fetch_youtube(session),
#     fetch_news(session),
#     fetch_reddit(session, subreddit="worldnews"),
#     fetch_reddit(session, subreddit="korea"),
# ]

# 아래처럼 교체:
async def get_feed_extended(force_refresh: bool = False, limit: int = 50) -> dict:
    """홈 피드 — 기존 소스 + JP + CN"""
    import aiohttp
    import cache

    try:
        if not force_refresh:
            cached = await cache.get("feed:home")
            if cached:
                return {**cached, "cached": True}
    except Exception:
        pass

    # 기존 수집기
    connector = aiohttp.TCPConnector(limit=10, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            fetch_youtube(session),
            fetch_news(session),
            fetch_reddit(session, subreddit="worldnews"),
            fetch_reddit(session, subreddit="korea"),
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    posts = []
    for r in results:
        if isinstance(r, list):
            posts.extend(r)

    # ── JP + CN 추가 수집 ──────────────────────
    jp_posts = await JapanCollector().collect(limit=20)
    cn_posts = await ChinaCollector().collect(limit=20)
    posts.extend(jp_posts)
    posts.extend(cn_posts)
    # ───────────────────────────────────────────

    # 중복 제거 (id 기준)
    seen = set()
    unique = []
    for p in posts:
        if p["id"] not in seen:
            seen.add(p["id"])
            unique.append(p)

    unique.sort(key=lambda p: p.get("score", 0), reverse=True)
    unique = unique[:limit]

    # AI 재작성 (상위 10개)
    top_posts = unique[:10]
    rewrites = await asyncio.gather(
        *[rewrite_and_summarize(p["originalTitle"], p["source"]) for p in top_posts],
        return_exceptions=True
    )
    for post, rewrite in zip(top_posts, rewrites):
        if isinstance(rewrite, dict):
            post["rewrittenTitle"] = rewrite.get("rewrittenTitle", post["originalTitle"])
            post["summary"] = rewrite.get("summary", "")
            post["badges"] = rewrite.get("badges", [])

    payload = {"posts": unique, "total": len(unique), "cached": False, "updated_at": now_iso()}

    try:
        await cache.set("feed:home", payload, CACHE_TTL_FEED)
    except Exception:
        pass

    return payload


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 3. region별 필터 엔드포인트 (server.py에 추가)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# server.py에 아래 라우트 추가:
#
# @app.get("/api/region/{region}")
# async def get_region_feed(region: str):
#     """JP 또는 CN 전용 피드"""
#     region = region.upper()
#     if region == "JP":
#         posts = await JapanCollector().collect(limit=30)
#     elif region == "CN":
#         posts = await ChinaCollector().collect(limit=30)
#     else:
#         raise HTTPException(status_code=400, detail="region must be JP or CN")
#     return {"posts": posts, "total": len(posts), "region": region}
