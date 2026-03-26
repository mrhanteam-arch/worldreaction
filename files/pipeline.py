# pipeline.py — 데이터 파이프라인 (공식 API만 사용)
import asyncio
import logging
import os
import hashlib
from datetime import datetime, timezone

log = logging.getLogger("wr.pipeline")

CACHE_TTL_FEED = 600    # 10분
CACHE_TTL_SEARCH = 3600 # 1시간

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def make_id(source, url):
    return hashlib.md5(f"{source}:{url}".encode()).hexdigest()[:16]

async def rewrite_and_summarize(title: str, source: str) -> dict:
    """AI로 제목 재작성 + 요약 생성"""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return {"rewrittenTitle": title, "summary": "", "badges": []}

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            messages=[{
                "role": "user",
                "content": f"""다음 해외 게시글 제목을 한국어로 자연스럽게 재작성하고 요약해줘.
원문을 그대로 번역하지 말고 한국 독자가 관심 가질 만한 방식으로 재작성해줘.

제목: {title}
출처: {source}

JSON으로만 답해줘:
{{"rewrittenTitle": "재작성된 제목", "summary": "2-3줄 요약", "badges": ["hot|praise|controversy|shock|funny|trend 중 해당하는 것들"]}}"""
            }]
        )
        import json, re
        text = resp.content[0].text.strip()
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            return json.loads(match.group())
    except Exception as e:
        log.warning("AI rewrite error: %s", e)

    return {"rewrittenTitle": title, "summary": "", "badges": []}


async def fetch_reddit(session, query: str = None, subreddit: str = None, limit: int = 20) -> list:
    """Reddit API — 제목/점수/댓글수/링크만 수집"""
    import aiohttp
    try:
        if query:
            url = f"https://www.reddit.com/search.json?q={query}&sort=top&t=week&limit={limit}"
        elif subreddit:
            url = f"https://www.reddit.com/r/{subreddit}/hot.json?limit={limit}"
        else:
            url = f"https://www.reddit.com/r/worldnews+korea+technology/hot.json?limit={limit}"

        headers = {"User-Agent": "WorldReaction/2.0", "Accept": "application/json"}
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status != 200:
                return []
            data = await r.json(content_type=None)

        posts = []
        for child in data.get("data", {}).get("children", []):
            d = child.get("data", {})
            if d.get("stickied") or d.get("over_18"):
                continue
            title = d.get("title", "").strip()
            if not title:
                continue
            permalink = "https://www.reddit.com" + d.get("permalink", "")
            from datetime import datetime, timezone
            ts = d.get("created_utc", 0)
            pub = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts else now_iso()

            # 카테고리 추론
            sub = d.get("subreddit", "").lower()
            cat = "news"
            if any(x in sub for x in ["kpop", "bangtan", "entertainment"]): cat = "entertainment"
            elif any(x in sub for x in ["soccer", "sports", "nba", "football"]): cat = "sports"
            elif any(x in sub for x in ["tech", "technology", "ai", "programming"]): cat = "tech"
            elif any(x in sub for x in ["politics", "worldnews", "news"]): cat = "news"

            posts.append({
                "id": make_id("reddit", permalink),
                "source": "reddit",
                "sourceUrl": permalink,
                "originalTitle": title,
                "rewrittenTitle": title,  # AI 재작성 전
                "summary": "",
                "score": d.get("score", 0),
                "commentCount": d.get("num_comments", 0),
                "publishedAt": pub,
                "badges": [],
                "category": cat,
                "subreddit": f"r/{d.get('subreddit', '')}",
            })
        return posts
    except Exception as e:
        log.error("Reddit fetch error: %s", e)
        return []


async def get_feed(force_refresh: bool = False, limit: int = 30) -> dict:
    """홈 피드 — Reddit 인기 게시글"""
    try:
        import cache
        if not force_refresh:
            cached = await cache.get("feed:home")
            if cached:
                return {**cached, "cached": True}
    except Exception:
        pass

    import aiohttp
    connector = aiohttp.TCPConnector(limit=5, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        # 주요 서브레딧에서 인기글 수집
        tasks = [
            fetch_reddit(session, subreddit="worldnews"),
            fetch_reddit(session, subreddit="korea"),
            fetch_reddit(session, subreddit="technology"),
            fetch_reddit(session, subreddit="kpop"),
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    posts = []
    for r in results:
        if isinstance(r, list):
            posts.extend(r)

    # score 기준 정렬
    posts.sort(key=lambda p: p.get("score", 0), reverse=True)
    posts = posts[:limit]

    # AI 재작성 (상위 10개만)
    top_posts = posts[:10]
    rewrite_tasks = [rewrite_and_summarize(p["originalTitle"], p["source"]) for p in top_posts]
    rewrites = await asyncio.gather(*rewrite_tasks, return_exceptions=True)

    for i, (post, rewrite) in enumerate(zip(top_posts, rewrites)):
        if isinstance(rewrite, dict):
            post["rewrittenTitle"] = rewrite.get("rewrittenTitle", post["originalTitle"])
            post["summary"] = rewrite.get("summary", "")
            post["badges"] = rewrite.get("badges", [])

    payload = {"posts": posts, "total": len(posts), "cached": False, "updated_at": now_iso()}

    try:
        import cache
        await cache.set("feed:home", payload, CACHE_TTL_FEED)
    except Exception:
        pass

    return payload


async def search(query: str, force_refresh: bool = False) -> dict:
    """검색 — Reddit API"""
    try:
        import cache
        if not force_refresh:
            cached = await cache.get(f"search:{query}")
            if cached:
                return {**cached, "cached": True}
    except Exception:
        pass

    import aiohttp
    async with aiohttp.ClientSession() as session:
        posts = await fetch_reddit(session, query=query, limit=25)

    # AI 재작성 (상위 8개)
    top = posts[:8]
    rewrites = await asyncio.gather(*[rewrite_and_summarize(p["originalTitle"], p["source"]) for p in top], return_exceptions=True)
    for post, rewrite in zip(top, rewrites):
        if isinstance(rewrite, dict):
            post["rewrittenTitle"] = rewrite.get("rewrittenTitle", post["originalTitle"])
            post["summary"] = rewrite.get("summary", "")
            post["badges"] = rewrite.get("badges", [])

    payload = {"query": query, "posts": posts, "total": len(posts), "cached": False}

    try:
        import cache
        await cache.set(f"search:{query}", payload, CACHE_TTL_SEARCH)
    except Exception:
        pass

    return payload
