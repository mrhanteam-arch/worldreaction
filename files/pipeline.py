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


async def fetch_youtube(session, query: str = None, limit: int = 20) -> list:
    """YouTube Data API v3 — 영상 제목/링크/조회수 수집"""
    api_key = os.environ.get("YOUTUBE_API_KEY", "")
    if not api_key:
        log.warning("YOUTUBE_API_KEY not set")
        return []

    try:
        import aiohttp

        if query:
            search_query = query
        else:
            search_query = "korea world reaction trending"

        url = (
            f"https://www.googleapis.com/youtube/v3/search"
            f"?part=snippet&q={search_query}&type=video"
            f"&order=viewCount&maxResults={limit}"
            f"&relevanceLanguage=ko&key={api_key}"
        )

        headers = {"Accept": "application/json"}
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status != 200:
                text = await r.text()
                log.error("YouTube API error %s: %s", r.status, text)
                return []
            data = await r.json(content_type=None)

        posts = []
        for item in data.get("items", []):
            snippet = item.get("snippet", {})
            video_id = item.get("id", {}).get("videoId", "")
            if not video_id:
                continue

            title = snippet.get("title", "").strip()
            if not title:
                continue

            video_url = f"https://www.youtube.com/watch?v={video_id}"
            pub = snippet.get("publishedAt", now_iso())

            # 카테고리 추론
            title_lower = title.lower()
            cat = "entertainment"
            if any(x in title_lower for x in ["news", "뉴스", "politics", "정치"]): cat = "news"
            elif any(x in title_lower for x in ["kpop", "k-pop", "bts", "blackpink", "aespa"]): cat = "entertainment"
            elif any(x in title_lower for x in ["soccer", "football", "sports", "스포츠", "nba"]): cat = "sports"
            elif any(x in title_lower for x in ["tech", "ai", "technology", "기술"]): cat = "tech"

            posts.append({
                "id": make_id("youtube", video_url),
                "source": "youtube",
                "sourceUrl": video_url,
                "originalTitle": title,
                "rewrittenTitle": title,
                "summary": "",
                "score": 0,
                "commentCount": 0,
                "publishedAt": pub,
                "badges": [],
                "category": cat,
                "thumbnail": snippet.get("thumbnails", {}).get("medium", {}).get("url", ""),
                "channelTitle": snippet.get("channelTitle", ""),
            })
        return posts
    except Exception as e:
        log.error("YouTube fetch error: %s", e)
        return []


async def fetch_news(session, query: str = None, limit: int = 20) -> list:
    """News API — 뉴스 기사 수집"""
    api_key = os.environ.get("NEWS_API_KEY", "")
    if not api_key:
        log.warning("NEWS_API_KEY not set")
        return []

    try:
        import aiohttp

        if query:
            url = (
                f"https://newsapi.org/v2/everything"
                f"?q={query}&sortBy=popularity&pageSize={limit}"
                f"&language=en&apiKey={api_key}"
            )
        else:
            url = (
                f"https://newsapi.org/v2/top-headlines"
                f"?categories=general&pageSize={limit}"
                f"&apiKey={api_key}"
            )

        headers = {"Accept": "application/json"}
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status != 200:
                text = await r.text()
                log.error("News API error %s: %s", r.status, text)
                return []
            data = await r.json(content_type=None)

        posts = []
        for article in data.get("articles", []):
            title = article.get("title", "").strip()
            source_url = article.get("url", "")
            if not title or not source_url or title == "[Removed]":
                continue

            pub = article.get("publishedAt", now_iso())

            posts.append({
                "id": make_id("news", source_url),
                "source": "news",
                "sourceUrl": source_url,
                "originalTitle": title,
                "rewrittenTitle": title,
                "summary": article.get("description", ""),
                "score": 0,
                "commentCount": 0,
                "publishedAt": pub,
                "badges": [],
                "category": "news",
                "thumbnail": article.get("urlToImage", ""),
                "channelTitle": article.get("source", {}).get("name", ""),
            })
        return posts
    except Exception as e:
        log.error("News fetch error: %s", e)
        return []


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
            ts = d.get("created_utc", 0)
            pub = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts else now_iso()

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
                "rewrittenTitle": title,
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
    """홈 피드 — YouTube + News + Reddit"""
    try:
        import cache
        if not force_refresh:
            cached = await cache.get("feed:home")
            if cached:
                return {**cached, "cached": True}
    except Exception:
        pass

    import aiohttp
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

    # score 기준 정렬 후 섞기
    posts.sort(key=lambda p: p.get("score", 0), reverse=True)
    posts = posts[:limit]

    # AI 재작성 (상위 10개만)
    top_posts = posts[:10]
    rewrite_tasks = [rewrite_and_summarize(p["originalTitle"], p["source"]) for p in top_posts]
    rewrites = await asyncio.gather(*rewrite_tasks, return_exceptions=True)

    for post, rewrite in zip(top_posts, rewrites):
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
    """검색 — YouTube + News + Reddit"""
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
        tasks = [
            fetch_youtube(session, query=query, limit=15),
            fetch_news(session, query=query, limit=15),
            fetch_reddit(session, query=query, limit=15),
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    posts = []
    for r in results:
        if isinstance(r, list):
            posts.extend(r)

    # AI 재작성 (상위 8개)
    top = posts[:8]
    rewrites = await asyncio.gather(
        *[rewrite_and_summarize(p["originalTitle"], p["source"]) for p in top],
        return_exceptions=True
    )
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
