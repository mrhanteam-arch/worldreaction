# pipeline.py — 데이터 파이프라인 (공식 API만 사용)
import asyncio
import logging
import os
import hashlib
from datetime import datetime, timezone, timedelta

log = logging.getLogger("wr.pipeline")

CACHE_TTL_FEED   = 600    # 10분
CACHE_TTL_SEARCH = 3600   # 1시간

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def make_id(source, url):
    return hashlib.md5(f"{source}:{url}".encode()).hexdigest()[:16]

def is_recent(published_at: str, days: int = 7) -> bool:
    """최근 N일 이내 게시글만 허용"""
    try:
        pub = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - pub).days <= days
    except Exception:
        return True

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


# ━━━ YouTube ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 실제 화제성 있는 글로벌 이슈 검색어
YOUTUBE_QUERIES = [
    "korea trending news 2026",
    "kpop reaction 2026",
    "korea international news today",
    "south korea world reaction",
    "bts new 2026",
]

# 퍼블리시 날짜 RFC3339 포맷 (7일 이내)
def youtube_published_after() -> str:
    dt = datetime.now(timezone.utc) - timedelta(days=7)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

async def fetch_youtube(session, query: str = None, limit: int = 10) -> list:
    """YouTube Data API v3 — 최근 7일 이내 영상만"""
    api_key = os.environ.get("YOUTUBE_API_KEY", "")
    if not api_key:
        log.warning("YOUTUBE_API_KEY not set")
        return []
    try:
        import aiohttp
        search_query = query or "korea world trending news"
        url = (
            f"https://www.googleapis.com/youtube/v3/search"
            f"?part=snippet"
            f"&q={search_query}"
            f"&type=video"
            f"&order=viewCount"
            f"&maxResults={limit}"
            f"&publishedAfter={youtube_published_after()}"   # 최근 7일
            f"&relevanceLanguage=ko"
            f"&key={api_key}"
        )
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status != 200:
                log.error("YouTube API error %s: %s", r.status, await r.text())
                return []
            data = await r.json(content_type=None)

        posts = []
        for item in data.get("items", []):
            snippet  = item.get("snippet", {})
            video_id = item.get("id", {}).get("videoId", "")
            if not video_id:
                continue
            title = snippet.get("title", "").strip()
            if not title:
                continue

            # 저품질 필터 (shorts 태그, 인도 콘텐츠 위주 키워드 제외)
            skip_keywords = ["#shorts", "tiktok reaction", "bollywood", "indian tiktok",
                             "samosa", "daizy", "simpal kharel", "reels #trending"]
            if any(kw.lower() in title.lower() for kw in skip_keywords):
                continue

            video_url = f"https://www.youtube.com/watch?v={video_id}"
            pub       = snippet.get("publishedAt", now_iso())

            title_lower = title.lower()
            cat = "entertainment"
            if any(x in title_lower for x in ["news", "뉴스", "politics", "정치", "breaking"]): cat = "news"
            elif any(x in title_lower for x in ["kpop", "k-pop", "bts", "blackpink", "aespa", "idol"]): cat = "entertainment"
            elif any(x in title_lower for x in ["soccer", "football", "sports", "스포츠", "nba", "son"]): cat = "sports"
            elif any(x in title_lower for x in ["tech", "ai", "technology", "기술", "samsung", "lg"]): cat = "tech"
            elif any(x in title_lower for x in ["economy", "경제", "stock", "market", "trade"]): cat = "economy"

            posts.append({
                "id":             make_id("youtube", video_url),
                "source":         "youtube",
                "sourceUrl":      video_url,
                "originalTitle":  title,
                "rewrittenTitle": title,
                "summary":        "",
                "score":          0,
                "commentCount":   0,
                "publishedAt":    pub,
                "badges":         [],
                "category":       cat,
                "region":         "GLOBAL",
                "thumbnail":      snippet.get("thumbnails", {}).get("medium", {}).get("url", ""),
                "channelTitle":   snippet.get("channelTitle", ""),
            })
        return posts
    except Exception as e:
        log.error("YouTube fetch error: %s", e)
        return []


# ━━━ News API ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async def fetch_news(session, query: str = None, limit: int = 20) -> list:
    """News API — 최근 7일 이내 기사"""
    api_key = os.environ.get("NEWS_API_KEY", "")
    if not api_key:
        log.warning("NEWS_API_KEY not set")
        return []
    try:
        import aiohttp
        from_date = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")

        if query:
            url = (
                f"https://newsapi.org/v2/everything"
                f"?q={query}&sortBy=popularity&pageSize={limit}"
                f"&language=en&from={from_date}&apiKey={api_key}"
            )
        else:
            url = (
                f"https://newsapi.org/v2/everything"
                f"?q=korea OR kpop OR samsung OR BTS&sortBy=popularity"
                f"&pageSize={limit}&language=en&from={from_date}&apiKey={api_key}"
            )

        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status != 200:
                log.error("News API error %s: %s", r.status, await r.text())
                return []
            data = await r.json(content_type=None)

        posts = []
        for article in data.get("articles", []):
            title      = article.get("title", "").strip()
            source_url = article.get("url", "")
            if not title or not source_url or title == "[Removed]":
                continue
            pub = article.get("publishedAt", now_iso())
            posts.append({
                "id":             make_id("news", source_url),
                "source":         "news",
                "sourceUrl":      source_url,
                "originalTitle":  title,
                "rewrittenTitle": title,
                "summary":        article.get("description", "") or "",
                "score":          0,
                "commentCount":   0,
                "publishedAt":    pub,
                "badges":         [],
                "category":       "news",
                "region":         "GLOBAL",
                "thumbnail":      article.get("urlToImage", "") or "",
                "channelTitle":   article.get("source", {}).get("name", ""),
            })
        return posts
    except Exception as e:
        log.error("News fetch error: %s", e)
        return []


# ━━━ Reddit (선택적) ━━━━━━━━━━━━━━━━━━━━━━━━
async def fetch_reddit(session, query: str = None, subreddit: str = None, limit: int = 20) -> list:
    try:
        import aiohttp
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
            ts  = d.get("created_utc", 0)
            pub = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts else now_iso()

            # 최근 7일 필터
            if not is_recent(pub, days=7):
                continue

            sub = d.get("subreddit", "").lower()
            cat = "news"
            if any(x in sub for x in ["kpop", "bangtan", "entertainment"]): cat = "entertainment"
            elif any(x in sub for x in ["soccer", "sports", "nba", "football"]): cat = "sports"
            elif any(x in sub for x in ["tech", "technology", "ai", "programming"]): cat = "tech"

            posts.append({
                "id":             make_id("reddit", permalink),
                "source":         "reddit",
                "sourceUrl":      permalink,
                "originalTitle":  title,
                "rewrittenTitle": title,
                "summary":        "",
                "score":          d.get("score", 0),
                "commentCount":   d.get("num_comments", 0),
                "publishedAt":    pub,
                "badges":         [],
                "category":       cat,
                "region":         "GLOBAL",
                "subreddit":      f"r/{d.get('subreddit', '')}",
            })
        return posts
    except Exception as e:
        log.error("Reddit fetch error: %s", e)
        return []


# ━━━ 홈 피드 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async def get_feed(force_refresh: bool = False, limit: int = 50) -> dict:
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
        # YouTube 다중 쿼리 + News + Reddit
        tasks = [
            fetch_youtube(session, query=q, limit=8) for q in YOUTUBE_QUERIES
        ] + [
            fetch_news(session, limit=20),
            fetch_reddit(session, subreddit="worldnews"),
            fetch_reddit(session, subreddit="korea"),
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # JP / CN 수집
    try:
        from japan_collector import JapanCollector
        jp_posts = await JapanCollector().collect(limit=20)
    except Exception as e:
        log.warning("JP collect error: %s", e)
        jp_posts = []

    try:
        from china_collector import ChinaCollector
        cn_posts = await ChinaCollector().collect(limit=20)
    except Exception as e:
        log.warning("CN collect error: %s", e)
        cn_posts = []

    posts = []
    for r in results:
        if isinstance(r, list):
            posts.extend(r)
    posts.extend(jp_posts)
    posts.extend(cn_posts)

    # 중복 제거
    seen = set()
    unique = []
    for p in posts:
        if p["id"] not in seen:
            seen.add(p["id"])
            unique.append(p)

    # score 기준 정렬 (RSS는 publishedAt 기준)
    unique.sort(key=lambda p: (p.get("score", 0), p.get("publishedAt", "")), reverse=True)
    unique = unique[:limit]

    # AI 재작성 (상위 10개)
    top = unique[:10]
    rewrites = await asyncio.gather(
        *[rewrite_and_summarize(p["originalTitle"], p["source"]) for p in top],
        return_exceptions=True
    )
    for post, rewrite in zip(top, rewrites):
        if isinstance(rewrite, dict):
            post["rewrittenTitle"] = rewrite.get("rewrittenTitle", post["originalTitle"])
            post["summary"]        = rewrite.get("summary", "")
            post["badges"]         = rewrite.get("badges", [])

    payload = {"posts": unique, "total": len(unique), "cached": False, "updated_at": now_iso()}

    try:
        import cache
        await cache.set("feed:home", payload, CACHE_TTL_FEED)
    except Exception:
        pass

    return payload


# ━━━ 검색 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async def search(query: str, force_refresh: bool = False) -> dict:
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
            fetch_youtube(session, query=query, limit=10),
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
            post["summary"]        = rewrite.get("summary", "")
            post["badges"]         = rewrite.get("badges", [])

    payload = {"query": query, "posts": posts, "total": len(posts), "cached": False}

    try:
        import cache
        await cache.set(f"search:{query}", payload, CACHE_TTL_SEARCH)
    except Exception:
        pass

    return payload
