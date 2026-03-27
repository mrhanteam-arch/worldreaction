# pipeline.py — 미국/일본/중국 균등 피드
import asyncio
import logging
import os
import hashlib
from datetime import datetime, timezone, timedelta

log = logging.getLogger("wr.pipeline")

CACHE_TTL_FEED   = 600
CACHE_TTL_SEARCH = 3600

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def make_id(source, url):
    return hashlib.md5(f"{source}:{url}".encode()).hexdigest()[:16]

def is_recent(published_at: str, days: int = 7) -> bool:
    try:
        pub = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - pub).days <= days
    except Exception:
        return True

def youtube_published_after(days: int = 7) -> str:
    dt = datetime.now(timezone.utc) - timedelta(days=days)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

async def rewrite_and_summarize(title: str, source: str) -> dict:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return {"rewrittenTitle": title, "summary": "", "badges": []}
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            messages=[{"role": "user", "content": f"""다음 해외 게시글 제목을 한국어로 자연스럽게 재작성하고 요약해줘.
원문을 그대로 번역하지 말고 한국 독자가 관심 가질 만한 방식으로 재작성해줘.

제목: {title}
출처: {source}

JSON으로만 답해줘:
{{"rewrittenTitle": "재작성된 제목", "summary": "2-3줄 요약", "badges": ["hot|praise|controversy|shock|funny|trend 중 해당하는 것들"]}}"""}]
        )
        import json, re
        text = resp.content[0].text.strip()
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            return json.loads(match.group())
    except Exception as e:
        log.warning("AI rewrite error: %s", e)
    return {"rewrittenTitle": title, "summary": "", "badges": []}


# ━━━ 저품질 필터 ━━━━━━━━━━━━━━━━━━━━━━━━━━━
SKIP_KEYWORDS = [
    "tiktok reaction", "bollywood", "indian tiktok", "samosa",
    "daizy aizy", "simpal kharel", "#shorts", "reels #trending",
    "eating challenge", "food challenge", "biryani", "hindi dubbed",
]

def is_quality(title: str) -> bool:
    t = title.lower()
    return not any(kw in t for kw in SKIP_KEYWORDS)

def infer_category(title: str) -> str:
    t = title.lower()
    if any(x in t for x in ["news", "politics", "breaking", "정치", "뉴스", "election", "war", "conflict"]): return "news"
    if any(x in t for x in ["kpop", "k-pop", "bts", "blackpink", "aespa", "idol", "concert", "album"]): return "entertainment"
    if any(x in t for x in ["soccer", "football", "nba", "sports", "son", "baseball", "olympic"]): return "sports"
    if any(x in t for x in ["tech", "ai", "technology", "samsung", "apple", "nvidia", "반도체"]): return "tech"
    if any(x in t for x in ["economy", "stock", "market", "trade", "tariff", "gdp", "inflation"]): return "economy"
    return "entertainment"


# ━━━ 미국: YouTube ━━━━━━━━━━━━━━━━━━━━━━━━━
US_YOUTUBE_QUERIES = [
    "korea trending world reaction this week",
    "south korea international news today",
    "kpop world reaction 2026",
    "korea politics economy news",
    "korea US relations news",
]

async def fetch_youtube_us(session, limit_per_query: int = 6) -> list:
    api_key = os.environ.get("YOUTUBE_API_KEY", "")
    if not api_key:
        return []

    import aiohttp
    all_posts = []

    for query in US_YOUTUBE_QUERIES:
        try:
            url = (
                f"https://www.googleapis.com/youtube/v3/search"
                f"?part=snippet&q={query}&type=video&order=viewCount"
                f"&maxResults={limit_per_query}"
                f"&publishedAfter={youtube_published_after(7)}"
                f"&relevanceLanguage=ko&key={api_key}"
            )
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=12)) as r:
                if r.status != 200:
                    continue
                data = await r.json(content_type=None)

            for item in data.get("items", []):
                snippet  = item.get("snippet", {})
                video_id = item.get("id", {}).get("videoId", "")
                if not video_id:
                    continue
                title = snippet.get("title", "").strip()
                if not title or not is_quality(title):
                    continue

                video_url = f"https://www.youtube.com/watch?v={video_id}"
                all_posts.append({
                    "id":             make_id("youtube", video_url),
                    "source":         "youtube",
                    "sourceUrl":      video_url,
                    "originalTitle":  title,
                    "rewrittenTitle": title,
                    "summary":        "",
                    "score":          0,
                    "commentCount":   0,
                    "publishedAt":    snippet.get("publishedAt", now_iso()),
                    "badges":         [],
                    "category":       infer_category(title),
                    "region":         "GLOBAL",
                    "thumbnail":      snippet.get("thumbnails", {}).get("medium", {}).get("url", ""),
                    "channelTitle":   snippet.get("channelTitle", ""),
                })
        except Exception as e:
            log.error("YouTube query error [%s]: %s", query, e)

    # 중복 제거
    seen = set()
    unique = []
    for p in all_posts:
        if p["id"] not in seen:
            seen.add(p["id"])
            unique.append(p)
    return unique


# ━━━ 미국: News API ━━━━━━━━━━━━━━━━━━━━━━━━
async def fetch_news_us(session, query: str = None, limit: int = 15) -> list:
    api_key = os.environ.get("NEWS_API_KEY", "")
    if not api_key:
        return []
    try:
        import aiohttp
        from_date = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
        q = query or "korea OR kpop OR samsung OR BTS OR \"south korea\""
        url = (
            f"https://newsapi.org/v2/everything"
            f"?q={q}&sortBy=popularity&pageSize={limit}"
            f"&language=en&from={from_date}&apiKey={api_key}"
        )
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=12)) as r:
            if r.status != 200:
                return []
            data = await r.json(content_type=None)

        posts = []
        for article in data.get("articles", []):
            title      = article.get("title", "").strip()
            source_url = article.get("url", "")
            if not title or not source_url or title == "[Removed]":
                continue
            posts.append({
                "id":             make_id("news", source_url),
                "source":         "news",
                "sourceUrl":      source_url,
                "originalTitle":  title,
                "rewrittenTitle": title,
                "summary":        article.get("description", "") or "",
                "score":          0,
                "commentCount":   0,
                "publishedAt":    article.get("publishedAt", now_iso()),
                "badges":         [],
                "category":       infer_category(title),
                "region":         "GLOBAL",
                "thumbnail":      article.get("urlToImage", "") or "",
                "channelTitle":   article.get("source", {}).get("name", ""),
            })
        return posts
    except Exception as e:
        log.error("News fetch error: %s", e)
        return []


# ━━━ Reddit ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
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
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=12)) as r:
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
            if not is_recent(pub, days=7):
                continue

            sub = d.get("subreddit", "").lower()
            cat = infer_category(title)
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
                "subreddit":      f"r/{sub}",
            })
        return posts
    except Exception as e:
        log.error("Reddit fetch error: %s", e)
        return []


# ━━━ 균등 믹싱 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def mix_balanced(us_posts: list, jp_posts: list, cn_posts: list, limit: int = 50) -> list:
    """
    미국/일본/중국 포스트를 균등하게 섞기
    비율: 미국 50% / 일본 25% / 중국 25%
    """
    us_limit = limit // 2           # 25개
    jp_limit = limit // 4           # 12개
    cn_limit = limit - us_limit - jp_limit  # 13개

    # 각 region 최신순 정렬
    us_sorted = sorted(us_posts, key=lambda p: p.get("score", 0) + (1 if is_recent(p.get("publishedAt",""), 1) else 0), reverse=True)
    jp_sorted = sorted(jp_posts, key=lambda p: p.get("publishedAt", ""), reverse=True)
    cn_sorted = sorted(cn_posts, key=lambda p: p.get("publishedAt", ""), reverse=True)

    selected_us = us_sorted[:us_limit]
    selected_jp = jp_sorted[:jp_limit]
    selected_cn = cn_sorted[:cn_limit]

    # 인터리빙: US 2개 → JP 1개 → CN 1개 순서로 섞기
    mixed = []
    us_q = list(selected_us)
    jp_q = list(selected_jp)
    cn_q = list(selected_cn)

    while us_q or jp_q or cn_q:
        for _ in range(2):
            if us_q: mixed.append(us_q.pop(0))
        if jp_q: mixed.append(jp_q.pop(0))
        if cn_q: mixed.append(cn_q.pop(0))

    return mixed[:limit]


# ━━━ 홈 피드 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
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
        # 미국 소스 수집
        us_tasks = [
            fetch_youtube_us(session),
            fetch_news_us(session),
            fetch_reddit(session, subreddit="worldnews"),
            fetch_reddit(session, subreddit="korea"),
        ]
        us_results = await asyncio.gather(*us_tasks, return_exceptions=True)

    us_posts = []
    for r in us_results:
        if isinstance(r, list):
            us_posts.extend(r)

    # 일본 수집
    try:
        from japan_collector import JapanCollector
        jp_posts = await JapanCollector().collect(limit=20)
    except Exception as e:
        log.warning("JP collect error: %s", e)
        jp_posts = []

    # 중국 수집
    try:
        from china_collector import ChinaCollector
        cn_posts = await ChinaCollector().collect(limit=20)
    except Exception as e:
        log.warning("CN collect error: %s", e)
        cn_posts = []

    log.info("수집 현황 — 미국: %d, 일본: %d, 중국: %d", len(us_posts), len(jp_posts), len(cn_posts))

    # 균등 믹싱
    mixed = mix_balanced(us_posts, jp_posts, cn_posts, limit=limit)

    # 중복 제거
    seen = set()
    unique = []
    for p in mixed:
        if p["id"] not in seen:
            seen.add(p["id"])
            unique.append(p)

    # AI 재작성 (상위 15개)
    top = unique[:15]
    rewrites = await asyncio.gather(
        *[rewrite_and_summarize(p["originalTitle"], p["source"]) for p in top],
        return_exceptions=True
    )
    for post, rewrite in zip(top, rewrites):
        if isinstance(rewrite, dict):
            post["rewrittenTitle"] = rewrite.get("rewrittenTitle", post["originalTitle"])
            post["summary"]        = rewrite.get("summary", "")
            post["badges"]         = rewrite.get("badges", [])

    payload = {
        "posts":      unique,
        "total":      len(unique),
        "cached":     False,
        "updated_at": now_iso(),
        "stats": {
            "us": len(us_posts),
            "jp": len(jp_posts),
            "cn": len(cn_posts),
        }
    }

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
        results = await asyncio.gather(
            fetch_youtube_us(session),
            fetch_news_us(session, query=query),
            fetch_reddit(session, query=query),
            return_exceptions=True
        )

    posts = []
    for r in results:
        if isinstance(r, list):
            posts.extend(r)

    # 검색어 필터
    q = query.lower()
    posts = [p for p in posts if q in p.get("originalTitle", "").lower() or q in p.get("summary", "").lower()] or posts

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
