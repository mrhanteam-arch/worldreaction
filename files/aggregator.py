# aggregator.py — 소스 균등 믹싱 + 검색 개선
import asyncio
import logging
import os
import hashlib
from datetime import datetime, timezone, timedelta

log = logging.getLogger("wr.aggregator")

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


# ━━━ 트렌드 키워드 (네이버 없이 자체 생성) ━━━━━━━━
async def get_trend_keywords() -> list:
    """
    네이버 API 없이 트렌드 키워드 생성
    1. Google Trends RSS (무료, 키 불필요)
    2. 실패 시 기본 키워드 사용
    """
    import aiohttp

    # Google Trends 한국 RSS (공식 공개 피드)
    google_trends_url = "https://trends.google.com/trends/trendingsearches/daily/rss?geo=KR"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                google_trends_url,
                headers={"User-Agent": "Mozilla/5.0 WorldReaction/2.0"},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                if r.status == 200:
                    import xml.etree.ElementTree as ET
                    text = await r.text(errors="replace")
                    root = ET.fromstring(text)
                    keywords = []
                    for item in root.findall(".//item")[:10]:
                        title = getattr(item.find("title"), "text", None)
                        if title:
                            keywords.append(title.strip())
                    if keywords:
                        log.info("Google Trends 키워드: %s", keywords[:5])
                        return keywords
    except Exception as e:
        log.warning("Google Trends 실패: %s", e)

    # 폴백: 고정 키워드
    return [
        "korea news", "kpop", "bts", "blackpink", "samsung",
        "south korea politics", "korea economy", "손흥민",
        "korea japan", "korea china",
    ]


# ━━━ RedditCollector ━━━━━━━━━━━━━━━━━━━━━━━
class RedditCollector:
    # 서브레딧 + 키워드 검색 혼합
    SUBREDDITS = [
        "worldnews", "korea", "kpop", "bangtan",
        "technology", "geopolitics", "asia", "southkorea",
    ]

    async def fetch_subreddit(self, session, subreddit: str, limit: int = 12) -> list:
        import aiohttp
        try:
            url     = f"https://www.reddit.com/r/{subreddit}/hot.json?limit={limit}"
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
                if not title or len(title) < 10:
                    continue
                permalink = "https://www.reddit.com" + d.get("permalink", "")
                ts  = d.get("created_utc", 0)
                pub = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts else now_iso()
                if not is_recent(pub, days=7):
                    continue
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
                    "category":       self._infer_category(title, subreddit),
                    "region":         "GLOBAL",
                    "subreddit":      f"r/{subreddit}",
                })
            return posts
        except Exception as e:
            log.error("Reddit [%s]: %s", subreddit, e)
            return []

    async def search_keyword(self, session, keyword: str, limit: int = 8) -> list:
        """키워드 기반 Reddit 검색 (트렌드 키워드 활용)"""
        import aiohttp
        try:
            url     = f"https://www.reddit.com/search.json?q={keyword}&sort=top&t=week&limit={limit}"
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
                if not title or len(title) < 10:
                    continue
                permalink = "https://www.reddit.com" + d.get("permalink", "")
                ts  = d.get("created_utc", 0)
                pub = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts else now_iso()
                if not is_recent(pub, days=7):
                    continue
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
                    "category":       self._infer_category(title, ""),
                    "region":         "GLOBAL",
                    "subreddit":      f"r/{d.get('subreddit', '')}",
                    "keyword":        keyword,
                })
            return posts
        except Exception as e:
            log.error("Reddit search [%s]: %s", keyword, e)
            return []

    def _infer_category(self, title: str, sub: str) -> str:
        sub = sub.lower()
        t   = title.lower()
        if sub in ["kpop", "bangtan"]: return "entertainment"
        if sub in ["technology"]:      return "tech"
        if any(x in t for x in ["economy", "trade", "stock", "tariff", "market", "gdp"]): return "economy"
        if any(x in t for x in ["kpop", "bts", "blackpink", "idol", "concert"]):          return "entertainment"
        if any(x in t for x in ["soccer", "football", "nba", "sports"]):                  return "sports"
        if any(x in t for x in ["tech", "ai", "samsung", "semiconductor"]):               return "tech"
        return "news"

    async def collect(self, keywords: list = None) -> list:
        import aiohttp
        connector = aiohttp.TCPConnector(limit=8, ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            # 서브레딧 수집
            sub_tasks = [self.fetch_subreddit(session, sub, 12) for sub in self.SUBREDDITS]
            # 키워드 검색 (트렌드 키워드 상위 4개)
            kw_tasks = []
            if keywords:
                for kw in keywords[:4]:
                    kw_tasks.append(self.search_keyword(session, kw, 6))

            results = await asyncio.gather(*(sub_tasks + kw_tasks), return_exceptions=True)

        posts = []
        for r in results:
            if isinstance(r, list):
                posts.extend(r)

        seen, unique = set(), []
        for p in posts:
            if p["id"] not in seen:
                seen.add(p["id"])
                unique.append(p)
        unique.sort(key=lambda p: p.get("score", 0), reverse=True)
        log.info("Reddit 수집: %d건", len(unique))
        return unique


# ━━━ YouTubeCollector ━━━━━━━━━━━━━━━━━━━━━━
class YouTubeCollector:
    SKIP_KEYWORDS = [
        "tiktok reaction", "bollywood", "indian tiktok", "samosa",
        "daizy aizy", "simpal kharel", "reels #trending",
        "eating challenge", "food challenge", "hindi dubbed",
    ]
    BASE_QUERIES = [
        "south korea world news this week",
        "kpop reaction 2026",
        "korea international politics 2026",
        "korea economy trade 2026",
        "bts 2026",
    ]

    def __init__(self):
        self.api_key = os.environ.get("YOUTUBE_API_KEY", "")

    def _published_after(self, days: int = 7) -> str:
        dt = datetime.now(timezone.utc) - timedelta(days=days)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    def _is_quality(self, title: str) -> bool:
        return not any(kw in title.lower() for kw in self.SKIP_KEYWORDS)

    def _infer_category(self, title: str) -> str:
        t = title.lower()
        if any(x in t for x in ["news", "politics", "breaking", "election", "war", "tariff"]): return "news"
        if any(x in t for x in ["kpop", "k-pop", "bts", "blackpink", "idol", "concert"]):     return "entertainment"
        if any(x in t for x in ["soccer", "football", "nba", "sports"]):                       return "sports"
        if any(x in t for x in ["tech", "ai", "technology", "samsung", "nvidia"]):             return "tech"
        if any(x in t for x in ["economy", "stock", "market", "trade", "gdp"]):                return "economy"
        return "entertainment"

    async def search_one(self, session, query: str, limit: int = 8) -> list:
        import aiohttp
        if not self.api_key:
            return []
        try:
            url = (
                f"https://www.googleapis.com/youtube/v3/search"
                f"?part=snippet&q={query}&type=video&order=viewCount"
                f"&maxResults={limit}"
                f"&publishedAfter={self._published_after(7)}"
                f"&relevanceLanguage=ko&key={self.api_key}"
            )
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=12)) as r:
                if r.status != 200:
                    return []
                data = await r.json(content_type=None)

            posts = []
            for item in data.get("items", []):
                snippet  = item.get("snippet", {})
                video_id = item.get("id", {}).get("videoId", "")
                if not video_id:
                    continue
                title = snippet.get("title", "").strip()
                if not title or not self._is_quality(title):
                    continue
                video_url = f"https://www.youtube.com/watch?v={video_id}"
                posts.append({
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
                    "category":       self._infer_category(title),
                    "region":         "GLOBAL",
                    "thumbnail":      snippet.get("thumbnails", {}).get("medium", {}).get("url", ""),
                    "channelTitle":   snippet.get("channelTitle", ""),
                })
            return posts
        except Exception as e:
            log.error("YouTube [%s]: %s", query, e)
            return []

    async def collect(self, keywords: list = None) -> list:
        import aiohttp
        # 기본 쿼리 + 트렌드 키워드 상위 3개
        queries = list(self.BASE_QUERIES)
        if keywords:
            for kw in keywords[:3]:
                queries.append(f"{kw} korea reaction")

        connector = aiohttp.TCPConnector(limit=5, ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks   = [self.search_one(session, q, 8) for q in queries]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        posts = []
        for r in results:
            if isinstance(r, list):
                posts.extend(r)

        seen, unique = set(), []
        for p in posts:
            if p["id"] not in seen:
                seen.add(p["id"])
                unique.append(p)
        log.info("YouTube 수집: %d건", len(unique))
        return unique


# ━━━ Aggregator ━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class Aggregator:

    def __init__(self):
        self.reddit  = RedditCollector()
        self.youtube = YouTubeCollector()

    async def _rewrite(self, post: dict) -> dict:
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            return post
        try:
            import anthropic, json, re
            client = anthropic.Anthropic(api_key=api_key)
            resp   = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=300,
                messages=[{"role": "user", "content": f"""다음 해외 게시글 제목을 한국어로 자연스럽게 재작성하고 요약해줘.

제목: {post['originalTitle']}
출처: {post['source']} {post.get('subreddit', '')}

JSON으로만 답해줘:
{{"rewrittenTitle": "재작성된 제목", "summary": "2-3줄 요약", "badges": ["hot|praise|controversy|shock|funny|trend 중 해당하는 것들"]}}"""}]
            )
            text  = resp.content[0].text.strip()
            match = re.search(r'\{.*\}', text, re.DOTALL)
            if match:
                result = json.loads(match.group())
                post["rewrittenTitle"] = result.get("rewrittenTitle", post["originalTitle"])
                post["summary"]        = result.get("summary", "")
                post["badges"]         = result.get("badges", [])
        except Exception as e:
            log.warning("AI rewrite: %s", e)
        return post

    def _interleave(self, reddit: list, youtube: list, jp: list, cn: list, limit: int = 50) -> list:
        """
        매체별 인터리빙 믹싱
        순서: Reddit → YouTube → JP → CN → Reddit → YouTube → ...
        """
        queues = {
            "reddit":  list(reddit),
            "youtube": list(youtube),
            "jp":      list(jp),
            "cn":      list(cn),
        }
        # 각 소스 최대 할당량
        caps = {
            "reddit":  limit // 3,
            "youtube": limit // 4,
            "jp":      limit // 6,
            "cn":      limit // 6,
        }
        counts = {k: 0 for k in queues}

        order  = ["reddit", "youtube", "jp", "cn"]
        mixed  = []
        seen   = set()

        while len(mixed) < limit:
            added = False
            for src in order:
                q = queues[src]
                if q and counts[src] < caps[src]:
                    post = q.pop(0)
                    if post["id"] not in seen:
                        seen.add(post["id"])
                        mixed.append(post)
                        counts[src] += 1
                        added = True
                    if len(mixed) >= limit:
                        break
            if not added:
                break

        return mixed

    async def run(self, limit: int = 50) -> dict:
        # 트렌드 키워드 수집 (Google Trends 또는 폴백)
        keywords = await get_trend_keywords()
        log.info("트렌드 키워드: %s", keywords[:5])

        # Reddit + YouTube 독립 수집 (키워드 전달)
        reddit_task  = self.reddit.collect(keywords=keywords)
        youtube_task = self.youtube.collect(keywords=keywords)

        reddit_posts, youtube_posts = await asyncio.gather(
            reddit_task, youtube_task, return_exceptions=True
        )

        reddit_posts  = reddit_posts  if isinstance(reddit_posts,  list) else []
        youtube_posts = youtube_posts if isinstance(youtube_posts, list) else []

        # JP / CN 수집
        try:
            from japan_collector import JapanCollector
            jp_posts = await JapanCollector().collect(limit=20)
        except Exception as e:
            log.warning("JP error: %s", e)
            jp_posts = []

        try:
            from china_collector import ChinaCollector
            cn_posts = await ChinaCollector().collect(limit=20)
        except Exception as e:
            log.warning("CN error: %s", e)
            cn_posts = []

        log.info("수집 현황 — Reddit: %d, YouTube: %d, JP: %d, CN: %d",
                 len(reddit_posts), len(youtube_posts), len(jp_posts), len(cn_posts))

        # 매체별 인터리빙
        mixed = self._interleave(reddit_posts, youtube_posts, jp_posts, cn_posts, limit=limit)

        # AI 재작성 (상위 15개)
        top = mixed[:15]
        await asyncio.gather(*[self._rewrite(p) for p in top], return_exceptions=True)

        return {
            "posts":      mixed,
            "total":      len(mixed),
            "cached":     False,
            "updated_at": now_iso(),
            "keywords":   keywords[:5],
            "stats": {
                "reddit":  len(reddit_posts),
                "youtube": len(youtube_posts),
                "jp":      len(jp_posts),
                "cn":      len(cn_posts),
            },
        }

    async def search(self, query: str) -> dict:
        """
        검색: Reddit + YouTube 동시에 쿼리
        BTS 검색 시 → bangtan, kpop, worldnews 등 다양한 소스에서 수집
        """
        import aiohttp

        connector = aiohttp.TCPConnector(limit=8, ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [
                # Reddit: 키워드 검색
                self.reddit.search_keyword(session, query, limit=10),
                # Reddit: 관련 서브레딧 직접 수집
                self.reddit.fetch_subreddit(session, "kpop",     8) if any(x in query.lower() for x in ["bts", "kpop", "blackpink", "k-pop"]) else asyncio.sleep(0),
                self.reddit.fetch_subreddit(session, "bangtan",  8) if any(x in query.lower() for x in ["bts", "방탄"])                        else asyncio.sleep(0),
                self.reddit.fetch_subreddit(session, "worldnews",8),
                # YouTube: 쿼리 검색
                self.youtube.search_one(session, query, limit=8),
                self.youtube.search_one(session, f"{query} korea reaction", limit=6),
                self.youtube.search_one(session, f"{query} 2026", limit=6),
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        posts = []
        for r in results:
            if isinstance(r, list):
                posts.extend(r)

        # JP/CN에서도 검색
        try:
            from japan_collector import JapanCollector
            jp = await JapanCollector().collect(limit=10)
            q  = query.lower()
            posts.extend([p for p in jp if q in p.get("originalTitle", "").lower()])
        except Exception:
            pass

        try:
            from china_collector import ChinaCollector
            cn = await ChinaCollector().collect(limit=10)
            q  = query.lower()
            posts.extend([p for p in cn if q in p.get("originalTitle", "").lower()])
        except Exception:
            pass

        # 중복 제거
        seen, unique = set(), []
        for p in posts:
            if p["id"] not in seen:
                seen.add(p["id"])
                unique.append(p)

        # score 기준 정렬
        unique.sort(key=lambda p: p.get("score", 0), reverse=True)

        # AI 재작성 (상위 8개)
        top = unique[:8]
        await asyncio.gather(*[self._rewrite(p) for p in top], return_exceptions=True)

        return {"query": query, "posts": unique, "total": len(unique), "cached": False}
