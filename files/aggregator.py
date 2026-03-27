# aggregator.py — 키워드 기반 데이터 병합 · 중복 제거 · 균등 믹싱
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


# ━━━ RedditCollector ━━━━━━━━━━━━━━━━━━━━━━━
class RedditCollector:
    SUBREDDITS = [
        "worldnews", "korea", "kpop", "bangtan",
        "technology", "geopolitics", "asia", "southkorea",
    ]

    async def fetch_subreddit(self, session, subreddit: str, limit: int = 15) -> list:
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

    def _infer_category(self, title: str, sub: str) -> str:
        sub = sub.lower()
        t   = title.lower()
        if sub in ["kpop", "bangtan"]: return "entertainment"
        if sub in ["technology"]:      return "tech"
        if any(x in t for x in ["economy", "trade", "stock", "tariff", "market"]): return "economy"
        if any(x in t for x in ["war", "military", "election", "politics"]):        return "news"
        return "news"

    async def collect(self, limit_per_sub: int = 12) -> list:
        import aiohttp
        connector = aiohttp.TCPConnector(limit=8, ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks   = [self.fetch_subreddit(session, sub, limit_per_sub) for sub in self.SUBREDDITS]
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
    QUERIES = [
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
        if any(x in t for x in ["soccer", "football", "nba", "sports", "baseball"]):           return "sports"
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

    async def collect(self) -> list:
        import aiohttp
        connector = aiohttp.TCPConnector(limit=5, ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks   = [self.search_one(session, q, 8) for q in self.QUERIES]
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
출처: {post['source']}

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

    def _mix_balanced(self, global_posts: list, jp_posts: list, cn_posts: list, limit: int = 50) -> list:
        us_limit = limit // 2
        jp_limit = limit // 4
        cn_limit = limit - us_limit - jp_limit

        us_q = sorted(global_posts, key=lambda p: p.get("score", 0), reverse=True)[:us_limit]
        jp_q = sorted(jp_posts,     key=lambda p: p.get("publishedAt", ""), reverse=True)[:jp_limit]
        cn_q = sorted(cn_posts,     key=lambda p: p.get("publishedAt", ""), reverse=True)[:cn_limit]

        mixed = []
        while us_q or jp_q or cn_q:
            for _ in range(2):
                if us_q: mixed.append(us_q.pop(0))
            if jp_q: mixed.append(jp_q.pop(0))
            if cn_q: mixed.append(cn_q.pop(0))
        return mixed[:limit]

    async def run(self, limit: int = 50) -> dict:
        # Reddit + YouTube 각자 세션 관리 (독립 수집)
        reddit_task  = self.reddit.collect(limit_per_sub=12)
        youtube_task = self.youtube.collect()

        reddit_posts, youtube_posts = await asyncio.gather(
            reddit_task, youtube_task, return_exceptions=True
        )

        global_posts = []
        if isinstance(reddit_posts,  list): global_posts.extend(reddit_posts)
        if isinstance(youtube_posts, list): global_posts.extend(youtube_posts)

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

        log.info("최종 현황 — 글로벌: %d, JP: %d, CN: %d",
                 len(global_posts), len(jp_posts), len(cn_posts))

        # 중복 제거
        seen, unique_global = set(), []
        for p in global_posts:
            if p["id"] not in seen:
                seen.add(p["id"])
                unique_global.append(p)

        # 균등 믹싱
        mixed = self._mix_balanced(unique_global, jp_posts, cn_posts, limit=limit)

        # AI 재작성 (상위 15개)
        top = mixed[:15]
        await asyncio.gather(*[self._rewrite(p) for p in top], return_exceptions=True)

        return {
            "posts":      mixed,
            "total":      len(mixed),
            "cached":     False,
            "updated_at": now_iso(),
            "stats": {
                "global": len(unique_global),
                "jp":     len(jp_posts),
                "cn":     len(cn_posts),
            },
        }
