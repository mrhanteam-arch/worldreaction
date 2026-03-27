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
    """
    관련성 높은 서브레딧에서만 수집
    키워드 검색 대신 서브레딧 직접 타게팅
    """

    # 한국/글로벌 이슈 관련 서브레딧 목록
    SUBREDDITS = [
        "worldnews", "korea", "kpop", "bangtan",
        "technology", "geopolitics", "asia",
        "southkorea", "kdrama",
    ]

    # 키워드 → 관련 서브레딧 매핑
    KEYWORD_SUBREDDIT_MAP = {
        "kpop":    ["kpop", "bangtan", "kdrama"],
        "samsung": ["technology", "android", "gadgets"],
        "bts":     ["bangtan", "kpop"],
        "korea":   ["korea", "worldnews", "southkorea", "asia"],
        "손흥민":   ["soccer", "reds", "worldnews"],
        "트럼프":   ["worldnews", "geopolitics"],
        "ai":      ["technology", "artificial", "MachineLearning"],
    }

    async def fetch_subreddit(self, session, subreddit: str, limit: int = 15) -> list:
        """서브레딧 hot 게시글 수집"""
        import aiohttp
        try:
            url = f"https://www.reddit.com/r/{subreddit}/hot.json?limit={limit}"
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
            log.error("Reddit subreddit error [%s]: %s", subreddit, e)
            return []

    def _infer_category(self, title: str, sub: str) -> str:
        t   = title.lower()
        sub = sub.lower()
        if any(x in sub for x in ["kpop", "bangtan", "kdrama"]): return "entertainment"
        if any(x in sub for x in ["soccer", "sports", "nba", "baseball"]): return "sports"
        if any(x in sub for x in ["technology", "android", "gadgets", "machinelearning"]): return "tech"
        if any(x in t for x in ["economy", "finance", "market", "trade", "stock", "tariff"]): return "economy"
        if any(x in t for x in ["war", "military", "election", "government", "politics"]): return "news"
        return "news"

    async def collect(self, session, limit_per_sub: int = 10) -> list:
        tasks   = [self.fetch_subreddit(session, sub, limit_per_sub) for sub in self.SUBREDDITS]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        posts = []
        for r in results:
            if isinstance(r, list):
                posts.extend(r)

        # 중복 제거 후 score 정렬
        seen, unique = set(), []
        for p in posts:
            if p["id"] not in seen:
                seen.add(p["id"])
                unique.append(p)
        unique.sort(key=lambda p: p.get("score", 0), reverse=True)
        return unique


# ━━━ YouTubeCollector ━━━━━━━━━━━━━━━━━━━━━━
class YouTubeCollector:
    """키워드 기반 YouTube 검색 — 최근 7일 · 저품질 필터"""

    SKIP_KEYWORDS = [
        "tiktok reaction", "bollywood", "indian tiktok", "samosa",
        "daizy aizy", "simpal kharel", "reels #trending",
        "eating challenge", "food challenge", "biryani", "hindi dubbed",
        "shorts #viral", "shorts #trending",
    ]

    # 한국/글로벌 이슈 검색 쿼리
    QUERIES = [
        "south korea world news this week",
        "kpop reaction 2026",
        "korea international politics 2026",
        "korea economy trade 2026",
        "bts 2026 new",
    ]

    def __init__(self):
        self.api_key = os.environ.get("YOUTUBE_API_KEY", "")

    def _published_after(self, days: int = 7) -> str:
        dt = datetime.now(timezone.utc) - timedelta(days=days)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    def _is_quality(self, title: str) -> bool:
        t = title.lower()
        return not any(kw in t for kw in self.SKIP_KEYWORDS)

    def _infer_category(self, title: str) -> str:
        t = title.lower()
        if any(x in t for x in ["news", "politics", "breaking", "election", "war", "tariff"]): return "news"
        if any(x in t for x in ["kpop", "k-pop", "bts", "blackpink", "idol", "concert", "album"]): return "entertainment"
        if any(x in t for x in ["soccer", "football", "nba", "sports", "baseball", "olympic"]): return "sports"
        if any(x in t for x in ["tech", "ai", "technology", "samsung", "apple", "nvidia", "반도체"]): return "tech"
        if any(x in t for x in ["economy", "stock", "market", "trade", "gdp", "inflation"]): return "economy"
        return "entertainment"

    async def search(self, session, query: str, limit: int = 8) -> list:
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
            log.error("YouTube search error [%s]: %s", query, e)
            return []

    async def collect(self, session) -> list:
        tasks   = [self.search(session, q, limit=8) for q in self.QUERIES]
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
한국 독자가 관심 가질 만한 방식으로 재작성해줘.

제목: {post['originalTitle']}
출처: {post['source']} ({post.get('subreddit', '')})

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
            log.warning("AI rewrite error: %s", e)
        return post

    def _mix_balanced(self, global_posts: list, jp_posts: list, cn_posts: list, limit: int = 50) -> list:
        """글로벌 50% / JP 25% / CN 25% 인터리빙"""
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
            if jp_q:  mixed.append(jp_q.pop(0))
            if cn_q:  mixed.append(cn_q.pop(0))
        return mixed[:limit]

    async def run(self, limit: int = 50) -> dict:
        import aiohttp

        # Naver 키워드 추출 시도
        keywords = []
        try:
            from naver_collector import NaverCollector
            connector = aiohttp.TCPConnector(limit=10, ssl=False)
            async with aiohttp.ClientSession(connector=connector) as session:
                keywords = await NaverCollector().extract_keywords(session, top_n=8)
        except Exception as e:
            log.warning("Naver 스킵: %s", e)
            keywords = ["korea", "kpop", "bts", "samsung"]

        log.info("사용 키워드: %s", keywords)

        # Reddit + YouTube 수집
        connector = aiohttp.TCPConnector(limit=10, ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            reddit_task  = self.reddit.collect(session, limit_per_sub=12)
            youtube_task = self.youtube.collect(session)
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

        log.info("수집 현황 — 글로벌: %d, JP: %d, CN: %d",
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
            "keywords":   keywords,
            "stats": {
                "global": len(unique_global),
                "jp":     len(jp_posts),
                "cn":     len(cn_posts),
            },
        }
