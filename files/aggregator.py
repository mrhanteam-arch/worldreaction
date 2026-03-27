# aggregator.py — 키워드 기반 데이터 병합 · 중복 제거 · 균등 믹싱
import asyncio
import logging
import os
import hashlib
from datetime import datetime, timezone, timedelta
from collections import defaultdict

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
    """키워드 기반 Reddit 검색"""

    async def search(self, session, keyword: str, limit: int = 10) -> list:
        import aiohttp
        try:
            url = f"https://www.reddit.com/search.json?q={keyword}&sort=top&t=week&limit={limit}"
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
                    "category":       self._infer_category(title, d.get("subreddit", "")),
                    "region":         "GLOBAL",
                    "subreddit":      f"r/{d.get('subreddit', '')}",
                    "keyword":        keyword,
                })
            return posts
        except Exception as e:
            log.error("Reddit search error [%s]: %s", keyword, e)
            return []

    def _infer_category(self, title: str, sub: str) -> str:
        t   = title.lower()
        sub = sub.lower()
        if any(x in t or x in sub for x in ["kpop", "bangtan", "bts", "entertainment", "idol"]): return "entertainment"
        if any(x in t or x in sub for x in ["soccer", "sports", "nba", "football", "baseball"]): return "sports"
        if any(x in t or x in sub for x in ["tech", "technology", "ai", "programming", "samsung"]): return "tech"
        if any(x in t or x in sub for x in ["economy", "finance", "market", "trade", "stock"]): return "economy"
        return "news"


# ━━━ YouTubeCollector ━━━━━━━━━━━━━━━━━━━━━━
class YouTubeCollector:
    """키워드 기반 YouTube 검색"""

    SKIP_KEYWORDS = [
        "tiktok reaction", "bollywood", "indian tiktok", "samosa",
        "daizy aizy", "simpal kharel", "#shorts", "reels #trending",
        "eating challenge", "food challenge", "biryani", "hindi dubbed",
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
        if any(x in t for x in ["news", "politics", "breaking", "election", "war"]): return "news"
        if any(x in t for x in ["kpop", "k-pop", "bts", "blackpink", "idol", "concert"]): return "entertainment"
        if any(x in t for x in ["soccer", "football", "nba", "sports", "baseball"]): return "sports"
        if any(x in t for x in ["tech", "ai", "technology", "samsung", "apple", "nvidia"]): return "tech"
        if any(x in t for x in ["economy", "stock", "market", "trade", "tariff"]): return "economy"
        return "entertainment"

    async def search(self, session, keyword: str, limit: int = 8) -> list:
        import aiohttp
        if not self.api_key:
            return []
        try:
            url = (
                f"https://www.googleapis.com/youtube/v3/search"
                f"?part=snippet&q={keyword}&type=video&order=viewCount"
                f"&maxResults={limit}"
                f"&publishedAfter={self._published_after(7)}"
                f"&relevanceLanguage=ko&key={self.api_key}"
            )
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=12)) as r:
                if r.status != 200:
                    log.warning("YouTube API %s: %s", r.status, keyword)
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
                    "keyword":        keyword,
                })
            return posts
        except Exception as e:
            log.error("YouTube search error [%s]: %s", keyword, e)
            return []


# ━━━ Aggregator ━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class Aggregator:
    """
    흐름:
    1. NaverCollector → 트렌드 키워드 추출
    2. 키워드 → RedditCollector + YouTubeCollector 병렬 검색
    3. JP/CN RSS 수집
    4. 전체 병합 · 중복 제거 · 균등 믹싱
    5. AI 재작성
    """

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
출처: {post['source']}
키워드: {post.get('keyword', '')}

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

    def _deduplicate(self, posts: list) -> list:
        seen, unique = set(), []
        for p in posts:
            if p["id"] not in seen:
                seen.add(p["id"])
                unique.append(p)
        return unique

    def _filter(self, posts: list) -> list:
        """score/날짜 기준 필터링"""
        return [p for p in posts if is_recent(p.get("publishedAt", ""), days=7)]

    def _mix_balanced(self, global_posts: list, jp_posts: list, cn_posts: list, limit: int = 50) -> list:
        """미국 50% / 일본 25% / 중국 25% 균등 믹싱"""
        us_limit = limit // 2
        jp_limit = limit // 4
        cn_limit = limit - us_limit - jp_limit

        us_q = list(sorted(global_posts, key=lambda p: p.get("score", 0), reverse=True))[:us_limit]
        jp_q = list(sorted(jp_posts,     key=lambda p: p.get("publishedAt", ""), reverse=True))[:jp_limit]
        cn_q = list(sorted(cn_posts,     key=lambda p: p.get("publishedAt", ""), reverse=True))[:cn_limit]

        mixed = []
        while us_q or jp_q or cn_q:
            for _ in range(2):
                if us_q: mixed.append(us_q.pop(0))
            if jp_q: mixed.append(jp_q.pop(0))
            if cn_q: mixed.append(cn_q.pop(0))
        return mixed[:limit]

    async def run(self, limit: int = 50) -> dict:
        import aiohttp
        from naver_collector import NaverCollector

        naver = NaverCollector()

        # ① Naver → 키워드 추출
        connector = aiohttp.TCPConnector(limit=10, ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            keywords = await naver.extract_keywords(session, top_n=8)
            log.info("추출된 키워드: %s", keywords)

            # ② 키워드 기반 Reddit + YouTube 병렬 검색
            search_tasks = []
            for kw in keywords[:6]:
                search_tasks.append(self.reddit.search(session, kw, limit=8))
                search_tasks.append(self.youtube.search(session, kw, limit=6))

            search_results = await asyncio.gather(*search_tasks, return_exceptions=True)

        global_posts = []
        for r in search_results:
            if isinstance(r, list):
                global_posts.extend(r)

        # ③ JP / CN RSS 수집
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

        log.info("수집 현황 — 글로벌: %d, JP: %d, CN: %d", len(global_posts), len(jp_posts), len(cn_posts))

        # ④ 필터 + 중복 제거
        global_posts = self._deduplicate(self._filter(global_posts))
        jp_posts     = self._deduplicate(jp_posts)
        cn_posts     = self._deduplicate(cn_posts)

        # ⑤ 균등 믹싱
        mixed = self._mix_balanced(global_posts, jp_posts, cn_posts, limit=limit)

        # ⑥ AI 재작성 (상위 15개)
        top      = mixed[:15]
        rewrites = await asyncio.gather(*[self._rewrite(p) for p in top], return_exceptions=True)

        return {
            "posts":      mixed,
            "total":      len(mixed),
            "cached":     False,
            "updated_at": now_iso(),
            "keywords":   keywords,
            "stats": {
                "global": len(global_posts),
                "jp":     len(jp_posts),
                "cn":     len(cn_posts),
            },
        }
