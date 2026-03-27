# naver_collector.py — 네이버 뉴스 API 기반 트렌드 키워드 수집
import asyncio
import logging
import os
import hashlib
from datetime import datetime, timezone

log = logging.getLogger("wr.naver")

def make_id(source, url):
    return hashlib.md5(f"{source}:{url}".encode()).hexdigest()[:16]

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def parse_naver_date(date_str: str) -> str:
    """네이버 날짜 포맷 → ISO"""
    try:
        # 예: Thu, 27 Mar 2026 10:00:00 +0900
        from email.utils import parsedate_to_datetime
        return parsedate_to_datetime(date_str).isoformat()
    except Exception:
        return now_iso()

def infer_category(title: str) -> str:
    t = title.lower()
    if any(x in t for x in ["경제", "주가", "환율", "금리", "gdp", "무역", "관세"]): return "economy"
    if any(x in t for x in ["정치", "대선", "국회", "대통령", "외교", "안보"]): return "news"
    if any(x in t for x in ["스포츠", "축구", "야구", "농구", "올림픽", "손흥민", "류현진"]): return "sports"
    if any(x in t for x in ["연예", "아이돌", "bts", "블랙핑크", "드라마", "영화", "k-pop", "kpop"]): return "entertainment"
    if any(x in t for x in ["기술", "ai", "반도체", "삼성", "테크", "it", "스타트업"]): return "tech"
    return "news"

# ━━━ NaverCollector ━━━━━━━━━━━━━━━━━━━━━━━━
class NaverCollector:
    """
    네이버 뉴스 검색 API
    역할: 트렌드 키워드 수집 + 기사 URL/제목 제공
    환경변수: NAVER_CLIENT_ID, NAVER_CLIENT_SECRET
    """

    BASE_URL = "https://openapi.naver.com/v1/search/news.json"

    # 기본 트렌드 쿼리 (키워드 미입력 시 사용)
    DEFAULT_QUERIES = [
        "해외반응", "글로벌 이슈", "한국 국제뉴스",
        "미국 한국", "일본 반응", "중국 반응",
        "kpop 해외", "삼성 해외", "손흥민",
    ]

    def __init__(self):
        self.client_id     = os.environ.get("NAVER_CLIENT_ID", "")
        self.client_secret = os.environ.get("NAVER_CLIENT_SECRET", "")

    def _headers(self) -> dict:
        return {
            "X-Naver-Client-Id":     self.client_id,
            "X-Naver-Client-Secret": self.client_secret,
            "Accept": "application/json",
        }

    async def search(self, session, query: str, display: int = 10) -> list:
        """단일 키워드로 뉴스 검색"""
        import aiohttp
        if not self.client_id:
            log.warning("NAVER_CLIENT_ID 없음 — 스킵")
            return []
        try:
            params = {
                "query":   query,
                "display": display,
                "sort":    "date",  # 최신순
            }
            async with session.get(
                self.BASE_URL,
                headers=self._headers(),
                params=params,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                if r.status != 200:
                    log.warning("Naver API %s: %s", r.status, query)
                    return []
                data = await r.json(content_type=None)

            posts = []
            for item in data.get("items", []):
                # HTML 태그 제거
                import re
                title = re.sub(r"<[^>]+>", "", item.get("title", "")).strip()
                link  = item.get("originallink") or item.get("link", "")
                pub   = parse_naver_date(item.get("pubDate", ""))

                if not title or not link:
                    continue

                posts.append({
                    "id":             make_id("naver", link),
                    "source":         "naver",
                    "sourceUrl":      link,
                    "originalTitle":  title,
                    "rewrittenTitle": title,
                    "summary":        "",
                    "score":          0,
                    "commentCount":   0,
                    "publishedAt":    pub,
                    "badges":         [],
                    "category":       infer_category(title),
                    "region":         "GLOBAL",
                    "keyword":        query,  # 어떤 키워드로 수집됐는지
                })
            return posts
        except Exception as e:
            log.error("Naver search error [%s]: %s", query, e)
            return []

    async def extract_keywords(self, session, top_n: int = 10) -> list:
        """
        수집된 뉴스 제목에서 트렌드 키워드 추출
        → Reddit/YouTube 검색에 사용할 키워드 리스트 반환
        """
        import aiohttp
        all_titles = []

        tasks = [self.search(session, q, display=5) for q in self.DEFAULT_QUERIES[:5]]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, list):
                for post in r:
                    all_titles.append(post["originalTitle"])

        if not all_titles:
            return ["korea trending", "kpop", "samsung", "bts", "손흥민"]

        # 간단한 키워드 빈도 추출
        from collections import Counter
        import re

        STOPWORDS = {"의", "이", "가", "을", "를", "은", "는", "에", "서", "와", "과",
                     "로", "으로", "도", "만", "한", "하다", "있다", "등", "및", "위해",
                     "the", "a", "an", "is", "in", "of", "to", "and", "for", "with"}

        word_counts = Counter()
        for title in all_titles:
            words = re.findall(r"[가-힣a-zA-Z]{2,}", title)
            for w in words:
                if w.lower() not in STOPWORDS and len(w) >= 2:
                    word_counts[w] += 1

        top_keywords = [w for w, _ in word_counts.most_common(top_n)]
        log.info("Naver 키워드 추출: %s", top_keywords[:5])
        return top_keywords

    async def collect(self, limit: int = 30) -> list:
        """전체 수집 (기본 쿼리 기반)"""
        import aiohttp
        connector = aiohttp.TCPConnector(limit=5, ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks   = [self.search(session, q, display=5) for q in self.DEFAULT_QUERIES]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        posts = []
        for r in results:
            if isinstance(r, list):
                posts.extend(r)

        # 중복 제거
        seen, unique = set(), []
        for p in posts:
            if p["id"] not in seen:
                seen.add(p["id"])
                unique.append(p)

        unique.sort(key=lambda p: p.get("publishedAt", ""), reverse=True)
        return unique[:limit]
