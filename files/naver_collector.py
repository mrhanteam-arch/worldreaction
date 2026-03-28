# naver_collector.py — 네이버 뉴스 API
import asyncio
import logging
import os
import hashlib
import re
from datetime import datetime, timezone
from collections import Counter

log = logging.getLogger("wr.naver")

def make_id(source, url):
    return hashlib.md5(f"{source}:{url}".encode()).hexdigest()[:16]

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def parse_naver_date(date_str: str) -> str:
    try:
        from email.utils import parsedate_to_datetime
        return parsedate_to_datetime(date_str).isoformat()
    except Exception:
        return now_iso()

def infer_category(title: str) -> str:
    t = title.lower()
    if any(x in t for x in ["경제", "주가", "환율", "금리", "gdp", "무역", "관세"]): return "economy"
    if any(x in t for x in ["정치", "대선", "국회", "대통령", "외교", "안보"]):       return "news"
    if any(x in t for x in ["스포츠", "축구", "야구", "농구", "올림픽", "손흥민"]):   return "sports"
    if any(x in t for x in ["연예", "아이돌", "bts", "블랙핑크", "드라마", "kpop"]): return "entertainment"
    if any(x in t for x in ["기술", "ai", "반도체", "삼성", "테크", "it"]):           return "tech"
    return "news"

STOPWORDS = {
    "quot", "amp", "lt", "gt", "apos",  # HTML 엔티티 잔여물
    "의", "이", "가", "을", "를", "은", "는", "에", "서", "와", "과",
    "로", "으로", "도", "만", "한", "하다", "있다", "등", "및", "위해",
    "the", "a", "an", "is", "in", "of", "to", "and", "for", "with",
    "기자", "뉴스", "보도", "연합", "속보", "단독", "오늘", "내일",
}

DEFAULT_QUERIES = [
    "해외반응", "글로벌 이슈", "한국 국제뉴스",
    "미국 한국", "일본 반응", "중국 반응",
    "kpop 해외", "삼성 해외", "손흥민",
]

class NaverCollector:
    BASE_URL = "https://openapi.naver.com/v1/search/news.json"

    def __init__(self):
        self.client_id     = os.environ.get("NAVER_CLIENT_ID", "").strip()
        self.client_secret = os.environ.get("NAVER_CLIENT_SECRET", "").strip()

    def _is_available(self) -> bool:
        return bool(self.client_id and self.client_secret)

    async def search(self, query: str, display: int = 10) -> list:
        """헤더를 요청마다 직접 전달 (세션 레벨 X)"""
        import aiohttp
        if not self._is_available():
            log.warning("NAVER 키 없음")
            return []
        try:
            # 헤더를 요청마다 직접 전달
            headers = {
                "X-Naver-Client-Id":     self.client_id,
                "X-Naver-Client-Secret": self.client_secret,
                "Accept":                "application/json; charset=UTF-8",
                "User-Agent":            "WorldReaction/2.0",
            }
            params = {
                "query":   query,
                "display": display,
                "sort":    "date",
            }
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self.BASE_URL,
                    headers=headers,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as r:
                    if r.status != 200:
                        text = await r.text()
                        log.warning("Naver API %s [%s]: %s", r.status, query, text[:200])
                        return []
                    data = await r.json(content_type=None)

            posts = []
            for item in data.get("items", []):
                title = re.sub(r"<[^>]+>", "", item.get("title", "")).strip()
                # HTML 엔티티 변환 (&quot; &amp; &lt; &gt; 등)
                import html
                title = html.unescape(title)
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
                    "keyword":        query,
                })
            log.info("Naver [%s]: %d건", query, len(posts))
            return posts
        except Exception as e:
            log.error("Naver search error [%s]: %s", query, e)
            return []

    async def extract_keywords(self, top_n: int = 10) -> list:
        """수집된 뉴스 제목에서 트렌드 키워드 추출"""
        tasks   = [self.search(q, display=5) for q in DEFAULT_QUERIES[:5]]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_titles = []
        for r in results:
            if isinstance(r, list):
                for post in r:
                    all_titles.append(post["originalTitle"])

        if not all_titles:
            log.warning("Naver 제목 수집 실패 — 폴백")
            return []

        word_counts = Counter()
        for title in all_titles:
            # HTML 엔티티 재처리
            import html as html_mod
            title = html_mod.unescape(title)
            words = re.findall(r"[가-힣a-zA-Z]{2,}", title)
            for w in words:
                wl = w.lower()
                # 불용어 제거 + 동사형 어미 제거 (했다, 됩니다 등)
                if wl in STOPWORDS:
                    continue
                if len(w) < 2:
                    continue
                # 영어는 3글자 이상만 (단순 두글자 영어 제외)
                if re.match(r"^[a-zA-Z]+$", w) and len(w) < 3:
                    continue
                word_counts[w] += 1

        # 상위 키워드 중 Reddit/YouTube 검색에 적합한 것만 선택
        # 한국어 키워드는 영어로 변환하여 검색에 활용
        top_words = [w for w, _ in word_counts.most_common(top_n * 2)]

        # 영어 키워드 우선, 한국어는 주요 고유명사만
        en_keywords = [w for w in top_words if re.match(r"^[a-zA-Z]", w)]
        ko_keywords = [w for w in top_words if re.match(r"^[가-힣]", w)]

        # 영어 + 한국어 혼합 (영어 먼저)
        keywords = (en_keywords + ko_keywords)[:top_n]
        log.info("Naver 키워드 추출 성공: %s", keywords[:5])
        return keywords

    async def collect(self, limit: int = 30) -> list:
        tasks   = [self.search(q, display=5) for q in DEFAULT_QUERIES]
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

        unique.sort(key=lambda p: p.get("publishedAt", ""), reverse=True)
        return unique[:limit]
