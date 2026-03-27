# japan_collector.py — 일본 반응 데이터 수집 모듈
import asyncio
import logging
import hashlib
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

log = logging.getLogger("wr.japan")

def make_id(source, url):
    return hashlib.md5(f"{source}:{url}".encode()).hexdigest()[:16]

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def parse_rss_date(date_str: str) -> str:
    """RSS 날짜 파싱 → ISO 형식 변환"""
    if not date_str:
        return now_iso()
    try:
        from email.utils import parsedate_to_datetime
        return parsedate_to_datetime(date_str).isoformat()
    except Exception:
        return now_iso()

def infer_category_jp(title: str) -> str:
    """일본어/영어 제목에서 카테고리 추론"""
    t = title.lower()
    if any(x in t for x in ["経済", "株", "円", "economy", "stock", "finance"]): return "economy"
    if any(x in t for x in ["政治", "選挙", "首相", "politics", "election"]): return "news"
    if any(x in t for x in ["スポーツ", "野球", "サッカー", "sport", "baseball"]): return "sports"
    if any(x in t for x in ["芸能", "アイドル", "音楽", "kpop", "entertainment"]): return "entertainment"
    if any(x in t for x in ["技術", "ai", "tech", "テック"]): return "tech"
    return "news"

# ── RSS 피드 목록 ──────────────────────────────────────────
JAPAN_RSS_FEEDS = [
    # NHK 공식 RSS
    {"url": "https://www3.nhk.or.jp/rss/news/cat0.xml",  "name": "NHK뉴스"},
    {"url": "https://www3.nhk.or.jp/rss/news/cat1.xml",  "name": "NHK정치"},
    {"url": "https://www3.nhk.or.jp/rss/news/cat3.xml",  "name": "NHK사회"},
    # Yahoo Japan RSS
    {"url": "https://news.yahoo.co.jp/rss/topics/top-picks.xml", "name": "Yahoo Japan"},
    # Japan Times (영어)
    {"url": "https://www.japantimes.co.jp/feed/",         "name": "Japan Times"},
    # Asahi Shimbun (영어)
    {"url": "https://www.asahi.com/rss/asahi/newsheadlines.rdf", "name": "Asahi"},
]

async def fetch_japan_rss(session, feed: dict) -> list:
    """단일 RSS 피드 수집"""
    import aiohttp
    try:
        headers = {
            "User-Agent": "WorldReaction/2.0 RSS Reader",
            "Accept": "application/rss+xml, application/xml, text/xml"
        }
        async with session.get(
            feed["url"], headers=headers,
            timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            if r.status != 200:
                log.warning("JP RSS %s status %s", feed["name"], r.status)
                return []
            text = await r.text(errors="replace")

        root = ET.fromstring(text)
        ns = {"atom": "http://www.w3.org/2005/Atom"}

        # RSS 2.0 / RDF 공통 파싱
        items = root.findall(".//item") or root.findall(".//atom:entry", ns)

        posts = []
        for item in items[:10]:  # 피드당 최대 10개
            title = (
                getattr(item.find("title"), "text", None) or
                getattr(item.find("atom:title", ns), "text", None) or ""
            ).strip()
            link = (
                getattr(item.find("link"), "text", None) or
                getattr(item.find("atom:link", ns), "get", lambda k, d="": d)("href") or ""
            ).strip()
            pub_raw = (
                getattr(item.find("pubDate"), "text", None) or
                getattr(item.find("dc:date", {"dc": "http://purl.org/dc/elements/1.1/"}), "text", None) or
                getattr(item.find("atom:updated", ns), "text", None) or ""
            )

            if not title or not link:
                continue

            posts.append({
                "id": make_id("jp_rss", link),
                "source": feed["name"],
                "sourceUrl": link,
                "originalTitle": title,
                "rewrittenTitle": title,
                "summary": "",
                "score": 0,
                "commentCount": 0,
                "publishedAt": parse_rss_date(pub_raw),
                "badges": [],
                "category": infer_category_jp(title),
                "region": "JP",
            })
        return posts
    except Exception as e:
        log.error("JP RSS error [%s]: %s", feed["name"], e)
        return []


class JapanCollector:
    """일본 반응 데이터 수집기"""

    async def collect(self, limit: int = 30) -> list:
        import aiohttp
        connector = aiohttp.TCPConnector(limit=5, ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [fetch_japan_rss(session, feed) for feed in JAPAN_RSS_FEEDS]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        posts = []
        for r in results:
            if isinstance(r, list):
                posts.extend(r)

        # 중복 제거 (id 기준)
        seen = set()
        unique = []
        for p in posts:
            if p["id"] not in seen:
                seen.add(p["id"])
                unique.append(p)

        # 최신순 정렬
        unique.sort(key=lambda p: p.get("publishedAt", ""), reverse=True)
        return unique[:limit]
