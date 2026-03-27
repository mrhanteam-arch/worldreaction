# china_collector.py — 중국 반응 데이터 수집 모듈
# 공식 RSS / 공개 API만 사용 (HTML 크롤링 없음)
import asyncio
import logging
import hashlib
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

log = logging.getLogger("wr.china")

def make_id(source, url):
    return hashlib.md5(f"{source}:{url}".encode()).hexdigest()[:16]

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def parse_rss_date(date_str: str) -> str:
    if not date_str:
        return now_iso()
    try:
        from email.utils import parsedate_to_datetime
        return parsedate_to_datetime(date_str).isoformat()
    except Exception:
        return now_iso()

def infer_category_cn(title: str) -> str:
    """중국어/영어 제목에서 카테고리 추론"""
    t = title.lower()
    if any(x in t for x in ["经济", "股市", "人民币", "economy", "finance", "stock"]): return "economy"
    if any(x in t for x in ["政治", "政府", "习近平", "politics", "government"]): return "news"
    if any(x in t for x in ["体育", "足球", "篮球", "sport", "soccer", "basketball"]): return "sports"
    if any(x in t for x in ["娱乐", "明星", "电影", "entertainment", "celebrity"]): return "entertainment"
    if any(x in t for x in ["科技", "技术", "ai", "tech", "technology"]): return "tech"
    return "news"

# ── RSS 피드 목록 ──────────────────────────────────────────
# 주의: 영어판 공식 RSS만 사용 (접근 가능한 소스)
CHINA_RSS_FEEDS = [
    # Xinhua 영어판 공식 RSS
    {"url": "http://www.xinhuanet.com/english/rss/worldrss.xml",     "name": "Xinhua World"},
    {"url": "http://www.xinhuanet.com/english/rss/chinarss.xml",     "name": "Xinhua China"},
    # China Daily 영어판 공식 RSS
    {"url": "https://www.chinadaily.com.cn/rss/china_rss.xml",       "name": "China Daily"},
    {"url": "https://www.chinadaily.com.cn/rss/world_rss.xml",       "name": "China Daily World"},
    # CGTN 영어판 공식 RSS
    {"url": "https://www.cgtn.com/subscribe/rss/section/china.do",   "name": "CGTN China"},
    {"url": "https://www.cgtn.com/subscribe/rss/section/world.do",   "name": "CGTN World"},
    # South China Morning Post (홍콩, 영어)
    {"url": "https://www.scmp.com/rss/91/feed",                      "name": "SCMP"},
]

async def fetch_china_rss(session, feed: dict) -> list:
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
                log.warning("CN RSS %s status %s", feed["name"], r.status)
                return []
            text = await r.text(errors="replace")

        root = ET.fromstring(text)
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        items = root.findall(".//item") or root.findall(".//atom:entry", ns)

        posts = []
        for item in items[:10]:
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
                getattr(item.find("atom:updated", ns), "text", None) or ""
            )

            if not title or not link:
                continue

            posts.append({
                "id": make_id("cn_rss", link),
                "source": feed["name"],
                "sourceUrl": link,
                "originalTitle": title,
                "rewrittenTitle": title,
                "summary": "",
                "score": 0,
                "commentCount": 0,
                "publishedAt": parse_rss_date(pub_raw),
                "badges": [],
                "category": infer_category_cn(title),
                "region": "CN",
            })
        return posts
    except Exception as e:
        log.error("CN RSS error [%s]: %s", feed["name"], e)
        return []


class ChinaCollector:
    """중국 반응 데이터 수집기"""

    async def collect(self, limit: int = 30) -> list:
        import aiohttp
        connector = aiohttp.TCPConnector(limit=5, ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [fetch_china_rss(session, feed) for feed in CHINA_RSS_FEEDS]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        posts = []
        for r in results:
            if isinstance(r, list):
                posts.extend(r)

        # 중복 제거
        seen = set()
        unique = []
        for p in posts:
            if p["id"] not in seen:
                seen.add(p["id"])
                unique.append(p)

        unique.sort(key=lambda p: p.get("publishedAt", ""), reverse=True)
        return unique[:limit]
