# china_collector.py — 중국 반응 데이터 수집 모듈
import asyncio
import logging
import hashlib
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta

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
        try:
            # ISO 형식 시도
            return datetime.fromisoformat(date_str.replace("Z", "+00:00")).isoformat()
        except Exception:
            return now_iso()

def is_recent(published_at: str, days: int = 7) -> bool:
    """최근 N일 이내만 허용"""
    try:
        pub = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - pub).days <= days
    except Exception:
        return True  # 날짜 파싱 실패 시 허용

def infer_category_cn(title: str) -> str:
    t = title.lower()
    if any(x in t for x in ["经济", "股市", "人民币", "economy", "finance", "stock", "trade", "tariff", "gdp"]): return "economy"
    if any(x in t for x in ["政治", "政府", "习近平", "politics", "government", "election", "military", "war"]): return "news"
    if any(x in t for x in ["体育", "足球", "篮球", "sport", "soccer", "basketball", "olympic"]): return "sports"
    if any(x in t for x in ["娱乐", "明星", "电影", "entertainment", "celebrity", "film", "music"]): return "entertainment"
    if any(x in t for x in ["科技", "技术", "ai", "tech", "technology", "digital", "cyber"]): return "tech"
    return "news"

# ── RSS 피드 목록 (Xinhua 제외 — 오래된 캐시 반환 문제) ──
CHINA_RSS_FEEDS = [
    # CGTN 영어판 (안정적)
    {"url": "https://www.cgtn.com/subscribe/rss/section/china.do",  "name": "CGTN China"},
    {"url": "https://www.cgtn.com/subscribe/rss/section/world.do",  "name": "CGTN World"},
    {"url": "https://www.cgtn.com/subscribe/rss/section/business.do","name": "CGTN Business"},
    # South China Morning Post (홍콩, 영어, 신뢰도 높음)
    {"url": "https://www.scmp.com/rss/91/feed",   "name": "SCMP China"},
    {"url": "https://www.scmp.com/rss/2/feed",    "name": "SCMP HK"},
    {"url": "https://www.scmp.com/rss/4/feed",    "name": "SCMP Asia"},
    # Global Times 영어판
    {"url": "https://www.globaltimes.cn/rss/outbrain.xml", "name": "Global Times"},
]

async def fetch_china_rss(session, feed: dict) -> list:
    import aiohttp
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 WorldReaction/2.0 RSS Reader",
            "Accept": "application/rss+xml, application/xml, text/xml, */*"
        }
        async with session.get(
            feed["url"], headers=headers,
            timeout=aiohttp.ClientTimeout(total=12)
        ) as r:
            if r.status != 200:
                log.warning("CN RSS %s status %s", feed["name"], r.status)
                return []
            text = await r.text(errors="replace")

        root = ET.fromstring(text)
        ns   = {"atom": "http://www.w3.org/2005/Atom"}
        items = root.findall(".//item") or root.findall(".//atom:entry", ns)

        posts = []
        for item in items[:15]:
            title = (
                getattr(item.find("title"), "text", None) or
                getattr(item.find("atom:title", ns), "text", None) or ""
            ).strip()

            # link 파싱
            link_el = item.find("link")
            if link_el is not None and link_el.text:
                link = link_el.text.strip()
            else:
                link_el2 = item.find("atom:link", ns)
                link = (link_el2.get("href", "") if link_el2 is not None else "").strip()

            pub_raw = (
                getattr(item.find("pubDate"), "text", None) or
                getattr(item.find("atom:updated", ns), "text", None) or
                getattr(item.find("atom:published", ns), "text", None) or ""
            )

            if not title or not link:
                continue

            pub = parse_rss_date(pub_raw)

            # 최근 7일 필터 — 오래된 기사 제외
            if not is_recent(pub, days=7):
                continue

            posts.append({
                "id":             make_id("cn_rss", link),
                "source":         feed["name"],
                "sourceUrl":      link,
                "originalTitle":  title,
                "rewrittenTitle": title,
                "summary":        "",
                "score":          0,
                "commentCount":   0,
                "publishedAt":    pub,
                "badges":         [],
                "category":       infer_category_cn(title),
                "region":         "CN",
            })
        return posts
    except Exception as e:
        log.error("CN RSS error [%s]: %s", feed["name"], e)
        return []


class ChinaCollector:
    async def collect(self, limit: int = 30) -> list:
        import aiohttp
        connector = aiohttp.TCPConnector(limit=5, ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks   = [fetch_china_rss(session, feed) for feed in CHINA_RSS_FEEDS]
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

        # 최신순 정렬
        unique.sort(key=lambda p: p.get("publishedAt", ""), reverse=True)
        log.info("CN 수집 완료: %d건", len(unique[:limit]))
        return unique[:limit]
