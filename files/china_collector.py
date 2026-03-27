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
            return datetime.fromisoformat(date_str.replace("Z", "+00:00")).isoformat()
        except Exception:
            return now_iso()

def is_recent(published_at: str, days: int = 7) -> bool:
    try:
        pub = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - pub).days <= days
    except Exception:
        return True

def clean_text(text: str) -> str:
    """깨진 인코딩 문자 제거"""
    if not text:
        return ""
    # 깨진 문자 패턴 제거 (�, 쁶, 셲 등)
    import re
    cleaned = re.sub(r'[^\x00-\x7F\uAC00-\uD7A3\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF]', '', text)
    cleaned = cleaned.strip()
    return cleaned if len(cleaned) > 5 else text  # 너무 많이 제거되면 원본 유지

def infer_category_cn(title: str) -> str:
    t = title.lower()
    if any(x in t for x in ["economy", "finance", "stock", "trade", "tariff", "gdp", "market", "bank"]): return "economy"
    if any(x in t for x in ["politics", "government", "military", "war", "election", "diplomat"]): return "news"
    if any(x in t for x in ["sport", "soccer", "basketball", "olympic", "tennis"]): return "sports"
    if any(x in t for x in ["entertainment", "celebrity", "film", "music", "concert"]): return "entertainment"
    if any(x in t for x in ["tech", "technology", "ai", "digital", "cyber", "science"]): return "tech"
    return "news"

CHINA_RSS_FEEDS = [
    # CGTN 영어판 (안정적, 최신 데이터)
    {"url": "https://www.cgtn.com/subscribe/rss/section/china.do",   "name": "CGTN China"},
    {"url": "https://www.cgtn.com/subscribe/rss/section/world.do",   "name": "CGTN World"},
    {"url": "https://www.cgtn.com/subscribe/rss/section/business.do","name": "CGTN Business"},
    # SCMP (홍콩, 영어)
    {"url": "https://www.scmp.com/rss/91/feed",  "name": "SCMP China"},
    {"url": "https://www.scmp.com/rss/4/feed",   "name": "SCMP Asia"},
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
        async with session.get(feed["url"], headers=headers, timeout=aiohttp.ClientTimeout(total=12)) as r:
            if r.status != 200:
                log.warning("CN RSS %s status %s", feed["name"], r.status)
                return []
            # UTF-8 강제 디코딩으로 인코딩 깨짐 방지
            raw   = await r.read()
            text  = raw.decode("utf-8", errors="replace")

        root  = ET.fromstring(text)
        ns    = {"atom": "http://www.w3.org/2005/Atom"}
        items = root.findall(".//item") or root.findall(".//atom:entry", ns)

        posts = []
        for item in items[:15]:
            title = (
                getattr(item.find("title"), "text", None) or
                getattr(item.find("atom:title", ns), "text", None) or ""
            ).strip()

            link_el = item.find("link")
            if link_el is not None and link_el.text:
                link = link_el.text.strip()
            else:
                link_el2 = item.find("atom:link", ns)
                link = (link_el2.get("href", "") if link_el2 is not None else "").strip()

            pub_raw = (
                getattr(item.find("pubDate"), "text", None) or
                getattr(item.find("atom:updated", ns), "text", None) or ""
            )

            if not title or not link:
                continue

            # 인코딩 깨짐 정리
            title = clean_text(title)
            if not title or len(title) < 5:
                continue

            pub = parse_rss_date(pub_raw)
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

        seen, unique = set(), []
        for p in posts:
            if p["id"] not in seen:
                seen.add(p["id"])
                unique.append(p)

        unique.sort(key=lambda p: p.get("publishedAt", ""), reverse=True)
        log.info("CN 수집 완료: %d건", len(unique[:limit]))
        return unique[:limit]
