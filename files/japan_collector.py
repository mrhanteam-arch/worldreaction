# japan_collector.py — 일본 반응 데이터 수집 모듈
import asyncio
import logging
import hashlib
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta

log = logging.getLogger("wr.japan")

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

def is_recent(published_at: str, days: int = 3) -> bool:
    try:
        pub = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - pub).days <= days
    except Exception:
        return True

def infer_category_jp(title: str) -> str:
    t = title.lower()
    if any(x in t for x in ["経済", "株", "円", "economy", "stock", "finance", "trade", "market"]): return "economy"
    if any(x in t for x in ["政治", "選挙", "首相", "politics", "election", "government", "military"]): return "news"
    if any(x in t for x in ["スポーツ", "野球", "サッカー", "sport", "baseball", "soccer", "tennis"]): return "sports"
    if any(x in t for x in ["芸能", "アイドル", "音楽", "kpop", "entertainment", "music"]): return "entertainment"
    if any(x in t for x in ["技術", "ai", "tech", "technology", "science", "科学"]): return "tech"
    return "news"

def decode_rss(raw: bytes, feed_url: str) -> str:
    """RSS 인코딩 자동 감지 — NHK는 UTF-8, 일부는 EUC-JP"""
    # XML 선언에서 인코딩 추출 시도
    import re
    head = raw[:200]
    match = re.search(rb'encoding=["\']([^"\']+)["\']', head)
    if match:
        enc = match.group(1).decode("ascii", errors="ignore").lower()
        try:
            return raw.decode(enc, errors="replace")
        except Exception:
            pass
    # 폴백: UTF-8 → EUC-JP → Shift-JIS 순서로 시도
    for enc in ["utf-8", "euc-jp", "shift-jis", "cp932"]:
        try:
            return raw.decode(enc, errors="strict")
        except Exception:
            continue
    return raw.decode("utf-8", errors="replace")

# NHK는 영어판만 사용 (인코딩 문제 완전 회피)
JAPAN_RSS_FEEDS = [
    # NHK World 영어판 (깔끔한 UTF-8)
    {"url": "https://www3.nhk.or.jp/nhkworld/en/news/feeds/", "name": "NHK World", "lang": "en"},
    # Japan Times 영어판
    {"url": "https://www.japantimes.co.jp/feed/",             "name": "Japan Times","lang": "en"},
    # Yahoo Japan (일본어 — 번역 적용)
    {"url": "https://news.yahoo.co.jp/rss/topics/top-picks.xml", "name": "Yahoo Japan", "lang": "ja"},
    # Asahi 영어판
    {"url": "https://www.asahi.com/ajw/rss.feed",             "name": "Asahi English","lang": "en"},
    # Mainichi 영어판
    {"url": "https://mainichi.jp/rss/etc/mainichi-flash.rss", "name": "Mainichi",   "lang": "en"},
]

async def fetch_japan_rss(session, feed: dict) -> list:
    import aiohttp
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 WorldReaction/2.0",
            "Accept": "application/rss+xml, application/xml, text/xml, */*"
        }
        async with session.get(feed["url"], headers=headers, timeout=aiohttp.ClientTimeout(total=12)) as r:
            if r.status != 200:
                log.warning("JP RSS %s status %s", feed["name"], r.status)
                return []
            raw  = await r.read()
            text = decode_rss(raw, feed["url"])

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
                getattr(item.find("atom:updated", ns), "text", None) or
                getattr(item.find("atom:published", ns), "text", None) or ""
            )

            if not title or not link:
                continue

            pub = parse_rss_date(pub_raw)
            if not is_recent(pub, days=3):
                continue

            posts.append({
                "id":             make_id("jp_rss", link),
                "source":         feed["name"],
                "sourceUrl":      link,
                "originalTitle":  title,
                "rewrittenTitle": title,
                "summary":        "",
                "score":          0,
                "commentCount":   0,
                "publishedAt":    pub,
                "badges":         [],
                "category":       infer_category_jp(title),
                "region":         "JP",
                "lang":           feed.get("lang", "en"),
            })
        return posts
    except Exception as e:
        log.error("JP RSS error [%s]: %s", feed["name"], e)
        return []


async def translate_jp_titles(posts: list) -> list:
    """일본어 제목 → 한국어 번역"""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return posts

    ja_posts = [p for p in posts if p.get("lang") == "ja"]
    if not ja_posts:
        return posts

    try:
        import anthropic, json, re
        client = anthropic.Anthropic(api_key=api_key)
        batch  = ja_posts[:10]
        titles = [p["originalTitle"] for p in batch]

        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=800,
            messages=[{"role": "user", "content": f"""다음 일본어 뉴스 제목들을 한국어로 자연스럽게 번역해줘.
한국 독자가 읽기 좋은 방식으로 번역하고, 너무 직역하지 마.

제목 목록 (JSON 배열):
{json.dumps(titles, ensure_ascii=False)}

반드시 같은 수의 번역 결과를 JSON 배열로만 반환해줘. 예: ["번역1", "번역2", ...]"""}]
        )
        text  = resp.content[0].text.strip()
        match = re.search(r'\[.*\]', text, re.DOTALL)
        if match:
            translated = json.loads(match.group())
            for i, post in enumerate(batch):
                if i < len(translated) and translated[i]:
                    post["rewrittenTitle"] = translated[i]
    except Exception as e:
        log.warning("JP 번역 오류: %s", e)
    return posts


class JapanCollector:
    async def collect(self, limit: int = 30) -> list:
        import aiohttp
        connector = aiohttp.TCPConnector(limit=5, ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks   = [fetch_japan_rss(session, feed) for feed in JAPAN_RSS_FEEDS]
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
        unique = unique[:limit]
        unique = await translate_jp_titles(unique)

        log.info("JP 수집 완료: %d건", len(unique))
        return unique
