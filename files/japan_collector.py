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
    if any(x in t for x in ["経済", "株", "円", "economy", "stock", "finance", "trade", "market", "tariff"]): return "economy"
    if any(x in t for x in ["政治", "選挙", "首相", "politics", "election", "government", "military", "defense"]): return "news"
    if any(x in t for x in ["スポーツ", "野球", "サッカー", "sport", "baseball", "soccer", "tennis", "olympic"]): return "sports"
    if any(x in t for x in ["芸能", "アイドル", "音楽", "kpop", "entertainment", "film", "music", "celebrity"]): return "entertainment"
    if any(x in t for x in ["技術", "ai", "tech", "technology", "digital", "科学", "science"]): return "tech"
    return "news"

JAPAN_RSS_FEEDS = [
    {"url": "https://www3.nhk.or.jp/rss/news/cat0.xml", "name": "NHK뉴스",   "lang": "ja"},
    {"url": "https://www3.nhk.or.jp/rss/news/cat1.xml", "name": "NHK정치",   "lang": "ja"},
    {"url": "https://www3.nhk.or.jp/rss/news/cat3.xml", "name": "NHK사회",   "lang": "ja"},
    {"url": "https://news.yahoo.co.jp/rss/topics/top-picks.xml", "name": "Yahoo Japan", "lang": "ja"},
    {"url": "https://www.japantimes.co.jp/feed/",       "name": "Japan Times","lang": "en"},
    {"url": "https://www3.nhk.or.jp/nhkworld/en/news/feeds/", "name": "NHK World", "lang": "en"},
]

async def fetch_japan_rss(session, feed: dict) -> list:
    import aiohttp
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 WorldReaction/2.0 RSS Reader",
            "Accept": "application/rss+xml, application/xml, text/xml, */*"
        }
        async with session.get(feed["url"], headers=headers, timeout=aiohttp.ClientTimeout(total=12)) as r:
            if r.status != 200:
                log.warning("JP RSS %s status %s", feed["name"], r.status)
                return []
            text = await r.text(errors="replace")

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
                "lang":           feed.get("lang", "ja"),
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
