# ranking.py — 전세계 반응 실시간 랭킹 모듈
# 수집된 게시글 score 기반으로 핫 토픽 랭킹 생성
import re
import logging
from collections import defaultdict
from datetime import datetime, timezone

log = logging.getLogger("wr.ranking")

# ── 배지 기준 ──────────────────────────────────────────────
BADGE_HOT   = 10000   # score 합산 1만 이상
BADGE_UP    = 3000    # score 합산 3천 이상
BADGE_NEW   = 0       # 나머지

# ── 한국어 불용어 (랭킹 제외 단어) ────────────────────────
STOPWORDS = {
    "the", "a", "an", "is", "in", "of", "to", "and", "for",
    "with", "on", "at", "by", "from", "this", "that", "it",
    "be", "are", "was", "has", "have", "had", "will", "not",
    "as", "or", "but", "if", "about", "after", "before",
    "이", "그", "저", "를", "은", "는", "이", "가", "에",
    "의", "로", "으로", "도", "만", "과", "와", "한", "하다",
    "있다", "없다", "이다", "됩니다", "합니다", "했다",
}

def extract_keywords(title: str) -> list:
    """제목에서 의미 있는 키워드 추출 (2글자 이상)"""
    # 특수문자 제거
    clean = re.sub(r"[^\w\s가-힣]", " ", title)
    words = clean.split()
    keywords = []
    for w in words:
        w = w.strip()
        if len(w) < 2:
            continue
        if w.lower() in STOPWORDS:
            continue
        # 숫자만인 경우 제외
        if re.match(r"^\d+$", w):
            continue
        keywords.append(w)
    return keywords


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def format_count(n: int) -> str:
    """숫자 → 읽기 좋은 형식"""
    if n >= 10000:
        return f"{n/10000:.1f}만"
    if n >= 1000:
        return f"{n/1000:.1f}천"
    return str(n)


def assign_badge(score: int) -> str:
    if score >= BADGE_HOT:
        return "hot"
    if score >= BADGE_UP:
        return "up"
    return "new"


async def build_ranking(posts: list, top_n: int = 10) -> dict:
    """
    수집된 posts 리스트에서 핫 토픽 랭킹 생성
    - 키워드별 score 합산
    - 소스 다양성 반영 (JP/CN/글로벌 구분)
    """
    keyword_score   = defaultdict(int)   # 키워드 → score 합산
    keyword_sources = defaultdict(set)   # 키워드 → 출처 소스 집합
    keyword_regions = defaultdict(set)   # 키워드 → region 집합

    for post in posts:
        title   = post.get("rewrittenTitle") or post.get("originalTitle", "")
        score   = post.get("score", 1)      # RSS 소스는 score=0 → 1로 보정
        source  = post.get("source", "")
        region  = post.get("region", "GLOBAL")

        # score 0인 경우 노출 빈도로 보정 (RSS/뉴스)
        if score == 0:
            score = 100

        keywords = extract_keywords(title)
        for kw in keywords:
            keyword_score[kw]   += score
            keyword_sources[kw].add(source)
            keyword_regions[kw].add(region)

    # 정렬 (score 합산 기준)
    sorted_kw = sorted(keyword_score.items(), key=lambda x: x[1], reverse=True)

    ranking = []
    for rank, (word, total_score) in enumerate(sorted_kw[:top_n], start=1):
        regions = list(keyword_regions[word])
        sources = list(keyword_sources[word])

        # 국가 반응 표시
        region_label = []
        if "JP" in regions:   region_label.append("🇯🇵")
        if "CN" in regions:   region_label.append("🇨🇳")
        if any(r not in ("JP", "CN") for r in regions):
            region_label.append("🌐")

        ranking.append({
            "rank":         rank,
            "word":         word,
            "count":        format_count(total_score),
            "rawScore":     total_score,
            "badge":        assign_badge(total_score),
            "regions":      regions,
            "regionLabel":  " ".join(region_label),
            "sources":      sources,
            "sourceCount":  len(sources),
        })

    return {
        "keywords":   ranking,
        "total":      len(ranking),
        "updated_at": now_iso(),
    }


async def get_ranking(force_refresh: bool = False) -> dict:
    """캐시 우선 → 없으면 실시간 계산"""
    try:
        import cache
        if not force_refresh:
            cached = await cache.get("trending:keywords")
            if cached:
                return {**cached, "cached": True}
    except Exception:
        pass

    # 현재 캐시된 피드 데이터 활용
    all_posts = []
    try:
        import cache
        # 메인 피드
        main = await cache.get("feed:home")
        if main:
            all_posts.extend(main.get("posts", []))
        # JP 피드
        jp = await cache.get("feed:jp")
        if jp:
            all_posts.extend(jp.get("posts", []))
        # CN 피드
        cn = await cache.get("feed:cn")
        if cn:
            all_posts.extend(cn.get("posts", []))
    except Exception as e:
        log.warning("캐시 로드 실패: %s", e)

    # 캐시 데이터 없으면 실시간 수집
    if not all_posts:
        log.info("캐시 없음 → 실시간 수집으로 랭킹 생성")
        try:
            import pipeline
            result = await pipeline.get_feed(force_refresh=True, limit=50)
            all_posts = result.get("posts", [])
        except Exception as e:
            log.error("실시간 수집 실패: %s", e)
            return _fallback_ranking()

    ranking = await build_ranking(all_posts, top_n=10)

    try:
        import cache
        await cache.set("trending:keywords", ranking, 900)  # 15분 캐시
    except Exception:
        pass

    return ranking


def _fallback_ranking() -> dict:
    """수집 실패 시 기본값"""
    return {
        "keywords": [
            {"rank": 1,  "word": "BTS",       "count": "2.4만", "rawScore": 24000, "badge": "hot", "regions": ["GLOBAL"], "regionLabel": "🌐", "sources": [], "sourceCount": 0},
            {"rank": 2,  "word": "트럼프 관세", "count": "1.8만", "rawScore": 18000, "badge": "hot", "regions": ["GLOBAL"], "regionLabel": "🌐", "sources": [], "sourceCount": 0},
            {"rank": 3,  "word": "삼성 AI",    "count": "1.2만", "rawScore": 12000, "badge": "hot", "regions": ["GLOBAL"], "regionLabel": "🌐", "sources": [], "sourceCount": 0},
            {"rank": 4,  "word": "손흥민",     "count": "9.8천", "rawScore": 9800,  "badge": "up",  "regions": ["GLOBAL"], "regionLabel": "🌐", "sources": [], "sourceCount": 0},
            {"rank": 5,  "word": "일본 지진",  "count": "8.1천", "rawScore": 8100,  "badge": "up",  "regions": ["JP"],     "regionLabel": "🇯🇵", "sources": [], "sourceCount": 0},
        ],
        "total": 5,
        "updated_at": now_iso(),
        "cached": False,
        "fallback": True,
    }
