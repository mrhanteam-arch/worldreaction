# server.py — WorldReaction v2 API 서버 (JP/CN 스케줄러 + 필터 추가)
import asyncio
import logging
import os
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("wr.server")

app = FastAPI(title="WorldReaction API v2")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 백그라운드 스케줄러 — 앱 시작 시 자동 실행
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

SCHEDULE_INTERVAL_MAIN = 600   # 기존 피드: 10분마다
SCHEDULE_INTERVAL_JP   = 900   # 일본 피드: 15분마다
SCHEDULE_INTERVAL_CN   = 900   # 중국 피드: 15분마다

async def scheduler_main():
    """기존 파이프라인 주기 수집"""
    while True:
        try:
            import pipeline
            log.info("[스케줄러] 메인 피드 수집 시작")
            await pipeline.get_feed(force_refresh=True, limit=50)
            log.info("[스케줄러] 메인 피드 수집 완료")
        except Exception as e:
            log.error("[스케줄러] 메인 피드 오류: %s", e)
        await asyncio.sleep(SCHEDULE_INTERVAL_MAIN)

async def scheduler_jp():
    """일본 피드 주기 수집"""
    while True:
        try:
            from japan_collector import JapanCollector
            import cache
            log.info("[스케줄러] JP 수집 시작")
            posts = await JapanCollector().collect(limit=30)
            await cache.set("feed:jp", {"posts": posts, "total": len(posts)}, SCHEDULE_INTERVAL_JP)
            log.info("[스케줄러] JP 수집 완료: %d건", len(posts))
        except Exception as e:
            log.error("[스케줄러] JP 오류: %s", e)
        await asyncio.sleep(SCHEDULE_INTERVAL_JP)

async def scheduler_cn():
    """중국 피드 주기 수집"""
    while True:
        try:
            from china_collector import ChinaCollector
            import cache
            log.info("[스케줄러] CN 수집 시작")
            posts = await ChinaCollector().collect(limit=30)
            await cache.set("feed:cn", {"posts": posts, "total": len(posts)}, SCHEDULE_INTERVAL_CN)
            log.info("[스케줄러] CN 수집 완료: %d건", len(posts))
        except Exception as e:
            log.error("[스케줄러] CN 오류: %s", e)
        await asyncio.sleep(SCHEDULE_INTERVAL_CN)

@app.on_event("startup")
async def startup_event():
    """서버 시작 시 스케줄러 3개 백그라운드 실행"""
    asyncio.create_task(scheduler_main())
    asyncio.create_task(scheduler_jp())
    asyncio.create_task(scheduler_cn())
    log.info("스케줄러 3개 시작 완료 (메인/JP/CN)")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 기존 엔드포인트 (변경 없음)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.get("/")
async def root():
    return {"status": "ok", "service": "WorldReaction API v2"}

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.get("/feed")
async def feed(
    refresh: bool = False,
    limit: int = 30,
    region: str = None,   # ← 필터 추가 (JP / CN / 없으면 전체)
):
    try:
        # region 필터 있을 때
        if region:
            region = region.upper()
            import cache
            cached = await cache.get(f"feed:{region.lower()}")
            if cached and not refresh:
                return JSONResponse(content={**cached, "cached": True})

            # 캐시 없으면 실시간 수집
            if region == "JP":
                from japan_collector import JapanCollector
                posts = await JapanCollector().collect(limit=limit)
            elif region == "CN":
                from china_collector import ChinaCollector
                posts = await ChinaCollector().collect(limit=limit)
            else:
                return JSONResponse(content={"posts": [], "total": 0, "error": "region은 JP 또는 CN"})

            result = {"posts": posts, "total": len(posts), "region": region, "cached": False}
            await cache.set(f"feed:{region.lower()}", result, SCHEDULE_INTERVAL_JP)
            return JSONResponse(content=result)

        # region 없으면 기존 전체 피드 (JP/CN 포함)
        import pipeline
        result = await pipeline.get_feed(force_refresh=refresh, limit=limit)
        return JSONResponse(content=result)

    except Exception as e:
        log.error("feed error: %s", e)
        return JSONResponse(content={"posts": [], "total": 0, "error": str(e)})


@app.get("/search")
async def search(q: str = Query(...), refresh: bool = False, region: str = None):
    try:
        import pipeline
        result = await pipeline.search(query=q, force_refresh=refresh)

        # region 필터 적용
        if region:
            region = region.upper()
            result["posts"] = [p for p in result["posts"] if p.get("region") == region]
            result["total"] = len(result["posts"])

        return JSONResponse(content=result)
    except Exception as e:
        log.error("search error: %s", e)
        return JSONResponse(content={"query": q, "posts": [], "total": 0, "error": str(e)})


@app.get("/trending")
async def trending():
    try:
        import cache
        cached = await cache.get("trending:keywords")
        if cached:
            return cached
    except Exception:
        pass
    return {
        "keywords": [
            {"rank": 1,  "word": "BTS",       "count": "2.4만", "badge": "hot"},
            {"rank": 2,  "word": "트럼프 관세", "count": "1.8만", "badge": "hot"},
            {"rank": 3,  "word": "삼성 AI",    "count": "1.2만", "badge": "up"},
            {"rank": 4,  "word": "손흥민",     "count": "9.8천", "badge": ""},
            {"rank": 5,  "word": "일본 지진",  "count": "8.1천", "badge": "new"},
            {"rank": 6,  "word": "넷플릭스",   "count": "6.3천", "badge": ""},
            {"rank": 7,  "word": "AI 반도체",  "count": "5.1천", "badge": "up"},
            {"rank": 8,  "word": "갤럭시 S25", "count": "4.2천", "badge": ""},
            {"rank": 9,  "word": "엔비디아",   "count": "3.8천", "badge": "new"},
            {"rank": 10, "word": "테슬라",     "count": "2.9천", "badge": ""},
        ]
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 신규: region 전용 엔드포인트
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.get("/region/{region}")
async def region_feed(region: str, limit: int = 30, refresh: bool = False):
    """/region/JP 또는 /region/CN 전용 피드"""
    region = region.upper()
    try:
        import cache
        if not refresh:
            cached = await cache.get(f"feed:{region.lower()}")
            if cached:
                return JSONResponse(content={**cached, "cached": True})

        if region == "JP":
            from japan_collector import JapanCollector
            posts = await JapanCollector().collect(limit=limit)
        elif region == "CN":
            from china_collector import ChinaCollector
            posts = await ChinaCollector().collect(limit=limit)
        else:
            return JSONResponse(status_code=400, content={"error": "region은 JP 또는 CN"})

        result = {"posts": posts, "total": len(posts), "region": region, "cached": False}
        await cache.set(f"feed:{region.lower()}", result, SCHEDULE_INTERVAL_JP)
        return JSONResponse(content=result)

    except Exception as e:
        log.error("region feed error [%s]: %s", region, e)
        return JSONResponse(content={"posts": [], "total": 0, "error": str(e)})


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
