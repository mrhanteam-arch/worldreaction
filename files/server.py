# server.py — WorldReaction v2 API 서버 (랭킹 스케줄러 추가)
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
# 스케줄러
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

SCHEDULE_INTERVAL_MAIN    = 600   # 메인 피드:  10분
SCHEDULE_INTERVAL_JP      = 900   # JP 피드:    15분
SCHEDULE_INTERVAL_CN      = 900   # CN 피드:    15분
SCHEDULE_INTERVAL_RANKING = 900   # 랭킹:       15분

async def scheduler_main():
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

async def scheduler_ranking():
    """15분마다 전체 피드 기반으로 랭킹 재계산"""
    while True:
        try:
            import ranking
            log.info("[스케줄러] 랭킹 계산 시작")
            result = await ranking.get_ranking(force_refresh=True)
            log.info("[스케줄러] 랭킹 완료: %d개 키워드", result.get("total", 0))
        except Exception as e:
            log.error("[스케줄러] 랭킹 오류: %s", e)
        await asyncio.sleep(SCHEDULE_INTERVAL_RANKING)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(scheduler_main())
    asyncio.create_task(scheduler_jp())
    asyncio.create_task(scheduler_cn())
    asyncio.create_task(scheduler_ranking())
    log.info("스케줄러 4개 시작 완료 (메인/JP/CN/랭킹)")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 엔드포인트
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.get("/")
async def root():
    return {"status": "ok", "service": "WorldReaction API v2"}

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.get("/feed")
async def feed(refresh: bool = False, limit: int = 30, region: str = None):
    try:
        if region:
            region = region.upper()
            import cache
            cached = await cache.get(f"feed:{region.lower()}")
            if cached and not refresh:
                return JSONResponse(content={**cached, "cached": True})
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
        if region:
            region = region.upper()
            result["posts"] = [p for p in result["posts"] if p.get("region") == region]
            result["total"] = len(result["posts"])
        return JSONResponse(content=result)
    except Exception as e:
        log.error("search error: %s", e)
        return JSONResponse(content={"query": q, "posts": [], "total": 0, "error": str(e)})

@app.get("/trending")
async def trending(refresh: bool = False):
    """
    전세계 반응 핫 토픽 랭킹
    - 수집된 게시글 score 합산 기반
    - JP 🇯🇵 / CN 🇨🇳 / 글로벌 🌐 반응 표시
    - 15분마다 자동 업데이트
    """
    try:
        import ranking
        result = await ranking.get_ranking(force_refresh=refresh)
        return JSONResponse(content=result)
    except Exception as e:
        log.error("trending error: %s", e)
        return JSONResponse(content={"keywords": [], "total": 0, "error": str(e)})

@app.get("/region/{region}")
async def region_feed(region: str, limit: int = 30, refresh: bool = False):
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
