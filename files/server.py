# server.py — WorldReaction v2 API 서버
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

@app.get("/")
async def root():
    return {"status": "ok", "service": "WorldReaction API v2"}

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.get("/feed")
async def feed(refresh: bool = False, limit: int = 30):
    try:
        import pipeline
        result = await pipeline.get_feed(force_refresh=refresh, limit=limit)
        return JSONResponse(content=result)
    except Exception as e:
        log.error("feed error: %s", e)
        return JSONResponse(content={"posts": [], "total": 0, "error": str(e)})

@app.get("/search")
async def search(q: str = Query(...), refresh: bool = False):
    try:
        import pipeline
        result = await pipeline.search(query=q, force_refresh=refresh)
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
            {"rank": 1, "word": "BTS", "count": "2.4만", "badge": "hot"},
            {"rank": 2, "word": "트럼프 관세", "count": "1.8만", "badge": "hot"},
            {"rank": 3, "word": "삼성 AI", "count": "1.2만", "badge": "up"},
            {"rank": 4, "word": "손흥민", "count": "9.8천", "badge": ""},
            {"rank": 5, "word": "일본 지진", "count": "8.1천", "badge": "new"},
            {"rank": 6, "word": "넷플릭스", "count": "6.3천", "badge": ""},
            {"rank": 7, "word": "AI 반도체", "count": "5.1천", "badge": "up"},
            {"rank": 8, "word": "갤럭시 S25", "count": "4.2천", "badge": ""},
            {"rank": 9, "word": "엔비디아", "count": "3.8천", "badge": "new"},
            {"rank": 10, "word": "테슬라", "count": "2.9천", "badge": ""},
        ]
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
