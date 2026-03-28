# cache.py — Upstash REST API
import os, json, logging
import urllib.request, urllib.parse
log = logging.getLogger("wr.cache")
TTL_DEFAULT = 600

def _cfg():
    url   = os.environ.get("UPSTASH_REDIS_REST_URL", "").strip()
    token = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "").strip()  # \n 제거
    if url and not url.startswith("http"):
        url = "https://" + url
    return url, token

def _req(path, method="GET", body=None):
    url, token = _cfg()
    if not url or not token: return None
    req = urllib.request.Request(
        url.rstrip("/") + path,
        data=json.dumps(body).encode() if body else None,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method=method
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return json.loads(r.read())
    except Exception as e:
        log.warning("cache error: %s", e)
        return None

async def get(key):
    try:
        r = _req(f"/get/{urllib.parse.quote(key, safe='')}")
        return json.loads(r["result"]) if r and r.get("result") else None
    except Exception: return None

async def set(key, value, ttl=TTL_DEFAULT):
    try:
        _req(f"/setex/{urllib.parse.quote(key, safe='')}/{ttl}", "POST", json.dumps(value, ensure_ascii=False))
    except Exception: pass
