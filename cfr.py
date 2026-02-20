import asyncio
import math
import os
import time
import random
from bson import json_util
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Union
from urllib.parse import quote, urlparse
import aiohttp
from cachetools import TTLCache
from fastapi import HTTPException
from dotenv import load_dotenv

load_dotenv()


def _req_env(key: str) -> str:
    v = os.getenv(key)
    if not v:
        raise RuntimeError(f"Missing required env key: {key}")
    return v


X1 = _req_env("Z9_A1")
X2 = _req_env("Z9_B2")
X3 = _req_env("Z9_C3")
X4 = _req_env("Z9_D4")
X5 = _req_env("Z9_E5")
X6 = _req_env("Z9_F6")
X7 = _req_env("Z9_G7")
X8 = _req_env("Z9_H8")
X9 = _req_env("Z9_I9")
X0 = _req_env("Z9_J0")

P = int(os.getenv("PORT", "4000"))
CTT = int(os.getenv("CACHE_TTL_SECONDS", "20"))
HTM = int(os.getenv("HTTP_TIMEOUT_MS", "8000"))
DBG = os.getenv("DEBUG_API", "true").lower() in ("true", "1")

GLOBAL_JOB_TIMEOUT_MS = int(os.getenv("GLOBAL_JOB_TIMEOUT_MS", "45000"))

CS = [
    "BTC", "ETH", "SOL", "XRP", "HYPE", "DOGE", "BNB", "BCH", "SUI",
    "ADA", "LINK", "ZEC", "AVAX", "PAXG", "LTC", "UNI", "TRX", "ARB",
    "APT", "OP", "TON", "DOT",
]

AdapterResult = Dict[str, Optional[float]]
AdapterFn = Callable[[], "asyncio.Future[AdapterResult]"]

_sess: Optional[aiohttp.ClientSession] = None
_sess_lock = asyncio.Lock()


def _n(*a, **k):
    return None


d_ = _n
i_ = _n
w_ = _n
e_ = _n

_cache = TTLCache(maxsize=128, ttl=CTT)

TA = 0
TF = 0
PEC: Dict[str, int] = {}
FURLS: List[str] = []


def iec(x: str):
    PEC[x] = PEC.get(x, 0) + 1


def ton(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        n = float(v)
        if math.isnan(n):
            return None
        return n
    except (ValueError, TypeError):
        return None


def d2p(d: Union[float, str, None], digits: int = 4) -> Optional[str]:
    if d is None:
        return None
    try:
        num = float(d)
        if math.isnan(num):
            return None
        return f"{(num * 100):.{digits}f}%"
    except Exception:
        return None


def ptts(t: Any) -> float:
    if t is None:
        return 0.0
    try:
        if isinstance(t, (int, float)):
            return float(t)
        if isinstance(t, str):
            s = t.strip()
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            try:
                return datetime.fromisoformat(s).timestamp()
            except Exception:
                from email.utils import parsedate_to_datetime
                try:
                    return parsedate_to_datetime(s).timestamp()
                except Exception:
                    return 0.0
    except Exception:
        return 0.0
    return 0.0


def sln(arr: List[Dict[str, Any]], key: str, n: int = 8) -> Optional[float]:
    if not isinstance(arr, list) or len(arr) == 0:
        return None
    sample = arr[0] if arr else {}
    time_key = next((k for k in ["event_time", "fundingTime", "funding_time", "t", "timestamp"] if k in sample), None)
    take = arr
    if time_key:
        take = sorted(arr, key=lambda it: ptts(it.get(time_key)), reverse=True)[:n]
    else:
        take = arr[:n]
    s = 0.0
    found = False
    for it in take:
        v = it.get(key) if isinstance(it, dict) else None
        num = ton(v)
        if num is None:
            continue
        s += num
        found = True
    return s if found else None


def ebp(data: Any, path: Union[str, List[str], Callable]) -> Any:
    if data is None:
        return None
    if callable(path):
        try:
            return path(data)
        except Exception:
            return None
    parts = path.split(".") if isinstance(path, str) else list(path)
    cur = data
    for p in parts:
        if cur is None:
            return None
        if isinstance(cur, dict):
            cur = cur.get(p)
            continue
        if isinstance(cur, list):
            try:
                idx = int(p)
                if 0 <= idx < len(cur):
                    cur = cur[idx]
                    continue
                return None
            except Exception:
                return None
        try:
            cur = getattr(cur, p)
        except Exception:
            return None
    return cur


async def gs() -> aiohttp.ClientSession:
    global _sess
    async with _sess_lock:
        if _sess is None or _sess.closed:
            total_read = max(1.0, HTM / 1000.0)
            connect_timeout = min(3.0, total_read)  # keep connect short (enterprise)
            timeout = aiohttp.ClientTimeout(total=total_read, connect=connect_timeout, sock_read=total_read)
            connector = aiohttp.TCPConnector(limit=100, ttl_dns_cache=300, enable_cleanup_closed=True)
            headers = {"User-Agent": "Funding-Normalizer/2.0", "Accept": "application/json, text/plain, */*",
                       "Connection": "keep-alive"}
            _sess = aiohttp.ClientSession(connector=connector, headers=headers, timeout=timeout, trust_env=False)
        return _sess


async def cs() -> None:
    global _sess
    if _sess and not _sess.closed:
        await _sess.close()


async def fu(url: str, label: Optional[str] = None) -> Optional[Any]:
    global TA, TF
    TA += 1
    exchange = (label.split(":")[0] if label else (
        urlparse(url).hostname.split(".")[0] if urlparse(url).hostname else "unknown"))
    iec(exchange)
    attempts = 3
    backoff_base = 0.1
    for attempt in range(1, attempts + 1):
        session = await gs()
        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise Exception(f"HTTP {resp.status} :: {text[:200]}")
                return await resp.json()
        except Exception:
            if attempt >= attempts:
                TF += 1
                FURLS.append(f"{label if label else 'unknown'} -> {url}")
                return None
            jitter = random.uniform(0, 0.12)
            await asyncio.sleep(backoff_base * attempt + jitter)
            continue


async def tfv(variants: List[Dict[str, Any]]) -> Optional[float]:
    for v in variants:
        data = await fu(v["url"], v.get("label"))
        if not data:
            continue
        if callable(v.get("extract")):
            try:
                extracted = v["extract"](data)
            except Exception:
                extracted = None
        else:
            extracted = ebp(data, v.get("extract", ""))
        num = ton(extracted)
        if num is not None:
            return num
    return None


def scb(coin: str) -> List[str]:
    C = coin.upper()
    return [
        f"{C}USDT", f"{C}-USDT", f"{C}_USDT", f"{C}USDTM", f"{C}USDT_PERP",
        f"{C}-PERP", f"{C}-PERPETUAL", f"{C}PERP", f"{C}/USDT", f"{C}USD",
    ]


EX_ADP: Dict[str, AdapterFn] = {}


async def a_bin() -> AdapterResult:
    o: AdapterResult = {}

    async def f(c):
        variants = [{"label": f"binance:{sym}", "url": X1.format(symbol=quote(sym)),
                     "extract": lambda data: data[0].get("fundingRate") if isinstance(data, list) and data else None}
                    for sym in scb(c)]
        o[c] = await tfv(variants)

    await asyncio.gather(*(f(c) for c in CS))
    return o


EX_ADP["binance"] = a_bin


async def a_byb() -> AdapterResult:
    o: AdapterResult = {}

    async def f(c):
        variants = [{"label": f"bybit:{sym}", "url": X2.format(symbol=quote(sym)),
                     "extract": lambda data: (data.get("result") or {}).get("list", [{}])[0].get("fundingRate")} for sym
                    in scb(c)]
        o[c] = await tfv(variants)

    await asyncio.gather(*(f(c) for c in CS))
    return o


EX_ADP["bybit"] = a_byb


async def a_bitg() -> AdapterResult:
    o: AdapterResult = {}

    async def f(c):
        sym1 = f"{c.upper()}USDT"
        sym2 = f"{c.upper()}-USDT"
        variants = [
            {"label": f"bitget:{sym1}", "url": X3.format(symbol=quote(sym1)),
             "extract": lambda data: (data.get("data") or [{}])[0].get("fundingRate")},
            {"label": f"bitget:{sym2}", "url": X3.format(symbol=quote(sym2)),
             "extract": lambda data: (data.get("data") or [{}])[0].get("fundingRate")},
        ]
        o[c] = await tfv(variants)

    await asyncio.gather(*(f(c) for c in CS))
    return o


EX_ADP["bitget"] = a_bitg


async def a_kuc() -> AdapterResult:
    o: AdapterResult = {}

    async def f(c):
        candidates = []
        if c.upper() == "BTC":
            candidates.extend(["XBTUSDTM", ".XBTUSDTMFPI8H"])
        candidates.extend([f"{c.upper()}USDTM", f"{c.upper()}USDT"])
        variants = [{"label": f"kucoin:{sym}", "url": X4.format(symbol=quote(sym)),
                     "extract": lambda data: (data.get("data") or {}).get("nextFundingRate")} for sym in candidates]
        o[c] = await tfv(variants)

    await asyncio.gather(*(f(c) for c in CS))
    return o


EX_ADP["kucoin"] = a_kuc


async def a_gate() -> AdapterResult:
    o: AdapterResult = {}

    async def f(c):
        c1 = f"{c.upper()}_USDT"
        c2 = f"{c.upper()}USDT"
        variants = [
            {"label": f"gate:{c1}", "url": X5.format(symbol=quote(c1)),
             "extract": lambda data: data[0].get("r") if isinstance(data, list) and data else None},
            {"label": f"gate:{c2}", "url": X5.format(symbol=quote(c2)),
             "extract": lambda data: data[0].get("r") if isinstance(data, list) and data else None},
        ]
        o[c] = await tfv(variants)

    await asyncio.gather(*(f(c) for c in CS))
    return o


EX_ADP["gate_io"] = a_gate


async def a_hu() -> AdapterResult:
    o: AdapterResult = {}
    for coin in CS:
        code = f"{coin.upper()}-USDT"
        url = X6.format(symbol=quote(code))
        r = await fu(url, f"huobi:{code}")
        if r and r.get("status") == "ok" and isinstance(r.get("data"), dict):
            rate = r["data"].get("funding_rate") or r["data"].get("estimated_rate")
            o[coin] = ton(rate)
        else:
            o[coin] = None
        await asyncio.sleep(0.08)
    return o


EX_ADP["huobi"] = a_hu


async def a_coinb() -> AdapterResult:
    o: AdapterResult = {}

    async def f(c):
        insts = [f"{c.upper()}-PERP", f"{c.upper()}-PERPETUAL", f"{c.upper()}-USD-PERP"]
        variants = [{"label": f"coinbase:{inst}", "url": X7.format(symbol=quote(inst)),
                     "extract": lambda data: sln((data.get("results") or data.get("data") or []), "funding_rate", 8)}
                    for inst in insts]
        o[c] = await tfv(variants)

    await asyncio.gather(*(f(c) for c in CS))
    return o


EX_ADP["coinbase"] = a_coinb


async def a_mex() -> AdapterResult:
    o: AdapterResult = {}

    async def f(c):
        c1 = f"{c.upper()}_USDT"
        c2 = f"{c.upper()}USDT"
        variants = [
            {"label": f"mexc:{c1}", "url": X8.format(symbol=quote(c1)),
             "extract": lambda data: (data.get("data") or {}).get("fundingRate")},
            {"label": f"mexc:{c2}", "url": X8.format(symbol=quote(c2)),
             "extract": lambda data: (data.get("data") or {}).get("fundingRate")},
        ]
        o[c] = await tfv(variants)

    await asyncio.gather(*(f(c) for c in CS))
    return o


EX_ADP["mexc"] = a_mex


async def a_okx() -> AdapterResult:
    o: AdapterResult = {}

    async def f(c):
        inst_id = f"{c.upper()}-USD-SWAP"
        url = X9.format(symbol=quote(inst_id))
        r = await fu(url, f"okx_funding:{inst_id}")
        if r and r.get("code") == "0" and isinstance(r.get("data"), list) and r["data"]:
            o[c] = ton(r["data"][0].get("fundingRate"))
        else:
            o[c] = None

    await asyncio.gather(*(f(c) for c in CS))
    return o


EX_ADP["okex"] = a_okx


async def a_binx() -> AdapterResult:
    o: AdapterResult = {}

    async def f(c):
        c1 = f"{c.upper()}-USDT"
        c2 = f"{c.upper()}USDT"
        variants = [
            {"label": f"bingx:{c1}", "url": X0.format(symbol=quote(c1)),
             "extract": lambda data: (data.get("data") or [{}])[0].get("fundingRate")},
            {"label": f"bingx:{c2}", "url": X0.format(symbol=quote(c2)),
             "extract": lambda data: (data.get("data") or [{}])[0].get("fundingRate")},
        ]
        o[c] = await tfv(variants)

    await asyncio.gather(*(f(c) for c in CS))
    return o


EX_ADP["bingx"] = a_binx

EX_KEYS = list(EX_ADP.keys())


async def ga(normalize_cache_key: str = "funding:normalized:allcoins_v2"):
    cached = _cache.get(normalize_cache_key)
    if cached:
        return {"ok": True, "source": "cache", "data": cached}

    global TA, TF
    TA = 0
    TF = 0
    PEC.clear()
    FURLS.clear()

    async def run_adapter(key: str):
        try:
            res = await EX_ADP[key]()
            return {"key": key, "result": res}
        except Exception:
            return {"key": key, "result": {c: None for c in CS}}

    start = time.time()

    tasks_map: Dict[asyncio.Task, str] = {}
    for k in EX_KEYS:
        t = asyncio.create_task(run_adapter(k))
        tasks_map[t] = k

    done, pending = await asyncio.wait(tasks_map.keys(), timeout=GLOBAL_JOB_TIMEOUT_MS / 1000.0)

    results: List[Dict[str, Any]] = []
    for t in done:
        try:
            results.append(t.result())
        except Exception:
            key = tasks_map.get(t, "unknown")
            results.append({"key": key, "result": {c: None for c in CS}})

    for t in pending:
        key = tasks_map.get(t, "unknown")
        try:
            t.cancel()
        except Exception:
            pass
        results.append({"key": key, "result": {c: None for c in CS}})

    duration_ms = (time.time() - start) * 1000.0

    # normalization (same logic as before)
    normalized: Dict[str, Dict[str, Optional[str]]] = {}
    for item in results:
        key = item["key"]
        r = item.get("result") or {}
        out_obj: Dict[str, Optional[str]] = {}
        for coin in CS:
            v = r.get(coin)
            out_obj[coin] = d2p(v) if v is not None and not (isinstance(v, float) and math.isnan(v)) else None
        normalized[key] = out_obj

    btc_values: List[float] = []
    for item in results:
        btc_val = item.get("result", {}).get("BTC")
        if btc_val is not None:
            try:
                if not math.isnan(float(btc_val)):
                    btc_values.append(float(btc_val))
            except Exception:
                continue
    btc_overall: Optional[str] = None
    if btc_values:
        avg = sum(btc_values) / len(btc_values)
        btc_overall = d2p(avg)
    normalized["btc_overall"] = {"BTC": btc_overall}

    null_counts = {coin: 0 for coin in CS}
    for ex in normalized:
        for coin in CS:
            if normalized[ex].get(coin) is None:
                null_counts[coin] += 1
    fully_missing = [c for c, cnt in null_counts.items() if cnt == len(EX_KEYS)]

    _cache[normalize_cache_key] = normalized
    return {
        "ok": True,
        "source": "live",
        "data": normalized,
        "fetchedAt": datetime.now().isoformat(),
        "diagnostics": {"fullyMissing": fully_missing, "durationMs": round(duration_ms, 2), "totalAttempts": TA,
                        "totalFailures": TF, "perExchangeCount": PEC}
    }


async def frr():
    try:
        return await ga()
    except Exception:
        raise HTTPException(status_code=500, detail={"ok": False, "error": "internal_error"})


def hlth():
    return {"ok": True, "ts": int(time.time() * 1000)}


def rfcoll(collection, data):
    collection.delete_many({})
    result = collection.insert_one(data)
    return result.inserted_id


if __name__ == "__main__":
    try:
        out = asyncio.run(ga())
        try:
            from pymongo import MongoClient

            URI = os.getenv("URI")
            NAME = os.getenv("NAME")
            client = MongoClient(URI)
            db = client[NAME]
            collection = db["fr"]
            try:
                inserted_id = rfcoll(collection, out)
            except Exception:
                pass
        except Exception:
            pass
        finally:
            try:
                client.close()
            except Exception:
                pass
        print(json_util.dumps(out))
    finally:
        try:
            asyncio.run(cs())
        except Exception:
            pass
