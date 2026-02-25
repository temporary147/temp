#!/usr/bin/env python3
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
import certifi
from pymongo import MongoClient
from pymongo.errors import PyMongoError, DuplicateKeyError
from pymongo.server_api import ServerApi
import logging

load_dotenv()


def a1(key: str) -> str:
    v = os.getenv(key)
    if not v:
        raise RuntimeError(f"Missing required env key: {key}")
    return v


E0 = a1("Z9_A1")
E1 = a1("Z9_B2")
E2 = a1("Z9_C3")
E3 = a1("Z9_D4")
E4 = a1("Z9_E5")
E5 = a1("Z9_F6")
E6 = a1("Z9_G7")
E7 = a1("Z9_H8")
E8 = a1("Z9_I9")
E9 = a1("Z9_J0")
E10 = a1("Z9_K1")
E11 = a1("Z9_K2")
p0 = int(os.getenv("PORT", "4000"))
c_ttl = int(os.getenv("CACHE_TTL_SECONDS", "20"))
h_tm = int(os.getenv("HTTP_TIMEOUT_MS", "8000"))
dbg_flag = os.getenv("DEBUG_API", "true").lower() in ("true", "1")
GJT = int(os.getenv("GLOBAL_JOB_TIMEOUT_MS", "45000"))
COINS = [
    "BTC",
    "ETH",
    "SOL",
    "XRP",
    "HYPE",
    "DOGE",
    "BNB",
    "BCH",
    "SUI",
    "ADA",
    "LINK",
    "ZEC",
    "AVAX",
    "PAXG",
    "LTC",
    "UNI",
    "TRX",
    "ARB",
    "APT",
    "OP",
    "TON",
    "DOT",
]
AR = Dict[str, Optional[float]]
AF = Callable[[], "asyncio.Future[AR]"]
sess_obj: Optional[aiohttp.ClientSession] = None
sess_lock = asyncio.Lock()


def null_func(*a, **k):
    return None


dX = null_func
iX = null_func
wX = null_func
eX = null_func
cache_obj = TTLCache(maxsize=128, ttl=c_ttl)
tot_attempts = 0
tot_failures = 0
per_ex_count: Dict[str, int] = {}
failed_urls: List[str] = []


def inc_ex_count(x: str):
    per_ex_count[x] = per_ex_count.get(x, 0) + 1


def to_num(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        n = float(v)
        if math.isnan(n):
            return None
        return n
    except (ValueError, TypeError):
        return None


def dec_to_pct(d: Union[float, str, None], digits: int = 4) -> Optional[str]:
    if d is None:
        return None
    try:
        num = float(d)
        if math.isnan(num):
            return None
        return f"{(num * 100):.{digits}f}%"
    except Exception:
        return None


def parse_ts_to_sec(t: Any) -> float:
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


def sfn(arr: List[Dict[str, Any]], key: str, n: int = 8) -> Optional[float]:
    if not isinstance(arr, list) or len(arr) == 0:
        return None
    sample = arr[0] if arr else {}
    time_key = next((k for k in ["event_time", "fundingTime", "funding_time", "t", "timestamp"] if k in sample), None)
    take = arr
    if time_key:
        take = sorted(arr, key=lambda it: parse_ts_to_sec(it.get(time_key)), reverse=True)[:n]
    else:
        take = arr[:n]
    s = 0.0
    found = False
    for it in take:
        v = it.get(key) if isinstance(it, dict) else None
        num = to_num(v)
        if num is None:
            continue
        s += num
        found = True
    return s if found else None


def epp(data: Any, path: Union[str, List[str], Callable]) -> Any:
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


async def get_session() -> aiohttp.ClientSession:
    global sess_obj
    async with sess_lock:
        if sess_obj is None or sess_obj.closed:
            total_read = max(1.0, h_tm / 1000.0)
            connect_timeout = min(3.0, total_read)
            timeout = aiohttp.ClientTimeout(total=total_read, connect=connect_timeout, sock_read=total_read)
            connector = aiohttp.TCPConnector(limit=100, ttl_dns_cache=300, enable_cleanup_closed=True)
            headers = {"User-Agent": "Funding-Normalizer/2.0", "Accept": "application/json, text/plain, */*",
                       "Connection": "keep-alive"}
            sess_obj = aiohttp.ClientSession(connector=connector, headers=headers, timeout=timeout, trust_env=False)
    return sess_obj


async def close_session() -> None:
    global sess_obj
    if sess_obj and not sess_obj.closed:
        await sess_obj.close()


async def fetch_url(url: str, label: Optional[str] = None) -> Optional[Any]:
    global tot_attempts, tot_failures
    tot_attempts += 1
    exchange = (label.split(":")[0] if label else (
        urlparse(url).hostname.split(".")[0] if urlparse(url).hostname else "unknown"))
    inc_ex_count(exchange)
    attempts = 3
    backoff_base = 0.1
    for attempt in range(1, attempts + 1):
        session = await get_session()
        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise Exception(f"HTTP {resp.status} :: {text[:200]}")
                return await resp.json()
        except Exception:
            if attempt >= attempts:
                tot_failures += 1
                failed_urls.append(f"{label if label else 'unknown'} -> {url}")
                return None
            jitter = random.uniform(0, 0.12)
            await asyncio.sleep(backoff_base * attempt + jitter)
            continue


async def try_variants(variants: List[Dict[str, Any]]) -> Optional[float]:
    for v in variants:
        data = await fetch_url(v["url"], v.get("label"))
        if not data:
            continue
        if callable(v.get("extract")):
            try:
                extracted = v["extract"](data)
            except Exception:
                extracted = None
        else:
            extracted = epp(data, v.get("extract", ""))
        num = to_num(extracted)
        if num is not None:
            return num
    return None


def symbol_combinations(coin: str) -> List[str]:
    C = coin.upper()
    return [
        f"{C}USDT",
        f"{C}-USDT",
        f"{C}_USDT",
        f"{C}USDTM",
        f"{C}USDT_PERP",
        f"{C}-PERP",
        f"{C}-PERPETUAL",
        f"{C}PERP",
        f"{C}/USDT",
        f"{C}USD",
    ]


ADAPTERS: Dict[str, AF] = {}


async def bce() -> AR:
    o: AR = {}
    instruments = ",".join([f"{coin}-USD-INVERSE-PERPETUAL" for coin in COINS])
    url = E10.format(instruments=quote(instruments))
    r = await fetch_url(url, "binance2:bulk")
    if not r or "Data" not in r:
        return {c: None for c in COINS}
    data = r.get("Data", {})
    for coin in COINS:
        inst_key = f"{coin}-USD-INVERSE-PERPETUAL"
        entry = data.get(inst_key)
        if entry:
            o[coin] = to_num(entry.get("VALUE"))
        else:
            o[coin] = None
    return o


ADAPTERS["binance2"] = bce


async def bbt2() -> AR:
    o: AR = {}
    instruments = ",".join([f"{coin}-USD-INVERSE-PERPETUAL" for coin in COINS])
    url = E11.format(instruments=quote(instruments))
    r = await fetch_url(url, "bybit2:bulk")
    if not r or "Data" not in r:
        return {c: None for c in COINS}
    data = r.get("Data", {})
    for coin in COINS:
        inst_key = f"{coin}-USD-INVERSE-PERPETUAL"
        entry = data.get(inst_key)
        if entry:
            o[coin] = to_num(entry.get("VALUE"))
        else:
            o[coin] = None
    return o


ADAPTERS["bybit2"] = bbt2


# async def ad_binance() -> AR:
#     o: AR = {}
#     async def _f(c):
#         variants = [
#             {"label": f"binance:{sym}", "url": E0.format(symbol=quote(sym)), "extract": lambda data: data[0].get("fundingRate") if isinstance(data, list) and data else None}
#             for sym in symbol_combinations(c)
#         ]
#         o[c] = await try_variants(variants)
#     await asyncio.gather(*(_f(c) for c in COINS))
#     return o
#
# ADAPTERS["binance"] = ad_binance
#
# async def ad_bybit() -> AR:
#     o: AR = {}
#     async def _f(c):
#         variants = [
#             {"label": f"bybit:{sym}", "url": E1.format(symbol=quote(sym)), "extract": lambda data: (data.get("result") or {}).get("list", [{}])[0].get("fundingRate")}
#             for sym in symbol_combinations(c)
#         ]
#         o[c] = await try_variants(variants)
#     await asyncio.gather(*(_f(c) for c in COINS))
#     return o
#
# ADAPTERS["bybit"] = ad_bybit

async def ad_bitget() -> AR:
    o: AR = {}

    async def _f(c):
        sym1 = f"{c.upper()}USDT"
        sym2 = f"{c.upper()}-USDT"
        variants = [
            {"label": f"bitget:{sym1}", "url": E2.format(symbol=quote(sym1)),
             "extract": lambda data: (data.get("data") or [{}])[0].get("fundingRate")},
            {"label": f"bitget:{sym2}", "url": E2.format(symbol=quote(sym2)),
             "extract": lambda data: (data.get("data") or [{}])[0].get("fundingRate")},
        ]
        o[c] = await try_variants(variants)

    await asyncio.gather(*(_f(c) for c in COINS))
    return o


ADAPTERS["bitget"] = ad_bitget


async def ad_kucoin() -> AR:
    o: AR = {}

    async def _f(c):
        candidates = []
        if c.upper() == "BTC":
            candidates.extend(["XBTUSDTM", ".XBTUSDTMFPI8H"])
        candidates.extend([f"{c.upper()}USDTM", f"{c.upper()}USDT"])
        variants = [
            {"label": f"kucoin:{sym}", "url": E3.format(symbol=quote(sym)),
             "extract": lambda data: (data.get("data") or {}).get("nextFundingRate")}
            for sym in candidates
        ]
        o[c] = await try_variants(variants)

    await asyncio.gather(*(_f(c) for c in COINS))
    return o


ADAPTERS["kucoin"] = ad_kucoin


async def ad_gate() -> AR:
    o: AR = {}

    async def _f(c):
        c1 = f"{c.upper()}_USDT"
        c2 = f"{c.upper()}USDT"
        variants = [
            {"label": f"gate:{c1}", "url": E4.format(symbol=quote(c1)),
             "extract": lambda data: data[0].get("r") if isinstance(data, list) and data else None},
            {"label": f"gate:{c2}", "url": E4.format(symbol=quote(c2)),
             "extract": lambda data: data[0].get("r") if isinstance(data, list) and data else None},
        ]
        o[c] = await try_variants(variants)

    await asyncio.gather(*(_f(c) for c in COINS))
    return o


ADAPTERS["gate_io"] = ad_gate


async def ad_huobi() -> AR:
    o: AR = {}
    for coin in COINS:
        code = f"{coin.upper()}-USDT"
        url = E5.format(symbol=quote(code))
        r = await fetch_url(url, f"huobi:{code}")
        if r and r.get("status") == "ok" and isinstance(r.get("data"), dict):
            rate = r["data"].get("funding_rate") or r["data"].get("estimated_rate")
            o[coin] = to_num(rate)
        else:
            o[coin] = None
        await asyncio.sleep(0.08)
    return o


ADAPTERS["huobi"] = ad_huobi


async def ad_coinbase() -> AR:
    o: AR = {}

    async def _f(c):
        insts = [f"{c.upper()}-PERP", f"{c.upper()}-PERPETUAL", f"{c.upper()}-USD-PERP"]
        variants = [
            {"label": f"coinbase:{inst}", "url": E6.format(symbol=quote(inst)),
             "extract": lambda data: sfn((data.get("results") or data.get("data") or []), "funding_rate", 8)}
            for inst in insts
        ]
        o[c] = await try_variants(variants)

    await asyncio.gather(*(_f(c) for c in COINS))
    return o


ADAPTERS["coinbase"] = ad_coinbase


async def ad_mexc() -> AR:
    o: AR = {}

    async def _f(c):
        c1 = f"{c.upper()}_USDT"
        c2 = f"{c.upper()}USDT"
        variants = [
            {"label": f"mexc:{c1}", "url": E7.format(symbol=quote(c1)),
             "extract": lambda data: (data.get("data") or {}).get("fundingRate")},
            {"label": f"mexc:{c2}", "url": E7.format(symbol=quote(c2)),
             "extract": lambda data: (data.get("data") or {}).get("fundingRate")},
        ]
        o[c] = await try_variants(variants)

    await asyncio.gather(*(_f(c) for c in COINS))
    return o


ADAPTERS["mexc"] = ad_mexc


async def ad_okx() -> AR:
    o: AR = {}

    async def _f(c):
        inst_id = f"{c.upper()}-USD-SWAP"
        url = E8.format(symbol=quote(inst_id))
        r = await fetch_url(url, f"okx_funding:{inst_id}")
        if r and r.get("code") == "0" and isinstance(r.get("data"), list) and r["data"]:
            o[c] = to_num(r["data"][0].get("fundingRate"))
        else:
            o[c] = None

    await asyncio.gather(*(_f(c) for c in COINS))
    return o


ADAPTERS["okex"] = ad_okx


async def ad_bingx() -> AR:
    o: AR = {}

    async def _f(c):
        c1 = f"{c.upper()}-USDT"
        c2 = f"{c.upper()}USDT"
        variants = [
            {"label": f"bingx:{c1}", "url": E9.format(symbol=quote(c1)),
             "extract": lambda data: (data.get("data") or [{}])[0].get("fundingRate")},
            {"label": f"bingx:{c2}", "url": E9.format(symbol=quote(c2)),
             "extract": lambda data: (data.get("data") or [{}])[0].get("fundingRate")},
        ]
        o[c] = await try_variants(variants)

    await asyncio.gather(*(_f(c) for c in COINS))
    return o


ADAPTERS["bingx"] = ad_bingx

ADAPTER_KEYS = list(ADAPTERS.keys())


async def run_all_adapters(normalize_cache_key: str = "funding:normalized:allcoins_v2"):
    cached = cache_obj.get(normalize_cache_key)
    if cached:
        return {"ok": True, "source": "cache", "data": cached}
    global tot_attempts, tot_failures
    tot_attempts = 0
    tot_failures = 0
    per_ex_count.clear()
    failed_urls.clear()

    async def _run_adapter(key: str):
        try:
            res = await ADAPTERS[key]()
            return {"key": key, "result": res}
        except Exception:
            return {"key": key, "result": {c: None for c in COINS}}

    start = time.time()
    tasks_map: Dict[asyncio.Task, str] = {}
    for k in ADAPTER_KEYS:
        t = asyncio.create_task(_run_adapter(k))
        tasks_map[t] = k
    done, pending = await asyncio.wait(tasks_map.keys(), timeout=GJT / 1000.0)
    results: List[Dict[str, Any]] = []
    for t in done:
        try:
            results.append(t.result())
        except Exception:
            key = tasks_map.get(t, "unknown")
            results.append({"key": key, "result": {c: None for c in COINS}})
    for t in pending:
        key = tasks_map.get(t, "unknown")
        try:
            t.cancel()
        except Exception:
            pass
        results.append({"key": key, "result": {c: None for c in COINS}})
    duration_ms = (time.time() - start) * 1000.0
    normalized: Dict[str, Dict[str, Optional[str]]] = {}
    for item in results:
        key = item["key"]
        r = item.get("result") or {}
        out_obj: Dict[str, Optional[str]] = {}
        for coin in COINS:
            v = r.get(coin)
            out_obj[coin] = dec_to_pct(v) if v is not None and not (isinstance(v, float) and math.isnan(v)) else None
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
        btc_overall = dec_to_pct(avg)
    normalized["btc_overall"] = {"BTC": btc_overall}
    null_counts = {coin: 0 for coin in COINS}
    for ex in normalized:
        for coin in COINS:
            if normalized[ex].get(coin) is None:
                null_counts[coin] += 1
    fully_missing = [c for c, cnt in null_counts.items() if cnt == len(ADAPTER_KEYS)]
    cache_obj[normalize_cache_key] = normalized
    return {
        "ok": True,
        "source": "live",
        "data": normalized,
        "fetchedAt": datetime.now().isoformat(),
        "diagnostics": {
            "fullyMissing": fully_missing,
            "durationMs": round(duration_ms, 2),
            "totalAttempts": tot_attempts,
            "totalFailures": tot_failures,
            "perExchangeCount": per_ex_count,
        },
    }


async def run_fetch():
    try:
        return await run_all_adapters()
    except Exception:
        raise HTTPException(status_code=500, detail={"ok": False, "error": "internal_error"})


def health_check():
    return {"ok": True, "ts": int(time.time() * 1000)}


def write_replace_collection(collection, data):
    collection.delete_many({})
    result = collection.insert_one(data)
    return result.inserted_id


def create_mongo_client(uri: str, allow_invalid_cert: bool = False, timeout_ms: int = 10000) -> MongoClient:
    kwargs = {
        "serverSelectionTimeoutMS": timeout_ms,
        "tls": True,
        "tlsCAFile": certifi.where(),
        "server_api": ServerApi("1"),
    }
    if allow_invalid_cert:
        kwargs["tlsAllowInvalidCertificates"] = True
    return MongoClient(uri, **kwargs)


def connect_mongo_with_fallbacks(uri: str) -> MongoClient:
    try:
        client = create_mongo_client(uri, allow_invalid_cert=False, timeout_ms=10000)
        client.admin.command("ping")
        return client
    except Exception as e_secure:
        msg = str(e_secure)
        if "SSL" in msg or "tls" in msg.lower() or "handshake" in msg.lower():
            try:
                client = create_mongo_client(uri, allow_invalid_cert=True, timeout_ms=20000)
                client.admin.command("ping")
                return client
            except Exception as e_insecure:
                raise e_secure from e_insecure
        raise


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logg = logging.getLogger("funding-normalizer")

if __name__ == "__main__":
    client = None
    try:
        out = asyncio.run(run_all_adapters())
        URI = os.getenv("URI")
        NAME = os.getenv("NAME")
        if not URI or not NAME:
            raise RuntimeError("Missing required environment variables for MongoDB: URI and NAME must be set.")
        try:
            client = connect_mongo_with_fallbacks(URI)
        except Exception as conn_exc:
            # logg.error("Failed to connect to MongoDB: %s", conn_exc)
            raise
        try:
            db = client[NAME]
            collection = db["fr"]
            inserted_id = write_replace_collection(collection, out)
            # logg.info("Mongo write successful, inserted id: %s", inserted_id)
        except PyMongoError as pm_err:
            # logg.exception("PyMongo error during rfcoll: %s", pm_err)
            raise
        except Exception as e:
            # logg.exception("Unexpected error during Mongo write: %s", e)
            raise
        finally:
            try:
                client.close()
            except Exception:
                pass
        print(json_util.dumps(out))
    finally:
        try:
            asyncio.run(close_session())
        except Exception:
            pass
