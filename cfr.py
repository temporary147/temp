#!/usr/bin/env python3
from __future__ import annotations
import asyncio, logging, math, os, random, time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Union
from urllib.parse import quote, urlparse
from zoneinfo import ZoneInfo

import aiohttp
import libsql
from cachetools import TTLCache
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
_logger = logging.getLogger("fnorm")


# --- env util ---------------------------------------------------------------
def _g(k: str, required: bool = True, default: Optional[str] = None) -> str:
    v = os.getenv(k, default)
    if required and (v is None or v == ""):
        raise RuntimeError(f"Missing required env var: {k}")
    return v or ""


# secret endpoints/templates (keeps same names as original)
_E0, _E1, _E2, _E3, _E4, _E5, _E6, _E7, _E8, _E9, _E10, _E11 = (
    _g(x) for x in ("Z9_A1", "Z9_B2", "Z9_C3", "Z9_D4", "Z9_E5", "Z9_F6",
                    "Z9_G7", "Z9_H8", "Z9_I9", "Z9_J0", "Z9_K1", "Z9_K2")
)

DT_URL = _g("DBT_URL")
DT_KEY = _g("DBT_KEY")
CACHE_TTL = int(_g("CACHE_TTL_SECONDS", required=False, default="20"))
HTTP_TIMEOUT = int(_g("HTTP_TIMEOUT_MS", required=False, default="15000"))
DEBUG_API = _g("DEBUG_API", required=False, default="true").lower() in ("true", "1")
JOB_TIMEOUT = int(_g("GLOBAL_JOB_TIMEOUT_MS", required=False, default="200000"))
CACHE_KEY = "funding:normalized:allcoins_v2"

# --- constants -------------------------------------------------------------
C: List[str] = [
    "BTC", "ETH", "XRP", "BNB", "SOL", "TRX", "DOGE", "BCH", "ADA", "HYPE", "XMR",
    "LINK", "XLM", "HBAR", "ZEC", "LTC", "AVAX", "SUI", "TON", "UNI", "DOT",
    "PAXG", "XAUT", "ARB", "APT"
]

_TS_KEYS = ("event_time", "fundingTime", "funding_time", "t", "timestamp")
_RETRIES = 3
_cache = TTLCache(maxsize=128, ttl=CACHE_TTL)

# --- session management ----------------------------------------------------
_session: Optional[aiohttp.ClientSession] = None
_session_lock = asyncio.Lock()


async def session() -> aiohttp.ClientSession:
    global _session
    async with _session_lock:
        if not _session or _session.closed:
            t = max(1.0, HTTP_TIMEOUT / 1000)
            _session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=100, ttl_dns_cache=300, enable_cleanup_closed=True),
                timeout=aiohttp.ClientTimeout(total=t, connect=min(3.0, t), sock_read=t),
                trust_env=False,
                headers={"User-Agent": "Funding-Normalizer/2.0", "Accept": "application/json, text/plain, */*",
                         "Connection": "keep-alive"},
            )
    return _session


async def close_session() -> None:
    global _session
    if _session and not _session.closed:
        await _session.close()


# --- telemetry simple ------------------------------------------------------
@dataclass
class Tel:
    attempts: int = 0
    failures: int = 0
    per_exchange: Dict[str, int] = None
    failed_urls: List[str] = None

    def __post_init__(self):
        self.per_exchange = {}
        self.failed_urls = []

    def reset(self):
        self.attempts = 0
        self.failures = 0
        self.per_exchange.clear()
        self.failed_urls.clear()

    def record_attempt(self, e: str):
        self.attempts += 1
        self.per_exchange[e] = self.per_exchange.get(e, 0) + 1

    def record_failure(self, label: str, url: str):
        self.failures += 1
        self.failed_urls.append(f"{label} -> {url}")


_tel = Tel()


# --- helpers ---------------------------------------------------------------
def _n(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        n = float(v)
        return None if math.isnan(n) else n
    except Exception:
        return None


def _p(d: Union[float, str, None], digits: int = 4) -> Optional[str]:
    if d is None:
        return None
    try:
        n = float(d)
        return None if math.isnan(n) else f"{(n * 100):.{digits}f}%"
    except Exception:
        return None


def _t(t: Any) -> float:
    if t is None:
        return 0.0
    try:
        if isinstance(t, (int, float)):
            return float(t)
        if isinstance(t, str):
            s = t.strip().rstrip("Z")
            if t.strip().endswith("Z"):
                s += "+00:00"
            try:
                return datetime.fromisoformat(s).timestamp()
            except Exception:
                from email.utils import parsedate_to_datetime
                try:
                    return parsedate_to_datetime(s).timestamp()
                except Exception:
                    return 0.0
    except Exception:
        pass
    return 0.0


def _sfn(arr: List[Dict[str, Any]], key: str, n: int = 8) -> Optional[float]:
    if not isinstance(arr, list) or not arr:
        return None
    ts_key = next((k for k in _TS_KEYS if k in arr[0]), None)
    take = sorted(arr, key=lambda x: _t(x.get(ts_key)), reverse=True)[:n] if ts_key else arr[:n]
    vals = [v for it in take if isinstance(it, dict) for v in [_n(it.get(key))] if v is not None]
    return sum(vals) if vals else None


def _e(data: Any, path: Union[str, List[str], Callable]) -> Any:
    if data is None:
        return None
    if callable(path):
        try:
            return path(data)
        except Exception:
            return None
    cur = data
    for p in (path.split(".") if isinstance(path, str) else list(path)):
        if cur is None:
            return None
        if isinstance(cur, dict):
            cur = cur.get(p)
            continue
        if isinstance(cur, list):
            try:
                i = int(p)
                cur = cur[i] if 0 <= i < len(cur) else None
                continue
            except Exception:
                return None
        try:
            cur = getattr(cur, p)
        except Exception:
            return None
    return cur


# --- robust fetch with retries & telemetry ---------------------------------
async def _fetch(url: str, label: Optional[str] = None) -> Optional[Any]:
    exchange = (label or "").split(":")[0] or (urlparse(url).hostname or "unknown").split(".")[0]
    _tel.record_attempt(exchange)
    for attempt in range(1, _RETRIES + 1):
        try:
            sess = await session()
            async with sess.get(url) as resp:
                if resp.status != 200:
                    raise aiohttp.ClientResponseError(resp.request_info, resp.history, status=resp.status)
                return await resp.json()
        except Exception:
            if attempt >= _RETRIES:
                _tel.record_failure(label or "unknown", url)
                return None
            await asyncio.sleep(0.08 * attempt + random.uniform(0, 0.12))


# --- small helpers for adapters --------------------------------------------
async def _tryv(variants: List[Dict[str, Any]]) -> Optional[float]:
    for v in variants:
        data = await _fetch(v["url"], v.get("label"))
        if not data:
            continue
        extracted = v["extract"](data) if callable(v.get("extract")) else _e(data, v.get("extract", ""))
        num = _n(extracted)
        if num is not None:
            return num
    return None


# --- adapter registry & factories -----------------------------------------
AdapterFn = Callable[[], "asyncio.Future[Dict[str, Optional[float]]]"]
_AD: Dict[str, AdapterFn] = {}


def reg(name: str):
    def deco(fn: AdapterFn):
        _AD[name] = fn
        return fn

    return deco


def bulk_factory(env_tpl: str, label_prefix: str):
    async def inner() -> Dict[str, Optional[float]]:
        instruments = ",".join(f"{c}-USD-INVERSE-PERPETUAL" for c in C)
        r = await _fetch(env_tpl.format(instruments=quote(instruments)), f"{label_prefix}:bulk")
        if not r or "Data" not in r:
            return {c: None for c in C}
        d = r["Data"]
        return {c: _n((d.get(f"{c}-USD-INVERSE-PERPETUAL") or {}).get("VALUE")) for c in C}

    return inner


# register bulk-style ones:
_AD["binance"] = bulk_factory(_E10, "binance")
_AD["bybit"] = bulk_factory(_E11, "bybit")


# specific adapters (kept behaviour same, but compact)
@reg("bitget")
async def _a_bitget() -> Dict[str, Optional[float]]:
    async def f(c):
        return {c: await _tryv([
            {"label": f"bitget:{s}", "url": _E2.format(symbol=quote(s)),
             "extract": lambda d: (d.get("data") or [{}])[0].get("fundingRate")}
            for s in (f"{c}USDT", f"{c}-USDT")
        ])}

    res = await asyncio.gather(*[f(c) for c in C])
    return {k: v for r in res for k, v in r.items()}


@reg("kucoin")
async def _a_kucoin() -> Dict[str, Optional[float]]:
    async def f(c):
        syms = (["XBTUSDTM", ".XBTUSDTMFPI8H"] if c == "BTC" else []) + [f"{c}USDTM", f"{c}USDT"]
        return {c: await _tryv([{"label": f"kucoin:{s}", "url": _E3.format(symbol=quote(s)),
                                 "extract": lambda d: (d.get("data") or {}).get("nextFundingRate")} for s in syms])}

    res = await asyncio.gather(*[f(c) for c in C])
    return {k: v for r in res for k, v in r.items()}


@reg("gate_io")
async def _a_gate() -> Dict[str, Optional[float]]:
    async def f(c):
        return {c: await _tryv([{"label": f"gate:{s}", "url": _E4.format(symbol=quote(s)),
                                 "extract": lambda d: d[0].get("r") if isinstance(d, list) and d else None} for s in
                                (f"{c}_USDT", f"{c}USDT")])}

    res = await asyncio.gather(*[f(c) for c in C])
    return {k: v for r in res for k, v in r.items()}


@reg("huobi")
async def _a_huobi() -> Dict[str, Optional[float]]:
    out: Dict[str, Optional[float]] = {}
    for coin in C:
        code = f"{coin}-USDT"
        r = await _fetch(_E5.format(symbol=quote(code)), f"huobi:{code}")
        if r and r.get("status") == "ok" and isinstance(r.get("data"), dict):
            out[coin] = _n(r["data"].get("funding_rate") or r["data"].get("estimated_rate"))
        else:
            out[coin] = None
        await asyncio.sleep(0.08)
    return out


@reg("coinbase")
async def _a_coinbase() -> Dict[str, Optional[float]]:
    async def f(c):
        return {c: await _tryv([{"label": f"coinbase:{inst}", "url": _E6.format(symbol=quote(inst)),
                                 "extract": lambda d: _sfn(d.get("results") or d.get("data") or [], "funding_rate", 8)}
                                for inst in (f"{c}-PERP", f"{c}-PERPETUAL", f"{c}-USD-PERP")])}

    res = await asyncio.gather(*[f(c) for c in C])
    return {k: v for r in res for k, v in r.items()}


@reg("mexc")
async def _a_mexc() -> Dict[str, Optional[float]]:
    async def f(c):
        return {c: await _tryv([{"label": f"mexc:{s}", "url": _E7.format(symbol=quote(s)),
                                 "extract": lambda d: (d.get("data") or {}).get("fundingRate")} for s in
                                (f"{c}_USDT", f"{c}USDT")])}

    res = await asyncio.gather(*[f(c) for c in C])
    return {k: v for r in res for k, v in r.items()}


@reg("okex")
async def _a_okx() -> Dict[str, Optional[float]]:
    async def f(c):
        inst = f"{c}-USD-SWAP"
        r = await _fetch(_E8.format(symbol=quote(inst)), f"okx_funding:{inst}")
        return {c: _n(r["data"][0].get("fundingRate")) if r and r.get("code") == "0" and r.get("data") else None}

    res = await asyncio.gather(*[f(c) for c in C])
    return {k: v for r in res for k, v in r.items()}


@reg("bingx")
async def _a_bingx() -> Dict[str, Optional[float]]:
    async def f(c):
        return {c: await _tryv([{"label": f"bingx:{s}", "url": _E9.format(symbol=quote(s)),
                                 "extract": lambda d: (d.get("data") or [{}])[0].get("fundingRate")} for s in
                                (f"{c}-USDT", f"{c}USDT")])}

    res = await asyncio.gather(*[f(c) for c in C])
    return {k: v for r in res for k, v in r.items()}


_AD_KEYS = list(_AD.keys())


# --- run all adapters and normalize ----------------------------------------
async def run_all_adapters(cache_key: str = CACHE_KEY) -> Dict[str, Any]:
    cached = _cache.get(cache_key)
    if cached:
        return {"ok": True, "source": "cache", "data": cached}
    _tel.reset()

    async def _run(k: str) -> Dict[str, Any]:
        try:
            return {"key": k, "result": await _AD[k]()}
        except Exception:
            return {"key": k, "result": {c: None for c in C}}

    start = time.time()
    tasks = [asyncio.create_task(_run(k)) for k in _AD_KEYS]
    done, pending = await asyncio.wait(tasks, timeout=JOB_TIMEOUT / 1000)
    results: List[Dict[str, Any]] = []
    for t in done:
        try:
            results.append(t.result())
        except Exception:
            results.append({"key": "unknown", "result": {c: None for c in C}})
    for t in pending:
        t.cancel()
        results.append({"key": getattr(t, "name", "unknown"), "result": {c: None for c in C}})
    duration_ms = (time.time() - start) * 1000

    normalized: Dict[str, Dict[str, Optional[str]]] = {
        item["key"]: {
            coin: _p(v) if v is not None and not (isinstance(v, float) and math.isnan(v)) else None
            for coin, v in ((c, (item.get("result") or {}).get(c)) for c in C)
        }
        for item in results
    }

    btc_vals = [
        float(item["result"]["BTC"]) for item in results
        if item.get("result", {}).get("BTC") is not None and not math.isnan(float(item["result"]["BTC"]))
    ]
    normalized["btc_overall"] = {"BTC": _p(sum(btc_vals) / len(btc_vals)) if btc_vals else None}

    null_counts = {c: sum(1 for ex in normalized if normalized[ex].get(c) is None) for c in C}
    fully_missing = [c for c, cnt in null_counts.items() if cnt == len(_AD_KEYS)]

    ist_now = datetime.now(ZoneInfo("Asia/Kolkata"))
    _cache[cache_key] = normalized

    return {
        "ok": True, "source": "live", "data": normalized,
        "fetchedAt": ist_now.strftime("%d-%m-%Y %I:%M:%S %p IST"),
        "diagnostics": {
            "fullyMissing": fully_missing,
            "durationMs": round(duration_ms, 2),
            "totalAttempts": _tel.attempts,
            "totalFailures": _tel.failures,
            "perExchangeCount": dict(_tel.per_exchange),
        },
    }


# --- DB snapshot writer (keeps same DDL/behavior) ---------------------------
_DDL = [
    """CREATE TABLE IF NOT EXISTS rate
    (
        id
        TEXT
        PRIMARY
        KEY
        DEFAULT (
        lower (
        hex(
        randomblob
       (
        16
       )))),
        name TEXT NOT NULL,
        symbol TEXT NOT NULL,
        rate TEXT,
        version INTEGER NOT NULL DEFAULT 1,
        updated_at TEXT NOT NULL DEFAULT
       (
           datetime
       (
           'now'
       )),
        UNIQUE
       (
           name,
           symbol
       )
        ) STRICT;""",
    "CREATE INDEX IF NOT EXISTS idx_rate_name   ON rate(name);",
    "CREATE INDEX IF NOT EXISTS idx_rate_symbol ON rate(symbol);",
    """CREATE TRIGGER IF NOT EXISTS trg_rate_updated_at
       AFTER
    UPDATE ON rate FOR EACH ROW
        WHEN OLD.updated_at = NEW.updated_at
    BEGIN
    UPDATE rate
    SET updated_at = NEW.updated_at,
        version    = OLD.version + 1
    WHERE id = OLD.id;
    END;"""
]


def write_snapshot(data_url: str, data_token: str, snapshot: Dict[str, Any]) -> int:
    conn = libsql.connect(data_url, auth_token=data_token)
    cur = conn.cursor()
    for ddl in _DDL:
        cur.execute(ddl)
    normalized = snapshot.get("data") or {}
    ist_now = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")
    cur.execute("BEGIN")
    try:
        inserted = 0
        for exchange, coin_map in normalized.items():
            if not isinstance(coin_map, dict):
                continue
            for symbol, pct_val in coin_map.items():
                cur.execute("DELETE FROM rate WHERE name = ? AND symbol = ?", (exchange, symbol))
                cur.execute("INSERT INTO rate (name, symbol, rate, updated_at) VALUES (?, ?, ?, ?)",
                            (exchange, symbol, pct_val, ist_now))
                inserted += 1
        conn.commit()
        return inserted
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# --- main ------------------------------------------------------------------
if __name__ == "__main__":
    try:
        snapshot = asyncio.run(run_all_adapters())
        if not snapshot:
            raise RuntimeError("No output from run_all_adapters()")
        count = write_snapshot(DT_URL, DT_KEY, snapshot)
        _logger.info("Successfully Executed: %d", count)
    except Exception as exc:
        _logger.exception("Fatal error: %s", exc)
        raise
    finally:
        try:
            asyncio.run(close_session())
        except Exception:
            pass
