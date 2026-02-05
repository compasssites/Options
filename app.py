#!/usr/bin/env python3
import csv
import io
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import FileResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles

CONFIG_PATH = os.getenv("CONFIG_PATH", "config.json")
APP_TOKEN = os.getenv("APP_TOKEN", "").strip()
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "600"))
MARKETWATCH_TTL_SECONDS = int(os.getenv("MARKETWATCH_TTL_SECONDS", str(CACHE_TTL_SECONDS)))
DEFAULT_STRIKE_STEP = float(os.getenv("DEFAULT_STRIKE_STEP", "5000"))
NSE_DEFAULT_STRIKE = int(os.getenv("NSE_DEFAULT_STRIKE", "26500"))
TE_API_KEY = os.getenv("TE_API_KEY", "").strip()
TE_CACHE_TTL_SECONDS = int(os.getenv("TE_CACHE_TTL_SECONDS", "60"))
TE_COMMODITIES_URL = "https://api.tradingeconomics.com/markets/commodities"
METALS_PROVIDER = os.getenv("METALS_PROVIDER", "auto").strip().lower()
METALS_API_KEY = os.getenv("METALS_API_KEY", "").strip()
METALS_API_BASE = os.getenv("METALS_API_BASE", "USD").strip().upper()
METALS_API_CACHE_TTL_SECONDS = int(os.getenv("METALS_API_CACHE_TTL_SECONDS", "300"))
METALS_API_LATEST_URL = "https://metals-api.com/api/latest"
METALS_API_TIMESERIES_URL = "https://metals-api.com/api/timeseries"

IST = timezone(timedelta(hours=5, minutes=30))

OUTPUT_COLUMNS = [
    "CE_OpenInterest",
    "CE_ChangeInOI",
    "CE_Volume",
    "CE_AbsoluteChange",
    "CE_BidQty",
    "CE_BidPrice",
    "CE_AskPrice",
    "CE_AskQty",
    "CE_LTP",
    "CE_PrevClose",
    "CE_PctChange",
    "CE_StrikePrice",
    "PE_LTP",
    "PE_PrevClose",
    "PE_PctChange",
    "PE_BidQty",
    "PE_BidPrice",
    "PE_AskPrice",
    "PE_AskQty",
    "PE_AbsoluteChange",
    "PE_Volume",
    "PE_ChangeInOI",
    "PE_OpenInterest",
]

OUTPUT_HEADERS = [
    "CALL_OI_Lots",
    "CALL_Chng_in_OI",
    "CALL_Volume",
    "CALL_Abs_Chng",
    "CALL_Bid_Qty",
    "CALL_Bid_Price",
    "CALL_Ask_Price",
    "CALL_Ask_Qty",
    "CALL_LTP",
    "CALL_Prev_Close",
    "CALL_Pct_Chng",
    "Strike_Price",
    "PUT_LTP",
    "PUT_Prev_Close",
    "PUT_Pct_Chng",
    "PUT_Bid_Qty",
    "PUT_Bid_Price",
    "PUT_Ask_Price",
    "PUT_Ask_Qty",
    "PUT_Abs_Chng",
    "PUT_Volume",
    "PUT_Chng_in_OI",
    "PUT_OI_Lots",
]

LITE_HEADERS = [
    "strike",
    "ce_ltp",
    "ce_bid",
    "ce_ask",
    "ce_oi",
    "ce_volume",
    "pe_ltp",
    "pe_bid",
    "pe_ask",
    "pe_oi",
    "pe_volume",
]

LITE_MAP = {
    "strike": "Strike_Price",
    "ce_ltp": "CALL_LTP",
    "ce_bid": "CALL_Bid_Price",
    "ce_ask": "CALL_Ask_Price",
    "ce_oi": "CALL_OI_Lots",
    "ce_volume": "CALL_Volume",
    "pe_ltp": "PUT_LTP",
    "pe_bid": "PUT_Bid_Price",
    "pe_ask": "PUT_Ask_Price",
    "pe_oi": "PUT_OI_Lots",
    "pe_volume": "PUT_Volume",
}

_DATE_RE = re.compile(r"^/Date\(([-+]?\d+)([+-]\d{4})?\)/$")

CACHE: Dict[str, Dict[str, Any]] = {}
MARKETWATCH_CACHE: Dict[str, Any] = {}
NSE_CACHE: Dict[str, Any] = {}
TE_CACHE: Dict[str, Any] = {}
METALS_CACHE: Dict[str, Any] = {}
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15"
)


app = FastAPI(title="Option Chain Hub", version="0.1.0")
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", include_in_schema=False)
def index() -> FileResponse:
    return FileResponse("static/index.html")


@app.get("/manifest.webmanifest", include_in_schema=False)
def manifest() -> FileResponse:
    return FileResponse("static/manifest.webmanifest", media_type="application/manifest+json")


@app.get("/sw.js", include_in_schema=False)
def service_worker() -> FileResponse:
    return FileResponse("static/sw.js", media_type="application/javascript")


@app.get("/api/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/api/ticker")
def ticker(token: Optional[str] = None, x_api_token: Optional[str] = Header(None)) -> Dict[str, Any]:
    check_token(token, x_api_token)
    provider = METALS_PROVIDER or "auto"

    if provider in ("auto", "metals_api", "metalsapi"):
        if METALS_API_KEY:
            items, fetched_at = get_metals_api_cached()
            if items:
                last_updated = datetime.fromtimestamp(fetched_at, tz=IST).isoformat(sep=" ", timespec="seconds")
                return {"source": "metals_api", "last_updated": last_updated, "items": items}
            if provider != "auto":
                raise HTTPException(status_code=502, detail="Metals API returned no items")
        elif provider != "auto":
            raise HTTPException(status_code=503, detail="METALS_API_KEY not set")

    if provider in ("auto", "tradingeconomics", "te"):
        if not TE_API_KEY:
            if provider == "auto":
                raise HTTPException(status_code=503, detail="No ticker API key configured")
            raise HTTPException(status_code=503, detail="TE_API_KEY not set")

        rows, fetched_at = get_te_commodities_cached()
        items = filter_te_metals(rows)
        if not items:
            raise HTTPException(status_code=502, detail="TradingEconomics returned no gold/silver rows")
        last_updated = datetime.fromtimestamp(fetched_at, tz=IST).isoformat(sep=" ", timespec="seconds")
        return {"source": "tradingeconomics", "last_updated": last_updated, "items": items}

    raise HTTPException(status_code=400, detail="Unknown ticker provider")


@app.get("/api/symbols")
def symbols(token: Optional[str] = None, x_api_token: Optional[str] = Header(None)) -> Dict[str, Any]:
    check_token(token, x_api_token)
    config = load_config()
    symbols_cfg = config.get("symbols", {})
    symbols_list = sorted(symbols_cfg.keys())
    sources = {symbol: (cfg.get("source", "mcx") if isinstance(cfg, dict) else "mcx") for symbol, cfg in symbols_cfg.items()}
    return {"symbols": symbols_list, "sources": sources}


@app.get("/api/expiries")
def expiries(
    symbol: str,
    token: Optional[str] = None,
    x_api_token: Optional[str] = Header(None),
) -> Dict[str, Any]:
    check_token(token, x_api_token)
    config = load_config()
    symbol = symbol.upper().strip()
    symbol_cfg = config.get("symbols", {}).get(symbol)
    if not symbol_cfg:
        raise HTTPException(status_code=404, detail="Unknown symbol")
    source = symbol_cfg.get("source", "mcx")
    expiries = []
    if source == "mcx":
        expiries = get_mcx_expiries(symbol)
    elif source == "nse":
        expiries = get_nse_expiries(symbol)
    if not expiries:
        expiries = symbol_cfg.get("expiries", [])
    return {"symbol": symbol, "expiries": expiries}


@app.get("/api/option-chain")
def option_chain(
    symbol: str,
    expiry: Optional[str] = None,
    format: str = "json",
    strike_step: Optional[float] = Query(None, ge=0),
    all_strikes: bool = False,
    force: bool = False,
    refresh: bool = False,
    download: bool = False,
    pretty: bool = False,
    as_text: bool = False,
    limit: Optional[int] = Query(None, ge=1),
    offset: Optional[int] = Query(None, ge=0),
    mode: Optional[str] = None,
    window: Optional[int] = Query(None, ge=0),
    lite: bool = False,
    token: Optional[str] = None,
    x_api_token: Optional[str] = Header(None),
) -> Response:
    check_token(token, x_api_token)

    if refresh:
        force = True

    symbol = symbol.upper().strip()
    config = load_config()
    symbol_cfg = config.get("symbols", {}).get(symbol)
    if not symbol_cfg:
        raise HTTPException(status_code=404, detail="Unknown symbol")

    source = symbol_cfg.get("source", "mcx")
    if source not in ("mcx", "nse"):
        raise HTTPException(status_code=501, detail="Source not implemented")

    if not expiry:
        if source == "mcx":
            expiry_list = get_mcx_expiries(symbol)
        elif source == "nse":
            expiry_list = get_nse_expiries(symbol)
        else:
            expiry_list = []
        if not expiry_list:
            expiry_list = symbol_cfg.get("expiries", [])
        expiry = expiry_list[0] if expiry_list else None

    step = DEFAULT_STRIKE_STEP if strike_step is None else strike_step

    rows, fetched_at = get_cached_rows(
        source=source,
        symbol=symbol,
        expiry=expiry,
        strike_step=step,
        all_strikes=all_strikes,
        force=force,
    )

    last_updated = datetime.fromtimestamp(fetched_at, tz=IST).isoformat(sep=" ", timespec="seconds")
    server_ts = datetime.now(tz=IST).isoformat(sep=" ", timespec="seconds")
    age_ms = int((time.time() - fetched_at) * 1000)
    underlying = get_underlying_value(rows)

    rows = sort_rows_by_strike(rows)
    if mode is None and window is not None:
        mode = "atm_window"
    if mode == "atm_window":
        rows = filter_atm_window(rows, underlying, window or 0)
    rows = apply_offset_limit(rows, offset, limit)

    output_rows = to_output_rows(rows)
    rows_payload = to_lite_rows(output_rows) if lite else output_rows

    if format.lower() == "csv":
        headers = LITE_HEADERS if lite else OUTPUT_HEADERS
        csv_text = to_csv(rows_payload, headers)
        filename = f"{symbol}_{expiry or 'LATEST'}_option_chain.csv"
        disposition = "attachment" if download else "inline"
        return Response(
            content=csv_text,
            media_type="text/csv",
            headers={
                "Content-Disposition": f"{disposition}; filename={filename}",
                "X-Last-Updated": last_updated,
            },
        )

    payload = {
        "symbol": symbol,
        "expiry": expiry,
        "last_updated": last_updated,
        "server_ts": server_ts,
        "source_ts": last_updated,
        "age_ms": age_ms,
        "underlying": underlying,
        "count": len(rows_payload),
        "rows": rows_payload,
    }

    if format.lower() == "ndjson":
        def ndjson_stream():
            meta = {k: payload[k] for k in payload if k != "rows"}
            yield json.dumps(meta, ensure_ascii=False) + "\n"
            for row in rows_payload:
                yield json.dumps(row, ensure_ascii=False) + "\n"

        return StreamingResponse(
            ndjson_stream(),
            media_type="application/x-ndjson; charset=utf-8",
            headers={"Content-Disposition": "inline", "Cache-Control": "no-store"},
        )

    if format.lower() == "lines":
        lines = [json.dumps({k: payload[k] for k in payload if k != "rows"}, ensure_ascii=False)]
        for row in rows_payload:
            lines.append(json.dumps(row, ensure_ascii=False))
        content = "\n".join(f"L{idx:04d} {line}" for idx, line in enumerate(lines, 1)) + "\n"
        return Response(
            content=content,
            media_type="text/plain; charset=utf-8",
            headers={"Content-Disposition": "inline", "Cache-Control": "no-store"},
        )

    if as_text or format.lower() in ("text", "prettytext", "plain"):
        json_text = json.dumps(payload, ensure_ascii=False, indent=2)
        return Response(
            content=json_text,
            media_type="text/plain; charset=utf-8",
            headers={"Cache-Control": "no-store"},
        )

    json_text = json.dumps(payload, ensure_ascii=False, indent=2 if pretty else None)
    return Response(content=json_text, media_type="application/json")


@app.get("/api/option-chain-lite")
def option_chain_lite(
    symbol: str,
    expiry: Optional[str] = None,
    format: str = "json",
    strike_step: Optional[float] = Query(None, ge=0),
    all_strikes: bool = False,
    force: bool = False,
    download: bool = False,
    pretty: bool = False,
    limit: Optional[int] = Query(None, ge=1),
    offset: Optional[int] = Query(None, ge=0),
    mode: Optional[str] = None,
    window: Optional[int] = Query(None, ge=0),
    token: Optional[str] = None,
    x_api_token: Optional[str] = Header(None),
) -> Response:
    return option_chain(
        symbol=symbol,
        expiry=expiry,
        format=format,
        strike_step=strike_step,
        all_strikes=all_strikes,
        force=force,
        download=download,
        pretty=pretty,
        limit=limit,
        offset=offset,
        mode=mode,
        window=window,
        lite=True,
        token=token,
        x_api_token=x_api_token,
    )


@app.get("/api/option-chain-pretty")
def option_chain_pretty(
    symbol: str,
    expiry: Optional[str] = None,
    format: str = "json",
    strike_step: Optional[float] = Query(None, ge=0),
    all_strikes: bool = False,
    force: bool = False,
    download: bool = False,
    limit: Optional[int] = Query(None, ge=1),
    offset: Optional[int] = Query(None, ge=0),
    mode: Optional[str] = None,
    window: Optional[int] = Query(None, ge=0),
    token: Optional[str] = None,
    x_api_token: Optional[str] = Header(None),
) -> Response:
    return option_chain(
        symbol=symbol,
        expiry=expiry,
        format=format,
        strike_step=strike_step,
        all_strikes=all_strikes,
        force=force,
        download=download,
        pretty=True,
        limit=limit,
        offset=offset,
        mode=mode,
        window=window,
        lite=False,
        token=token,
        x_api_token=x_api_token,
    )


@app.get("/api/option-chain-chat")
def option_chain_chat(
    symbol: str,
    expiry: Optional[str] = None,
    format: str = "ndjson",
    strike_step: Optional[float] = Query(None, ge=0),
    all_strikes: bool = False,
    force: bool = False,
    download: bool = False,
    limit: Optional[int] = Query(None, ge=1),
    offset: Optional[int] = Query(None, ge=0),
    window: int = Query(60, ge=0),
    token: Optional[str] = None,
    x_api_token: Optional[str] = Header(None),
) -> Response:
    return option_chain(
        symbol=symbol,
        expiry=expiry,
        format=format,
        strike_step=strike_step,
        all_strikes=all_strikes,
        force=force,
        download=download,
        pretty=True,
        limit=limit,
        offset=offset,
        mode="atm_window",
        window=window,
        lite=True,
        token=token,
        x_api_token=x_api_token,
    )


@app.post("/api/refresh")
def refresh(
    symbol: str,
    expiry: Optional[str] = None,
    strike_step: Optional[float] = Query(None, ge=0),
    all_strikes: bool = False,
    token: Optional[str] = None,
    x_api_token: Optional[str] = Header(None),
) -> Dict[str, Any]:
    check_token(token, x_api_token)

    symbol = symbol.upper().strip()
    config = load_config()
    symbol_cfg = config.get("symbols", {}).get(symbol)
    if not symbol_cfg:
        raise HTTPException(status_code=404, detail="Unknown symbol")

    source = symbol_cfg.get("source", "mcx")
    if source not in ("mcx", "nse"):
        raise HTTPException(status_code=501, detail="Source not implemented")

    if not expiry:
        if source == "mcx":
            expiry_list = get_mcx_expiries(symbol)
        elif source == "nse":
            expiry_list = get_nse_expiries(symbol)
        else:
            expiry_list = []
        if not expiry_list:
            expiry_list = symbol_cfg.get("expiries", [])
        expiry = expiry_list[0] if expiry_list else None

    step = DEFAULT_STRIKE_STEP if strike_step is None else strike_step

    rows, fetched_at = get_cached_rows(
        source=source,
        symbol=symbol,
        expiry=expiry,
        strike_step=step,
        all_strikes=all_strikes,
        force=True,
    )

    last_updated = datetime.fromtimestamp(fetched_at, tz=IST).isoformat(sep=" ", timespec="seconds")
    return {"symbol": symbol, "expiry": expiry, "last_updated": last_updated, "count": len(rows)}


def check_token(token: Optional[str], header_token: Optional[str]) -> None:
    if not APP_TOKEN:
        return
    supplied = (token or "") or (header_token or "")
    if supplied != APP_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid token")


def load_config() -> Dict[str, Any]:
    path = Path(CONFIG_PATH)
    if not path.exists():
        return {"symbols": {}}
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except json.JSONDecodeError:
        return {"symbols": {}}


def get_cached_rows(
    source: str,
    symbol: str,
    expiry: Optional[str],
    strike_step: float,
    all_strikes: bool,
    force: bool,
) -> Tuple[List[Dict[str, Any]], float]:
    cache_key = f"{source}:{symbol}:{expiry or ''}:{strike_step}:{all_strikes}"
    now = time.time()

    if not force:
        entry = CACHE.get(cache_key)
        if entry and now - entry["fetched_at"] < CACHE_TTL_SECONDS:
            return entry["rows"], entry["fetched_at"]

    if source == "mcx":
        rows = fetch_mcx_option_chain(symbol, expiry)
        rows = normalize_rows(rows)
    elif source == "nse":
        rows = fetch_nse_option_chain(symbol, expiry, force=force)
    else:
        rows = []
    rows = add_derived_fields(rows)
    if not all_strikes:
        rows = [row for row in rows if is_round_strike(row.get("CE_StrikePrice"), strike_step)]

    CACHE[cache_key] = {"rows": rows, "fetched_at": now}
    return rows, now


def fetch_mcx_option_chain(symbol: str, expiry: Optional[str]) -> List[Dict[str, Any]]:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "Accept-Language": "en-GB,en;q=0.9",
            "Referer": "https://www.mcxindia.com/market-data/option-chain",
            "Origin": "https://www.mcxindia.com",
        }
    )

    session.get("https://www.mcxindia.com/market-data/option-chain", timeout=20)
    payload = {"Commodity": symbol, "Expiry": expiry}

    resp = session.post(
        "https://www.mcxindia.com/backpage.aspx/GetOptionChain",
        json=payload,
        timeout=20,
    )
    try:
        resp.raise_for_status()
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else None
        if status in (401, 403):
            fallback_rows = fetch_mcx_option_chain_from_marketwatch(symbol, expiry)
            if fallback_rows:
                return fallback_rows
            raise HTTPException(
                status_code=502,
                detail="MCX option chain blocked (403). MarketWatch fallback returned no rows.",
            ) from exc
        raise

    data: Any = resp.json()
    if isinstance(data, dict) and "d" in data:
        data = data["d"]
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError:
            pass

    return extract_rows(data)


def fetch_mcx_option_chain_from_marketwatch(symbol: str, expiry: Optional[str]) -> List[Dict[str, Any]]:
    rows = get_marketwatch_rows()
    symbol = symbol.upper().strip()
    expiry = expiry or None

    chain: Dict[float, Dict[str, Any]] = {}
    for item in rows:
        if not isinstance(item, dict):
            continue
        if item.get("Symbol") != symbol and item.get("ProductCode") != symbol:
            continue
        if expiry and str(item.get("ExpiryDate")) != str(expiry):
            continue
        opt_type = (item.get("OptionType") or "").upper()
        if opt_type not in ("CE", "PE"):
            continue
        strike = item.get("StrikePrice")
        if strike is None:
            continue
        try:
            strike_key = float(strike)
        except (TypeError, ValueError):
            continue
        entry = chain.setdefault(strike_key, {"CE_StrikePrice": strike})
        prefix = "CE_" if opt_type == "CE" else "PE_"
        underlying_val = item.get("UnderlineValue", item.get("UnderlyingValue", ""))
        if underlying_val not in ("", None):
            entry["UnderlyingValue"] = underlying_val
        entry[f"{prefix}OpenInterest"] = item.get("OpenInterest", "")
        entry[f"{prefix}ChangeInOI"] = item.get("ChangeInOI", item.get("ChangeInOpenInterest", ""))
        entry[f"{prefix}Volume"] = item.get("Volume", "")
        entry[f"{prefix}AbsoluteChange"] = item.get("AbsoluteChange", "")
        entry[f"{prefix}BidQty"] = item.get("BuyQuantity", "")
        entry[f"{prefix}BidPrice"] = item.get("BuyPrice", "")
        entry[f"{prefix}AskPrice"] = item.get("SellPrice", "")
        entry[f"{prefix}AskQty"] = item.get("SellQuantity", "")
        entry[f"{prefix}LTP"] = item.get("LTP", "")

    sorted_strikes = sorted(chain.keys())
    return [chain[strike] for strike in sorted_strikes]


def get_underlying_value(rows: List[Dict[str, Any]]) -> Optional[float]:
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key in ("UnderlyingValue", "UnderlineValue", "underlyingValue", "UnderlineValue"):
            val = row.get(key)
            if val not in ("", None):
                try:
                    return float(val)
                except (TypeError, ValueError):
                    continue
    return None


def sort_rows_by_strike(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def strike_key(item: Dict[str, Any]) -> float:
        val = item.get("CE_StrikePrice") if isinstance(item, dict) else None
        try:
            return float(val)
        except (TypeError, ValueError):
            return float("inf")

    return sorted(rows, key=strike_key)


def filter_atm_window(rows: List[Dict[str, Any]], underlying: Optional[float], window: int) -> List[Dict[str, Any]]:
    if underlying is None or not rows:
        return rows

    strikes = []
    for row in rows:
        val = row.get("CE_StrikePrice") if isinstance(row, dict) else None
        try:
            strikes.append(float(val))
        except (TypeError, ValueError):
            strikes.append(float("inf"))

    if not strikes:
        return rows

    closest_idx = min(range(len(strikes)), key=lambda i: abs(strikes[i] - underlying))
    start = max(0, closest_idx - window)
    end = min(len(rows), closest_idx + window + 1)
    return rows[start:end]


def apply_offset_limit(rows: List[Dict[str, Any]], offset: Optional[int], limit: Optional[int]) -> List[Dict[str, Any]]:
    start = offset or 0
    if start < 0:
        start = 0
    if limit is None:
        return rows[start:]
    return rows[start : start + limit]


def fetch_nse_option_chain(symbol: str, expiry: Optional[str], force: bool = False) -> List[Dict[str, Any]]:
    payload = get_nse_payload(symbol, expiry=expiry, force=force)
    data = payload.get("data") if isinstance(payload, dict) else []
    if not isinstance(data, list):
        return []

    expiry_norm = normalize_nse_expiry(expiry) if expiry else None
    rows = []
    for item in data:
        if not isinstance(item, dict):
            continue
        expiry_value = item.get("expiryDates")
        if expiry_norm and normalize_nse_expiry(expiry_value) != expiry_norm:
            continue
        rows.append(nse_record_to_row(item))
    return rows


def get_nse_payload(symbol: str, expiry: Optional[str] = None, force: bool = False) -> Dict[str, Any]:
    symbol = symbol.upper().strip()
    expiry_key = normalize_nse_expiry(expiry) if expiry else "EXPIRIES"
    cache_key = f"{symbol}:{expiry_key}"
    now = time.time()
    cached = NSE_CACHE.get(cache_key)
    if not force and cached and now - cached.get("fetched_at", 0) < CACHE_TTL_SECONDS:
        return cached.get("payload", {})
    payload = fetch_nse_option_chain_raw(symbol, expiry=expiry)
    NSE_CACHE[cache_key] = {"payload": payload, "fetched_at": now}
    return payload


def fetch_nse_option_chain_raw(symbol: str, expiry: Optional[str] = None) -> Dict[str, Any]:
    session = requests.Session()
    if symbol == "NIFTY":
        referer = "https://www.nseindia.com/get-quote/optionchain/NIFTY/NIFTY-50"
    else:
        referer = "https://www.nseindia.com/option-chain"
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "*/*",
            "Accept-Language": "en-GB,en;q=0.9",
            "Referer": referer,
            "Origin": "https://www.nseindia.com",
        }
    )

    session.get("https://www.nseindia.com", timeout=20)
    session.get(referer, timeout=20)

    if expiry:
        expiry_query = normalize_nse_expiry(expiry)
        params_value = f"expiryDate={expiry_query}"
    else:
        params_value = f"strikePrice={NSE_DEFAULT_STRIKE}"

    resp = session.get(
        "https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi",
        params={
            "functionName": "getOptionChainData",
            "symbol": symbol,
            "params": params_value,
        },
        timeout=20,
    )
    resp.raise_for_status()
    return resp.json()


def get_nse_expiries(symbol: str) -> List[str]:
    payload = get_nse_payload(symbol)
    data = payload.get("data") if isinstance(payload, dict) else []
    expiries: Dict[datetime, str] = {}
    for item in data or []:
        if not isinstance(item, dict):
            continue
        value = item.get("expiryDates")
        if not value:
            continue
        parsed = parse_nse_expiry(value)
        if not parsed:
            continue
        expiries[parsed] = format_nse_expiry(parsed)
    sorted_expiries = [expiries[key] for key in sorted(expiries.keys())]
    return sorted_expiries[:7]


def parse_nse_expiry(value: str) -> Optional[datetime]:
    if not value:
        return None
    value = str(value).strip()
    if re.match(r"^\d{2}-\d{2}-\d{4}$", value):
        day, month, year = value.split("-")
        return datetime(int(year), int(month), int(day))
    for fmt in ("%d-%b-%Y", "%d-%B-%Y"):
        try:
            return datetime.strptime(value.title(), fmt)
        except ValueError:
            continue
    return None


def format_nse_expiry(dt: datetime) -> str:
    return dt.strftime("%d-%b-%Y")


def normalize_nse_expiry(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    parsed = parse_nse_expiry(value)
    if parsed:
        return format_nse_expiry(parsed)
    return str(value)


def nse_record_to_row(item: Dict[str, Any]) -> Dict[str, Any]:
    ce = item.get("CE") or {}
    pe = item.get("PE") or {}

    return {
        "CE_OpenInterest": nse_get(ce, "openInterest"),
        "CE_ChangeInOI": nse_get(ce, "changeinOpenInterest", "changeInOpenInterest"),
        "CE_Volume": nse_get(ce, "totalTradedVolume"),
        "CE_AbsoluteChange": nse_get(ce, "change"),
        "CE_BidQty": nse_get(ce, "buyQuantity1", "bidQty", "totalBuyQuantity"),
        "CE_BidPrice": nse_get(ce, "buyPrice1", "bidprice", "bidPrice"),
        "CE_AskPrice": nse_get(ce, "sellPrice1", "askPrice", "askprice"),
        "CE_AskQty": nse_get(ce, "sellQuantity1", "askQty", "totalSellQuantity"),
        "CE_LTP": nse_get(ce, "lastPrice", "ltp"),
        "CE_StrikePrice": item.get("strikePrice", ""),
        "PE_LTP": nse_get(pe, "lastPrice", "ltp"),
        "PE_BidQty": nse_get(pe, "buyQuantity1", "bidQty", "totalBuyQuantity"),
        "PE_BidPrice": nse_get(pe, "buyPrice1", "bidprice", "bidPrice"),
        "PE_AskPrice": nse_get(pe, "sellPrice1", "askPrice", "askprice"),
        "PE_AskQty": nse_get(pe, "sellQuantity1", "askQty", "totalSellQuantity"),
        "PE_AbsoluteChange": nse_get(pe, "change"),
        "PE_Volume": nse_get(pe, "totalTradedVolume"),
        "PE_ChangeInOI": nse_get(pe, "changeinOpenInterest", "changeInOpenInterest"),
        "PE_OpenInterest": nse_get(pe, "openInterest"),
        "UnderlyingValue": nse_get(ce, "underlyingValue", "underlyingValue") or nse_get(pe, "underlyingValue"),
    }


def nse_get(side: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in side:
            return side.get(key)
    return ""


def fetch_mcx_marketwatch() -> List[Dict[str, Any]]:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/json; charset=utf-8",
            "Referer": "https://www.mcxindia.com/market-data/option-chain",
            "Origin": "https://www.mcxindia.com",
        }
    )

    session.get("https://www.mcxindia.com/market-data/option-chain", timeout=20)
    resp = session.post(
        "https://www.mcxindia.com/backpage.aspx/GetMarketWatch",
        data=json.dumps({}),
        timeout=20,
    )
    resp.raise_for_status()
    payload: Any = resp.json()
    if isinstance(payload, dict) and "d" in payload:
        payload = payload["d"]
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            return []
    if isinstance(payload, dict):
        data = payload.get("Data")
        if isinstance(data, list):
            return data
    return []


def get_marketwatch_rows() -> List[Dict[str, Any]]:
    now = time.time()
    cached = MARKETWATCH_CACHE.get("rows")
    fetched_at = MARKETWATCH_CACHE.get("fetched_at", 0)
    if cached and now - fetched_at < MARKETWATCH_TTL_SECONDS:
        return cached
    rows = fetch_mcx_marketwatch()
    MARKETWATCH_CACHE["rows"] = rows
    MARKETWATCH_CACHE["fetched_at"] = now
    return rows


def fetch_te_commodities() -> List[Dict[str, Any]]:
    resp = requests.get(
        TE_COMMODITIES_URL,
        params={"c": TE_API_KEY},
        timeout=20,
    )
    resp.raise_for_status()
    payload: Any = resp.json()
    if isinstance(payload, list):
        return payload
    return []


def get_te_commodities_cached() -> Tuple[List[Dict[str, Any]], float]:
    now = time.time()
    cached = TE_CACHE.get("rows")
    fetched_at = TE_CACHE.get("fetched_at", 0)
    if cached and now - fetched_at < TE_CACHE_TTL_SECONDS:
        return cached, fetched_at
    rows = fetch_te_commodities()
    TE_CACHE["rows"] = rows
    TE_CACHE["fetched_at"] = now
    return rows, now


def filter_te_metals(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = str(row.get("Name", "")).strip()
        symbol = str(row.get("Symbol", "")).strip()
        key = f"{name} {symbol}".lower()
        if not any(tag in key for tag in ("gold", "silver", "xau", "xag")):
            continue
        items.append(
            {
                "name": name,
                "symbol": symbol,
                "last": row.get("Last", ""),
                "change": row.get("DailyChange", row.get("Change", "")),
                "change_pct": row.get("DailyPercentualChange", row.get("PercentualChange", "")),
                "unit": row.get("unit", row.get("Unit", "")),
                "last_update": row.get("LastUpdate", row.get("Date", "")),
            }
        )
    items.sort(key=lambda item: item.get("name", ""))
    return items


def fetch_metals_api_latest() -> Dict[str, Any]:
    resp = requests.get(
        METALS_API_LATEST_URL,
        params={
            "access_key": METALS_API_KEY,
            "base": METALS_API_BASE,
            "symbols": "XAU,XAG",
        },
        timeout=20,
    )
    resp.raise_for_status()
    payload: Any = resp.json()
    if isinstance(payload, dict) and payload.get("success") is False:
        error_info = payload.get("error", {})
        message = error_info.get("info") or error_info.get("type") or "Metals API error"
        raise HTTPException(status_code=502, detail=str(message))
    return payload if isinstance(payload, dict) else {}


def fetch_metals_api_timeseries(symbol: str, start_date: str, end_date: str) -> Dict[str, Any]:
    resp = requests.get(
        METALS_API_TIMESERIES_URL,
        params={
            "access_key": METALS_API_KEY,
            "base": METALS_API_BASE,
            "symbols": symbol,
            "start_date": start_date,
            "end_date": end_date,
        },
        timeout=20,
    )
    resp.raise_for_status()
    payload: Any = resp.json()
    if isinstance(payload, dict) and payload.get("success") is False:
        return {}
    return payload if isinstance(payload, dict) else {}


def build_metals_api_items(
    latest_payload: Dict[str, Any],
    prev_rates: Dict[str, Any],
) -> List[Dict[str, Any]]:
    rates = latest_payload.get("rates") if isinstance(latest_payload, dict) else {}
    if not isinstance(rates, dict):
        rates = {}

    def price_from_rate(rate: Optional[float]) -> Optional[float]:
        if rate in (None, 0):
            return None
        return 1.0 / rate

    items: List[Dict[str, Any]] = []
    for symbol, name in (("XAU", "Gold"), ("XAG", "Silver")):
        rate = to_float(rates.get(symbol))
        last_price = price_from_rate(rate)
        prev_rate = to_float(prev_rates.get(symbol))
        prev_price = price_from_rate(prev_rate)
        change = None
        change_pct = None
        if last_price is not None and prev_price not in (None, 0):
            change = last_price - prev_price
            change_pct = (change / prev_price) * 100
        items.append(
            {
                "name": name,
                "symbol": symbol,
                "last": format_number(last_price, 2),
                "change": format_number(change, 2),
                "change_pct": format_number(change_pct, 2),
                "unit": f"{METALS_API_BASE}/oz",
                "last_update": latest_payload.get("date", ""),
            }
        )
    return items


def get_metals_api_cached() -> Tuple[List[Dict[str, Any]], float]:
    now = time.time()
    cached = METALS_CACHE.get("items")
    fetched_at = METALS_CACHE.get("fetched_at", 0)
    if cached and now - fetched_at < METALS_API_CACHE_TTL_SECONDS:
        return cached, fetched_at

    latest_payload = fetch_metals_api_latest()
    today = datetime.utcnow().date()
    end_date = (today - timedelta(days=1)).isoformat()
    start_date = end_date
    prev_rates: Dict[str, Any] = {}
    for symbol in ("XAU", "XAG"):
        timeseries_payload = fetch_metals_api_timeseries(symbol, start_date, end_date)
        if not isinstance(timeseries_payload, dict):
            continue
        series = timeseries_payload.get("rates")
        if not isinstance(series, dict):
            continue
        if end_date in series and isinstance(series[end_date], dict):
            prev_rates[symbol] = series[end_date].get(symbol)
    items = build_metals_api_items(latest_payload, prev_rates)
    METALS_CACHE["items"] = items
    METALS_CACHE["fetched_at"] = now
    return items, now


def parse_expiry_date(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.strptime(value.upper(), "%d%b%Y")
    except ValueError:
        return None


def get_mcx_expiries(symbol: str) -> List[str]:
    rows = get_marketwatch_rows()
    symbol = symbol.upper().strip()
    today = datetime.now(tz=IST).date()
    expiries: Dict[datetime, str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        if row.get("Symbol") != symbol and row.get("ProductCode") != symbol:
            continue
        instrument = (row.get("InstrumentName") or "").upper()
        if instrument and instrument not in ("OPTFUT", "OPTIDX"):
            continue
        expiry_value = row.get("ExpiryDate")
        if not expiry_value:
            continue
        expiry_dt = parse_expiry_date(str(expiry_value))
        if not expiry_dt:
            continue
        if expiry_dt.date() < today:
            continue
        expiries[expiry_dt] = expiry_value
    sorted_expiries = [expiries[key] for key in sorted(expiries.keys())]
    limit = 4 if symbol in {"SILVERM", "GOLDM"} else 3
    return sorted_expiries[:limit]


def extract_rows(data: Any) -> List[Dict[str, Any]]:
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("Data", "data", "Table", "Table1", "table"):
            value = data.get(key)
            if isinstance(value, list):
                return value
        for value in data.values():
            if isinstance(value, list):
                return value
    return []


def normalize_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        new_row: Dict[str, Any] = {}
        for key, val in row.items():
            if val is None:
                val = ""
            if isinstance(val, str):
                val = val.strip()
                val = parse_dotnet_date(val)
            new_row[key] = val
        normalized.append(new_row)
    return normalized


def parse_dotnet_date(value: str) -> str:
    match = _DATE_RE.match(value)
    if not match:
        return value

    ms = int(match.group(1))
    if ms <= 0:
        return ""

    dt_utc = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    offset_str = match.group(2)
    if offset_str:
        sign = 1 if offset_str[0] == "+" else -1
        hours = int(offset_str[1:3])
        minutes = int(offset_str[3:5])
        offset = timedelta(minutes=sign * (hours * 60 + minutes))
        tz = timezone(offset)
        dt_local = dt_utc.astimezone(tz)
    else:
        dt_local = dt_utc.astimezone(IST)

    return dt_local.isoformat(sep=" ", timespec="seconds")


def to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None
        cleaned = cleaned.replace(",", "")
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def format_number(value: Optional[float], decimals: int = 2) -> Any:
    if value is None:
        return ""
    return round(value, decimals)


def get_prev_close_value(row: Dict[str, Any], prefix: str) -> Optional[float]:
    for key in (
        f"{prefix}_PrevClose",
        f"{prefix}_PreviousClose",
        f"{prefix}_PrevClosePrice",
        f"{prefix}_PreviousClosePrice",
    ):
        val = row.get(key)
        parsed = to_float(val)
        if parsed is not None:
            return parsed
    return None


def add_change_fields(row: Dict[str, Any], prefix: str) -> None:
    ltp = to_float(row.get(f"{prefix}_LTP"))
    abs_chg = to_float(row.get(f"{prefix}_AbsoluteChange"))
    prev_close = get_prev_close_value(row, prefix)

    if prev_close is None and ltp is not None and abs_chg is not None:
        prev_close = ltp - abs_chg

    pct_change = None
    if prev_close not in (None, 0):
        if abs_chg is not None:
            pct_change = (abs_chg / prev_close) * 100
        elif ltp is not None:
            pct_change = ((ltp - prev_close) / prev_close) * 100

    row[f"{prefix}_PrevClose"] = format_number(prev_close, 2)
    row[f"{prefix}_PctChange"] = format_number(pct_change, 2)


def add_derived_fields(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    for row in rows:
        if not isinstance(row, dict):
            continue
        add_change_fields(row, "CE")
        add_change_fields(row, "PE")
    return rows


def is_round_strike(value: Any, step: float) -> bool:
    try:
        strike = float(value)
    except (TypeError, ValueError):
        return False
    if step <= 0:
        return True
    quotient = strike / step
    return abs(quotient - round(quotient)) < 1e-6


def to_output_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    output_rows: List[Dict[str, Any]] = []
    for row in rows:
        trimmed = {key: row.get(key, "") for key in OUTPUT_COLUMNS}
        output_rows.append(
            {OUTPUT_HEADERS[i]: trimmed.get(OUTPUT_COLUMNS[i], "") for i in range(len(OUTPUT_COLUMNS))}
        )
    return output_rows


def to_lite_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    lite_rows: List[Dict[str, Any]] = []
    for row in rows:
        lite_rows.append({key: row.get(src, "") for key, src in LITE_MAP.items()})
    return lite_rows


def to_csv(rows: List[Dict[str, Any]], headers: List[str]) -> str:
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(headers)
    for row in rows:
        writer.writerow([row.get(header, "") for header in headers])
    return buffer.getvalue()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=False)
