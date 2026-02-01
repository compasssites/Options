#!/usr/bin/env python3
import argparse
import csv
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download MCX option chain data to CSV."
    )
    parser.add_argument("--commodity", default="SILVERM", help="e.g. SILVERM")
    parser.add_argument("--expiry", default="18FEB2026", help="e.g. 18FEB2026")
    parser.add_argument("--outdir", default=".", help="Output directory for CSV files")
    parser.add_argument(
        "--outfile",
        default="option_chain.csv",
        help="CSV filename to write (overwritten each run).",
    )
    parser.add_argument(
        "--strike-step",
        type=float,
        default=5000.0,
        help="Keep only strikes in multiples of this step (default: 5000).",
    )
    parser.add_argument(
        "--all-strikes",
        action="store_true",
        help="Do not filter strikes (override --strike-step).",
    )
    return parser.parse_args()


def extract_rows(data):
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in (
            "Data",
            "data",
            "Table",
            "Table1",
            "table",
            "optionChain",
            "OptionChain",
        ):
            val = data.get(key)
            if isinstance(val, list):
                return val
        for val in data.values():
            if isinstance(val, list):
                return val
    raise ValueError("Could not locate rows in response payload.")


_DATE_RE = re.compile(r"^/Date\(([-+]?\d+)([+-]\d{4})?\)/$")


def _parse_dotnet_date(value: str, default_tz: timezone) -> str:
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
        dt_local = dt_utc.astimezone(default_tz)

    return dt_local.isoformat(sep=" ", timespec="seconds")


def normalize_rows(rows, default_tz: timezone):
    normalized = []
    for row in rows:
        if not isinstance(row, dict):
            normalized.append(row)
            continue
        new_row = {}
        for key, val in row.items():
            if val is None:
                val = ""
            if isinstance(val, str):
                val = val.strip()
                val = _parse_dotnet_date(val, default_tz)
            new_row[key] = val
        normalized.append(new_row)
    return normalized


def is_round_strike(value, step: float) -> bool:
    try:
        strike = float(value)
    except (TypeError, ValueError):
        return False
    if step <= 0:
        return True
    quotient = strike / step
    return abs(quotient - round(quotient)) < 1e-6


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
    "CE_StrikePrice",
    "PE_LTP",
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
    "Strike_Price",
    "PUT_LTP",
    "PUT_Bid_Qty",
    "PUT_Bid_Price",
    "PUT_Ask_Price",
    "PUT_Ask_Qty",
    "PUT_Abs_Chng",
    "PUT_Volume",
    "PUT_Chng_in_OI",
    "PUT_OI_Lots",
]


def main() -> None:
    args = parse_args()
    outdir = Path(args.outdir).expanduser().resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
        }
    )

    session.get("https://www.mcxindia.com/market-data/option-chain", timeout=30)

    payload = {"Commodity": args.commodity, "Expiry": args.expiry}
    resp = session.post(
        "https://www.mcxindia.com/backpage.aspx/GetOptionChain",
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()

    data = resp.json()
    if isinstance(data, dict) and "d" in data:
        if isinstance(data.get("d"), str):
            try:
                data = json.loads(data["d"])
            except json.JSONDecodeError:
                pass
        elif isinstance(data.get("d"), dict):
            data = data["d"]
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError:
            pass

    rows = extract_rows(data)
    default_tz = timezone(timedelta(hours=5, minutes=30))
    rows = normalize_rows(rows, default_tz)
    if not args.all_strikes:
        rows = [
            row
            for row in rows
            if is_round_strike(row.get("CE_StrikePrice"), args.strike_step)
        ]

    trimmed_rows = []
    for row in rows:
        trimmed_rows.append({k: row.get(k, "") for k in OUTPUT_COLUMNS})

    csv_path = outdir / args.outfile
    if trimmed_rows:
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(OUTPUT_HEADERS)
            for row in trimmed_rows:
                writer.writerow([row.get(k, "") for k in OUTPUT_COLUMNS])
    else:
        csv_path.write_text("", encoding="utf-8")
    print(f"Saved CSV: {csv_path}")


if __name__ == "__main__":
    main()
