#!/usr/bin/env python3
"""
Daily Relay → Dune loader (relay_requests ONLY, hard-coded chain names)
- Computes "yesterday" in Europe/Amsterdam unless a --date YYYY-MM-DD is provided
- Fetches Relay requests for REFERRER
- Flattens to the agreed schema (with in/out chain names from hard-coded map)
- Filters rows to the given date (local day window)
- Inserts CSV into Dune table via CSV endpoint

Env:
  REFERRER        (default: zkp2p.xyz)
  DUNE_API_KEY    (required in CI)
  DUNE_NAMESPACE  (default: unhappyben)
  DUNE_TABLE      (default: relay_requests)

CLI:
  --date YYYY-MM-DD  # Europe/Amsterdam local date to load (default: yesterday)
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
import pandas as pd
from dateutil import parser as dtp

# -------------------------
# Config (env + defaults)
# -------------------------
REFERRER = os.getenv("REFERRER", "zkp2p.xyz")
BASE_URL = "https://api.relay.link/requests/v2"
PAGE_LIMIT = 50
SLEEP_BETWEEN = 0.25

DUNE_API_KEY   = os.getenv("DUNE_API_KEY", "")
DUNE_NAMESPACE = os.getenv("DUNE_NAMESPACE", "unhappyben")
DUNE_TABLE     = os.getenv("DUNE_TABLE", "relay_requests")

DUNE_CREATE_URL = "https://api.dune.com/api/v1/table/create"
DUNE_INSERT_URL = f"https://api.dune.com/api/v1/table/{DUNE_NAMESPACE}/{DUNE_TABLE}/insert"

JSON_HEADERS = {"X-DUNE-API-KEY": DUNE_API_KEY, "Content-Type": "application/json"}
CSV_HEADERS  = {"X-DUNE-API-KEY": DUNE_API_KEY, "Content-Type": "text/csv"}

# -------------------------
# HARD-CODED chain map (from your list)
# -------------------------
CHAIN_MAP = {
    999:        "Hyperliquid HyperEVM",      # note: some registries use 999 for Wanchain Testnet
    146:        "Sonic Mainnet",
    5000:       "Mantle Mainnet",
    8253038:    "Bitcoin",                   # Relay internal ID
    9745:       "Plasma Mainnet",
    56:         "BNB Chain (BSC)",
    43114:      "Avalanche C-Chain",
    1337:       "Localhost / Dev",
    480:        "World Chain Mainnet",
    42220:      "Celo Mainnet",
    33139:      "ApeChain Mainnet",
    792703809:  "Solana",                    # Relay internal ID
    1:          "Ethereum Mainnet",
    137:        "Polygon PoS",
    534352:     "Scroll Mainnet",
    42161:      "Arbitrum One",
    8453:       "Base Mainnet",
    100:        "Gnosis Chain (xDai)",
    747:        "Flow EVM Mainnet",
}

def chain_name(cid):
    if cid is None or (isinstance(cid, float) and pd.isna(cid)):
        return None
    try:
        return CHAIN_MAP.get(int(cid))
    except Exception:
        return None

# -------------------------
# Helpers
# -------------------------
def sget(d, path, default=None):
    cur = d
    for k in path:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return default
    return cur

def to_iso(ts):
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        return pd.to_datetime(ts, unit="s", utc=True).isoformat()
    try:
        return dtp.parse(ts).isoformat()
    except Exception:
        return ts

def to_float(x):
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None

def to_int(x):
    try:
        if x is None or x == "":
            return None
        return int(x)
    except Exception:
        return None

def to_bool(x):
    if isinstance(x, bool): return x
    if x in ("true","True","1",1): return True
    if x in ("false","False","0",0): return False
    return None

def ensure_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Enforce dtypes and column order exactly as in the table."""
    # Cast numerics
    int_cols = [
        "slippage_tolerance_bps","time_estimate_min",
        "fee_currency_chain_id","fee_currency_decimals",
        "in_chain_id","in_decimals",
        "out_chain_id","out_decimals",
    ]
    float_cols = [
        "fee_usd_gas","fee_usd_fixed","fee_usd_price","fee_usd_gateway",
        "in_amount_formatted","in_amount_usd",
        "out_amount_formatted","out_amount_usd",
        "rate_out_per_in",
    ]
    bool_cols = ["subsidized_request","uses_external_liquidity"]

    for c in int_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    for c in float_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in bool_cols:
        if c in df.columns:
            df[c] = df[c].astype("boolean")

    # Column order (with in/out chain names; origin/dest chain-name cols removed)
    cols = [
        "request_id","status","created_at","updated_at","user","recipient","referrer",
        "slippage_tolerance_bps","subsidized_request","uses_external_liquidity",
        "time_estimate_min","fail_reason","refund_fail_reason",
        "fee_gas","fee_fixed","fee_price","fee_gateway",
        "fee_usd_gas","fee_usd_fixed","fee_usd_price","fee_usd_gateway",
        "fee_currency_chain_id","fee_currency_address","fee_currency_symbol","fee_currency_decimals",
        "quoted_price_out_atomic",

        # in (with chain name)
        "in_chain_id","in_chain_name","in_address","in_symbol","in_decimals",
        "in_amount_atomic","in_amount_formatted","in_amount_usd","in_min_amount_atomic",

        # out (with chain name)
        "out_chain_id","out_chain_name","out_address","out_symbol","out_decimals",
        "out_amount_atomic","out_amount_formatted","out_amount_usd","out_min_amount_atomic",

        "rate_out_per_in",

        # keep router + route addresses (IDs not used for naming)
        "origin_router","dest_router",
        "origin_input_address","origin_output_address",
        "dest_input_address","dest_output_address",
    ]
    df = df.reindex(columns=cols)

    # Timestamps as 'YYYY-MM-DD HH:MM:SS'
    for c in ["created_at","updated_at"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce", utc=True).dt.strftime("%Y-%m-%d %H:%M:%S")

    # Booleans as 'true'/'false' strings
    for c in ["subsidized_request","uses_external_liquidity"]:
        if c in df.columns:
            df[c] = df[c].map({True: "true", False: "false"}).fillna("")

    return df

def create_table_if_needed():
    if not DUNE_API_KEY:
        raise SystemExit("❌ Missing DUNE_API_KEY env var.")
    schema = [
        {"name":"request_id","type":"varchar","nullable":False},
        {"name":"status","type":"varchar","nullable":True},
        {"name":"created_at","type":"timestamp","nullable":True},
        {"name":"updated_at","type":"timestamp","nullable":True},
        {"name":"user","type":"varchar","nullable":True},
        {"name":"recipient","type":"varchar","nullable":True},
        {"name":"referrer","type":"varchar","nullable":True},

        {"name":"slippage_tolerance_bps","type":"bigint","nullable":True},
        {"name":"subsidized_request","type":"boolean","nullable":True},
        {"name":"uses_external_liquidity","type":"boolean","nullable":True},
        {"name":"time_estimate_min","type":"bigint","nullable":True},
        {"name":"fail_reason","type":"varchar","nullable":True},
        {"name":"refund_fail_reason","type":"varchar","nullable":True},

        {"name":"fee_gas","type":"varchar","nullable":True},
        {"name":"fee_fixed","type":"varchar","nullable":True},
        {"name":"fee_price","type":"varchar","nullable":True},
        {"name":"fee_gateway","type":"varchar","nullable":True},

        {"name":"fee_usd_gas","type":"double","nullable":True},
        {"name":"fee_usd_fixed","type":"double","nullable":True},
        {"name":"fee_usd_price","type":"double","nullable":True},
        {"name":"fee_usd_gateway","type":"double","nullable":True},

        {"name":"fee_currency_chain_id","type":"bigint","nullable":True},
        {"name":"fee_currency_address","type":"varchar","nullable":True},
        {"name":"fee_currency_symbol","type":"varchar","nullable":True},
        {"name":"fee_currency_decimals","type":"integer","nullable":True},

        {"name":"quoted_price_out_atomic","type":"varchar","nullable":True},

        # in (with chain name)
        {"name":"in_chain_id","type":"bigint","nullable":True},
        {"name":"in_chain_name","type":"varchar","nullable":True},
        {"name":"in_address","type":"varchar","nullable":True},
        {"name":"in_symbol","type":"varchar","nullable":True},
        {"name":"in_decimals","type":"integer","nullable":True},
        {"name":"in_amount_atomic","type":"varchar","nullable":True},
        {"name":"in_amount_formatted","type":"double","nullable":True},
        {"name":"in_amount_usd","type":"double","nullable":True},
        {"name":"in_min_amount_atomic","type":"varchar","nullable":True},

        # out (with chain name)
        {"name":"out_chain_id","type":"bigint","nullable":True},
        {"name":"out_chain_name","type":"varchar","nullable":True},
        {"name":"out_address","type":"varchar","nullable":True},
        {"name":"out_symbol","type":"varchar","nullable":True},
        {"name":"out_decimals","type":"integer","nullable":True},
        {"name":"out_amount_atomic","type":"varchar","nullable":True},
        {"name":"out_amount_formatted","type":"double","nullable":True},
        {"name":"out_amount_usd","type":"double","nullable":True},
        {"name":"out_min_amount_atomic","type":"varchar","nullable":True},

        {"name":"rate_out_per_in","type":"double","nullable":True},

        # keep router + route addresses only
        {"name":"origin_router","type":"varchar","nullable":True},
        {"name":"dest_router","type":"varchar","nullable":True},
        {"name":"origin_input_address","type":"varchar","nullable":True},
        {"name":"origin_output_address","type":"varchar","nullable":True},
        {"name":"dest_input_address","type":"varchar","nullable":True},
        {"name":"dest_output_address","type":"varchar","nullable":True},
    ]
    payload = {
        "table_name": DUNE_TABLE,
        "namespace": DUNE_NAMESPACE,
        "is_private": False,
        "description": "Relay requests flattened (daily loader) with hard-coded in/out chain names",
        "schema": schema,
    }
    resp = requests.post(DUNE_CREATE_URL, headers=JSON_HEADERS, json=payload, timeout=120)
    if resp.status_code in (200, 201, 409):
        info = {}
        try:
            info = resp.json()
        except Exception:
            pass
        print(f"Table ensure: status={resp.status_code} full_name={info.get('full_name','(unknown)')} existed={info.get('already_existed')}")
    else:
        raise SystemExit(f"❌ Dune create failed: {resp.status_code} - {resp.text}")

def insert_csv(csv_body: str):
    resp = requests.post(DUNE_INSERT_URL, headers=CSV_HEADERS, data=csv_body.encode("utf-8"), timeout=600)
    if resp.status_code != 200:
        raise SystemExit(f"❌ Insert failed: {resp.status_code} - {resp.text}")
    print("✅ Insert response:", resp.json())

def compute_local_day_window_eu_amsterdam(target_date_str: str | None):
    tz = ZoneInfo("Europe/Amsterdam")
    if target_date_str:
        d = datetime.strptime(target_date_str, "%Y-%m-%d").date()
    else:
        # "yesterday" in Europe/Amsterdam
        now_local = datetime.now(tz)
        d = (now_local - timedelta(days=1)).date()
    start_local = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=tz)
    end_local   = datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=tz)
    # Convert to UTC for comparisons
    start_utc = start_local.astimezone(ZoneInfo("UTC"))
    end_utc   = end_local.astimezone(ZoneInfo("UTC"))
    return d.isoformat(), start_utc, end_utc

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Local date (Europe/Amsterdam) to load, YYYY-MM-DD. Defaults to yesterday.", default=None)
    args = parser.parse_args()

    day_str, start_utc, end_utc = compute_local_day_window_eu_amsterdam(args.date)
    print(f"Loading date (Europe/Amsterdam): {day_str}  UTC window: {start_utc.isoformat()} .. {end_utc.isoformat()}")

    # 1) Fetch all Relay requests (paginate)
    all_requests, continuation, seen = [], None, set()
    while True:
        params = {"referrer": REFERRER, "limit": PAGE_LIMIT}
        if continuation:
            params["continuation"] = continuation
        r = requests.get(BASE_URL, params=params, timeout=60)
        r.raise_for_status()
        payload = r.json()
        batch = payload.get("requests", []) or []
        for item in batch:
            _id = item.get("id")
            if _id and _id not in seen:
                seen.add(_id)
                all_requests.append(item)
        continuation = payload.get("continuation")
        print(f"Fetched: {len(all_requests)}")
        if not continuation:
            break
        time.sleep(SLEEP_BETWEEN)
    print(f"Total fetched: {len(all_requests)}")

    # 2) Flatten
    rows = []
    for req in all_requests:
        rid  = req.get("id")
        data = req.get("data", {}) or {}
        meta = data.get("metadata", {}) or {}

        cur_in   = sget(meta, ["currencyIn"])  or {}
        cur_out  = sget(meta, ["currencyOut"]) or {}
        in_curr  = cur_in.get("currency")  or {}
        out_curr = cur_out.get("currency") or {}
        fee_curr = sget(data, ["feeCurrencyObject"]) or {}

        # build in/out chain names from hard-coded map
        in_cid  = to_int(in_curr.get("chainId"))
        out_cid = to_int(out_curr.get("chainId"))

        rows.append({
            "request_id": rid,
            "status": req.get("status"),
            "created_at": to_iso(req.get("createdAt")),
            "updated_at": to_iso(req.get("updatedAt")),
            "user": req.get("user"),
            "recipient": req.get("recipient"),
            "referrer": req.get("referrer"),

            "slippage_tolerance_bps": to_int(sget(data, ["slippageTolerance"])),
            "subsidized_request": to_bool(sget(data, ["subsidizedRequest"])),
            "uses_external_liquidity": to_bool(sget(data, ["usesExternalLiquidity"])),
            "time_estimate_min": to_int(sget(data, ["timeEstimate"])),
            "fail_reason": sget(data, ["failReason"]),
            "refund_fail_reason": sget(data, ["refundFailReason"]),

            "fee_gas": sget(data, ["fees","gas"]),
            "fee_fixed": sget(data, ["fees","fixed"]),
            "fee_price": sget(data, ["fees","price"]),
            "fee_gateway": sget(data, ["fees","gateway"]),

            "fee_usd_gas": to_float(sget(data, ["feesUsd","gas"])),
            "fee_usd_fixed": to_float(sget(data, ["feesUsd","fixed"])),
            "fee_usd_price": to_float(sget(data, ["feesUsd","price"])),
            "fee_usd_gateway": to_float(sget(data, ["feesUsd","gateway"])),

            "fee_currency_chain_id": to_int(fee_curr.get("chainId")),
            "fee_currency_address": fee_curr.get("address"),
            "fee_currency_symbol": fee_curr.get("symbol"),
            "fee_currency_decimals": to_int(fee_curr.get("decimals")),

            "quoted_price_out_atomic": sget(data, ["price"]),

            # in (with chain name)
            "in_chain_id": in_cid,
            "in_chain_name": chain_name(in_cid),
            "in_address": in_curr.get("address"),
            "in_symbol": in_curr.get("symbol"),
            "in_decimals": to_int(in_curr.get("decimals")),
            "in_amount_atomic": cur_in.get("amount"),
            "in_amount_formatted": to_float(cur_in.get("amountFormatted")),
            "in_amount_usd": to_float(cur_in.get("amountUsd")),
            "in_min_amount_atomic": cur_in.get("minimumAmount"),

            # out (with chain name)
            "out_chain_id": out_cid,
            "out_chain_name": chain_name(out_cid),
            "out_address": out_curr.get("address"),
            "out_symbol": out_curr.get("symbol"),
            "out_decimals": to_int(out_curr.get("decimals")),
            "out_amount_atomic": cur_out.get("amount"),
            "out_amount_formatted": to_float(cur_out.get("amountFormatted")),
            "out_amount_usd": to_float(cur_out.get("amountUsd")),
            "out_min_amount_atomic": cur_out.get("minimumAmount"),

            "rate_out_per_in": to_float(sget(meta, ["rate"])),

            # keep router + route addresses for lineage
            "origin_router": sget(meta, ["route","origin","router"]),
            "dest_router":   sget(meta, ["route","destination","router"]),
            "origin_input_address": sget(meta, ["route","origin","inputCurrency","currency","address"]),
            "origin_output_address": sget(meta, ["route","origin","outputCurrency","currency","address"]),
            "dest_input_address": sget(meta, ["route","destination","inputCurrency","currency","address"]),
            "dest_output_address": sget(meta, ["route","destination","outputCurrency","currency","address"]),
        })

    df = pd.DataFrame(rows)

    # Normalize + ensure schema
    df = ensure_schema(df)

    # Create helper timestamp (UTC) for filtering
    df["_created_at_ts"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)

    # Filter to Europe/Amsterdam "day" window (in UTC)
    mask = (df["_created_at_ts"] >= pd.to_datetime(start_utc)) & (df["_created_at_ts"] <= pd.to_datetime(end_utc))
    df_day = df.loc[mask].drop(columns=["_created_at_ts"]).copy()

    print(f"Rows for {day_str}: {len(df_day)}")
    if len(df_day) == 0:
        print("No rows to upload for this date. Exiting cleanly.")
        return

    # Write CSV
    csv_path = f"relay_requests_{day_str}.csv"
    df_day.to_csv(csv_path, index=False, na_rep="")
    print(f"CSV written: {csv_path}")

    # Ensure table exists, then insert
    create_table_if_needed()
    with open(csv_path, "r", encoding="utf-8") as f:
        csv_body = f.read()
    insert_csv(csv_body)

if __name__ == "__main__":
    main()
