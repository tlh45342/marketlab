#
# study_windows_from_1m.py
#
# Build 5 / 20 / 60 day volatility windows
# using 1-minute data from MarketLab
# ALSO logs missing/thin days AND enqueues recovery
#

import pandas as pd
import requests
from pathlib import Path
from datetime import datetime, timedelta
import tcore


# --------------------------------------------------
# config
# --------------------------------------------------

INPUT_FILE   = Path(r"N:\data\TEST5.csv")
OUTPUT_FILE  = Path(r"N:\data\WINDOWS.xlsx")
MISSING_FILE = Path(r"N:\data\missing.xlsx")
QUEUE_FILE   = Path(r"N:\data\1mqueue_recovery.csv")

BASE_URL = "http://192.168.150.102:8000/v1"

TIMEOUT   = 5
MIN_BARS  = 50   # threshold for valid trading day


# --------------------------------------------------
# logging
# --------------------------------------------------

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")


# --------------------------------------------------
# enqueue recovery work
# --------------------------------------------------

def enqueue(symbol, day):

    row = pd.DataFrame([{
        "symbol": symbol,
        "date": day
    }])

    if QUEUE_FILE.exists():
        df = pd.read_csv(QUEUE_FILE)
        df = pd.concat([df, row], ignore_index=True)
    else:
        df = row

    df = df.drop_duplicates()

    df.to_csv(QUEUE_FILE, index=False)

    log(f"[ENQUEUE] {symbol} {day}")


# --------------------------------------------------
# load symbols (NO HEADER)
# --------------------------------------------------

def load_symbols():
    df = pd.read_csv(INPUT_FILE, header=None)
    symbols = df.iloc[:, 0].dropna().astype(str).str.strip().unique()
    return symbols


# --------------------------------------------------
# trading days (exclude today)
# --------------------------------------------------

def get_trading_days():
    today = datetime.now().date()

    end = today - timedelta(days=1)   # exclude today
    start = end - timedelta(days=120)

    days = tcore.get_trading_range(
        start.strftime("%Y-%m-%d"),
        end.strftime("%Y-%m-%d")
    )

    log(f"Trading days count (no today): {len(days)}")

    return days[-90:]  # buffer


# --------------------------------------------------
# fetch + aggregate
# --------------------------------------------------

def fetch_day(symbol, day):
    url = f"{BASE_URL}/symbol/{symbol}/day/{day}"

    try:
        r = requests.get(url, timeout=TIMEOUT)

        if r.status_code != 200:
            log(f"[FAIL] {symbol} {day} status={r.status_code}")
            return None, "FAIL"

        data = r.json()

        if not isinstance(data, list):
            log(f"[FAIL] {symbol} {day} bad format")
            return None, "FAIL"

        bars = len(data)

        if bars < MIN_BARS:
            log(f"[THIN] {symbol} {day} bars={bars}")
            return None, "THIN"

        df = pd.DataFrame(data)
        df.columns = [c.lower() for c in df.columns]

        required = {"open", "high", "low", "close", "volume"}
        if not required.issubset(df.columns):
            log(f"[FAIL] {symbol} {day} missing columns")
            return None, "FAIL"

        return {
            "date": day,
            "open": df.iloc[0]["open"],
            "high": df["high"].max(),
            "low": df["low"].min(),
            "close": df.iloc[-1]["close"],
            "volume": df["volume"].sum(),
        }, "OK"

    except Exception as e:
        log(f"[ERROR] {symbol} {day} {e}")
        return None, "FAIL"


# --------------------------------------------------
# build daily series
# --------------------------------------------------

def build_daily_series(symbol, days, missing_rows):
    rows = []

    for day in days:
        d, status = fetch_day(symbol, day)

        if status == "OK":
            rows.append(d)
        else:
            missing_rows.append((symbol, day))
            enqueue(symbol, day)   # 🔥 NEW BEHAVIOR

    log(f"[INFO] {symbol} valid days collected: {len(rows)}")

    if len(rows) < 60:
        return None

    df = pd.DataFrame(rows)
    df["range"] = df["high"] - df["low"]

    return df.sort_values("date")


# --------------------------------------------------
# compute windows
# --------------------------------------------------

def compute_windows(df):
    return {
        "range_5":  df["range"].tail(5).mean(),
        "range_20": df["range"].tail(20).mean(),
        "range_60": df["range"].tail(60).mean(),
    }


# --------------------------------------------------
# main
# --------------------------------------------------

def main():
    log("Starting study (1m → 1d aggregation)")

    symbols = load_symbols()
    trading_days = get_trading_days()

    missing_rows = []

    # sanity test
    test_day = trading_days[-3]
    log(f"[TEST] Checking MARA {test_day}")

    test, status = fetch_day("MARA", test_day)
    if status == "OK":
        log("[TEST PASS] MARA fetch working")
    else:
        log("[TEST FAIL] MARA fetch failed")

    results = []

    for i, symbol in enumerate(symbols, 1):
        log(f"[{i}/{len(symbols)}] {symbol}")

        df = build_daily_series(symbol, trading_days, missing_rows)

        if df is None:
            log(f"[SKIP] insufficient data {symbol}")
            continue

        stats = compute_windows(df)

        results.append({
            "symbol": symbol,
            "range_5":  round(stats["range_5"], 4),
            "range_20": round(stats["range_20"], 4),
            "range_60": round(stats["range_60"], 4),
            "expansion": round(stats["range_5"] / stats["range_20"], 3)
        })

    # ----------------------------------------
    # write main output
    # ----------------------------------------

    df_out = pd.DataFrame(results)

    if len(df_out) > 0:
        df_out.to_excel(OUTPUT_FILE, index=False)
        log(f"Done. Wrote {len(df_out)} rows → {OUTPUT_FILE}")
    else:
        log("No valid results to write.")

    # ----------------------------------------
    # write missing data
    # ----------------------------------------

    if missing_rows:
        df_missing = pd.DataFrame(missing_rows, columns=["symbol", "date"])
        df_missing = df_missing.drop_duplicates()

        df_missing.to_excel(MISSING_FILE, index=False)

        log(f"Wrote missing data → {MISSING_FILE} ({len(df_missing)} rows)")


# --------------------------------------------------

if __name__ == "__main__":
    main()
