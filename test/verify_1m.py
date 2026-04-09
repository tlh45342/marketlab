#
# verify_1m.py (refactored)
#

import requests
import pandas as pd
from datetime import datetime

BASE_URL = "http://192.168.150.102:8000/v1"


def ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# --------------------------------------------------
# FETCH
# --------------------------------------------------

def fetch_bars(symbol, timeframe, start, end):

    url = (
        f"{BASE_URL}/bars"
        f"?symbol={symbol}"
        f"&timeframe={timeframe}"
        f"&start={start}"
        f"&end={end}"
    )

    print(f"\n[{ts()}] [REQUEST]")
    print(url)

    r = requests.get(url, timeout=10)

    print(f"[{ts()}] [STATUS] {r.status_code}")

    try:
        data = r.json()
    except Exception:
        print("[ERROR] Failed to decode JSON")
        print(r.text)
        return pd.DataFrame()

    # --------------------------------------------------
    # DEBUG RAW RESPONSE
    # --------------------------------------------------
    print(f"[{ts()}] [RAW COUNT] {len(data)}")

    if len(data) > 0:
        print("[SAMPLE RAW ROW]")
        print(data[0])

    if not data:
        print(f"[{ts()}] [WARN] No data returned")
        return pd.DataFrame()

    return pd.DataFrame(data)


# --------------------------------------------------
# NORMALIZE
# --------------------------------------------------

def normalize_timestamp(df):
    if df.empty or "timestamp" not in df.columns:
        return df

    ts_sample = df["timestamp"].iloc[0]

    if isinstance(ts_sample, (int, float)):
        if ts_sample > 1e12:
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        else:
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    else:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)

    return df


# --------------------------------------------------
# DISPLAY
# --------------------------------------------------

def print_summary(df, limit=10):

    if df.empty:
        print("\n❌ No data (empty dataframe)")
        return

    df = normalize_timestamp(df)

    print("\n--- SUMMARY ---")
    print(f"Rows: {len(df)}")

    if "timestamp" in df.columns:
        print(f"Start: {df['timestamp'].min()}")
        print(f"End  : {df['timestamp'].max()}")

    print("\n--- SAMPLE ---")
    print(df.head(limit).to_string(index=False))


# --------------------------------------------------
# MAIN
# --------------------------------------------------

if __name__ == "__main__":

    symbol = "AA"
    date = "2021-01-04"

    df = fetch_bars(
        symbol=symbol,
        timeframe="1m",
        start=date,
        end=date
    )

    print_summary(df)
