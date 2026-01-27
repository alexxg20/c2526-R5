import os
import time
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

import requests
import pandas as pd
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()

DOMAIN = "https://data.ny.gov"
OUT_DIR = os.path.join("data", "raw")
APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN")  # optional

# 5 consecutive days: 24..27 if END is 28 excl (your current config)
START_DAY = date(2024, 1, 24)
END_DAY_EXCL = date(2024, 1, 28)  # end exclusive -> includes 24..27

MONTH_START = date(2024, 1, 1)
MONTH_END_EXCL = date(2024, 2, 1)

DATASETS = {
    "hourly_ridership": {
        "id": "wujg-7c2s",
        "date_col": "transit_timestamp",
        "dtype": "floating_timestamp",
        "range_start": START_DAY,
        "range_end_excl": END_DAY_EXCL,
        # Optional: reduce payload (recommended)
        "select": "transit_timestamp,transit_mode,station_complex_id,borough,payment_method,fare_class_category,ridership,transfers,latitude,longitude",
        # If you want even smaller:
        # "select": "transit_timestamp,station_complex_id,borough,ridership,transfers",
        "per_day": True,  # <-- key: fetch one day at a time
    },
    "major_incidents": {
        "id": "j6d2-s8m2",
        "date_col": "month",
        "dtype": "floating_timestamp",
        "range_start": MONTH_START,
        "range_end_excl": MONTH_END_EXCL,
        "select": None,
        "per_day": False,  # monthly dataset, fetch once
    },
    "terminal_otp": {
        "id": "vtvh-gimj",
        "date_col": "month",
        "dtype": "floating_timestamp",
        "range_start": MONTH_START,
        "range_end_excl": MONTH_END_EXCL,
        "select": None,
        "per_day": False,  # monthly dataset, fetch once
    },
}

# ---------- Robust HTTP session with retries ----------
def build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=6,
        connect=6,
        read=6,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

SESSION = build_session()

def headers() -> Dict[str, str]:
    h = {"Accept": "application/json"}
    if APP_TOKEN:
        h["X-App-Token"] = APP_TOKEN
    return h

def build_where(date_col: str, dtype: str, start_d: date, end_excl: date) -> str:
    if dtype == "calendar_date":
        return f"{date_col} >= '{start_d.isoformat()}' AND {date_col} < '{end_excl.isoformat()}'"
    start_iso = datetime(start_d.year, start_d.month, start_d.day, 0, 0, 0).isoformat()
    end_iso = datetime(end_excl.year, end_excl.month, end_excl.day, 0, 0, 0).isoformat()
    return f"{date_col} >= '{start_iso}' AND {date_col} < '{end_iso}'"

def fetch_all_rows(
    dataset_id: str,
    where: Optional[str] = None,
    page_size: int = 25000,  # <-- smaller page reduces timeout risk
    select: Optional[str] = None,
) -> List[dict]:
    base = f"{DOMAIN}/resource/{dataset_id}.json"
    out: List[dict] = []
    offset = 0

    while True:
        params = {"$limit": page_size, "$offset": offset}
        if where:
            params["$where"] = where
        if select:
            params["$select"] = select

        # timeout=(connect_seconds, read_seconds)
        r = SESSION.get(base, headers=headers(), params=params, timeout=(10, 180))
        r.raise_for_status()
        batch = r.json()

        if not batch:
            break

        out.extend(batch)
        if len(batch) < page_size:
            break

        offset += page_size
        time.sleep(0.2)

    return out

def daterange(start_d: date, end_excl: date):
    d = start_d
    while d < end_excl:
        yield d
        d += timedelta(days=1)

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    for name, cfg in DATASETS.items():
        ds_id = cfg["id"]
        date_col = cfg["date_col"]
        dtype = cfg["dtype"]
        start_d = cfg["range_start"]
        end_excl = cfg["range_end_excl"]
        select = cfg.get("select")
        per_day = cfg.get("per_day", False)

        print(f"\n=== {name} ({ds_id}) ===")

        if per_day:
            # Fetch each day separately (much more stable)
            all_parts = []
            for d in daterange(start_d, end_excl):
                d_end = d + timedelta(days=1)
                where = build_where(date_col, dtype, d, d_end)
                print(f"DAY {d.isoformat()} WHERE: {where}")

                rows = fetch_all_rows(ds_id, where=where, select=select)
                print(f"  -> {len(rows)} rows")
                if rows:
                    all_parts.append(pd.DataFrame(rows))

            df = pd.concat(all_parts, ignore_index=True) if all_parts else pd.DataFrame()

        else:
            where = build_where(date_col, dtype, start_d, end_excl)
            print(f"WHERE: {where}")

            rows = fetch_all_rows(ds_id, where=where, select=select)
            df = pd.DataFrame(rows)

        out_csv = os.path.join(
            OUT_DIR,
            f"{name}_{start_d.isoformat()}_to_{end_excl.isoformat()}_end_excl.csv"
        )
        df.to_csv(out_csv, index=False)
        print(f"Saved {len(df)} rows -> {out_csv}")

if __name__ == "__main__":
    main()
