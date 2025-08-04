#!/usr/bin/env python
"""
Enrich booking_requests.csv with ORS distance & duration.
"""

from pathlib import Path
import time, pandas as pd
from routes.openrouteservice_wrapper import OpenRouteServiceWrapper
from scripts.run_etl import PIPELINES      # to discover valid country codes

ORS = OpenRouteServiceWrapper()            # raises if ORS_API_KEY missing
RATE_LIMIT_DELAY = 0.3                     # ≈ 40 calls/min


def _enrich_file(csv_path: Path) -> None:
    df = pd.read_csv(csv_path)

    req_cols = {"pickup_lat", "pickup_lon", "dropoff_lat", "dropoff_lon"}
    if not req_cols.issubset(df.columns):
        print(f"{csv_path.name} lacks lat/lon columns – skipped", flush=True)
        return

    distances, durations = [], []
    for _, row in df.iterrows():
        start = (row["pickup_lon"], row["pickup_lat"])
        end   = (row["dropoff_lon"], row["dropoff_lat"])

        try:
            res = ORS.get_route(start, end)
        except Exception as exc:
            # Log network error during API call, 400/429 response, etc.
            print(f"Route failed {start}->{end}: {exc}", flush=True)
            distances.append("")   # or None
            durations.append("")
            continue

        distances.append(res.get("distance_m", ""))
        durations.append(res.get("duration_s", ""))
        time.sleep(RATE_LIMIT_DELAY)

    df["distance_m"] = distances
    df["duration_s"] = durations

    out_csv = csv_path.with_name(csv_path.stem + "_dist.csv")
    df.to_csv(out_csv, index=False)
    print(f"{csv_path} → {out_csv.name}")
    return df


def enrich_country(cc: str) -> None:
    """Run enrichment for one country code (mx, co, cr, …)"""
    dfs = []
    for csv in Path("tmp", cc).rglob("booking_requests.csv"):
        dfs.append(_enrich_file(csv))

    if dfs:                          
        final = Path(f"data/processed/{cc}.csv")
        pd.concat(dfs, ignore_index=True).to_csv(final, index=False)
        print(f"Saved output with distance → {final}", flush=True)



