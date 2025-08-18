#!/usr/bin/env python
"""
Enrich booking_requests.csv with ORS distance & duration.
"""

from pathlib import Path
import time, pandas as pd
import os, random
from routes.openrouteservice_wrapper import OpenRouteServiceWrapper
from scripts.run_etl import PIPELINES      # to discover valid country codes

ORS = OpenRouteServiceWrapper()            # raises if ORS_API_KEY missing
# ~35 requests/min keeps us safely under the free 40 RPM cap
MAX_RPM     = int(os.getenv("ORS_MAX_RPM", "35"))
BASE_DELAY  = 60.0 / MAX_RPM
JITTER_FRAC = 0.2  # add ±20% jitter to avoid bursts


def _enrich_file(csv_path: Path) -> pd.DataFrame | None:
    df = pd.read_csv(csv_path)

    req_cols = {"pickup_lat", "pickup_lon", "dropoff_lat", "dropoff_lon"}
    if not req_cols.issubset(df.columns):
        print(f"{csv_path.name} lacks lat/lon columns – skipped", flush=True)
        return

    distances, durations = [], []
    for _, row in df.iterrows():
        start = (row["pickup_lon"], row["pickup_lat"])
        end   = (row["dropoff_lon"], row["dropoff_lat"])

        res = ORS.get_route(start, end)

        if ("error" in res) or (res.get("distance_m") is None) or (res.get("duration_s") is None):
            distances.append("")
            durations.append("")
        else:
            distances.append(res["distance_m"])
            durations.append(res["duration_s"])

        time.sleep(BASE_DELAY + random.uniform(0, BASE_DELAY * JITTER_FRAC))

    df["distance_m"] = distances
    df["duration_s"] = durations

    out_csv = csv_path.with_name(csv_path.stem + "_dist.csv")
    df.to_csv(out_csv, index=False)
    print(f"{csv_path} → {out_csv.name}", flush=True)
    return df


def enrich_country(cc: str) -> None:
    """Run enrichment for one country code (mx, co, cr, …)"""
    base = Path("tmp") / cc
    files = list(base.rglob("booking_requests.csv"))
    print(f"[{cc}] found {len(files)} booking files under {base}", flush=True)

    dfs = []
    for csv in files:
        enriched = _enrich_file(csv)
        if enriched is None or enriched.empty:
            print(f"[{cc}] skipped empty/failed: {csv}", flush=True)
            continue
        dfs.append(enriched)

    if not dfs:
        print(f"[{cc}] nothing to concatenate - leaving data/processed/{cc}.csv as-is", flush=True)
        return

    final = Path(f"data/processed/{cc}.csv")
    try:
        pd.concat(dfs, ignore_index=True).to_csv(final, index=False)
    except Exception as e:
        # print full traceback and re-raise so Docker logs show the cause
        import traceback; traceback.print_exc()
        raise
    print(f"Saved output with distance → {final}", flush=True)




