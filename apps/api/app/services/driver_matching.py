# apps/api/app/services/driver_matching.py
from pathlib import Path
import pandas as pd, numpy as np
from haversine import haversine
from scipy.optimize import linear_sum_assignment

CAP_RANK = {"small": 1, "medium": 2, "large": 3}

def match_trips(country: str):
    cc = country.lower()
    data_dir = Path("data/processed")

    drivers = (
        pd.read_csv(data_dir / "sample_drivers.csv")
        .query("country == @country.upper()")
        .reset_index(drop=True)         
    )
    trips = (
        pd.read_csv(data_dir / f"{cc}_mock_trip_logs.csv")
        .reset_index(drop=True)         
    )

    # ---------- build feasible pairs ---------------------------------------
    pairs = []
    for ti, t in trips.iterrows():        # ti is now row-position
        for di, d in drivers.iterrows():  # di is row-position
            if CAP_RANK[d.capacity] < CAP_RANK[t.cargo_size]:
                continue
            dist_km = haversine(
                (d.base_location_lat, d.base_location_lon),
                (t.pickup_lat,        t.pickup_lon)
            )
            if dist_km > 50:
                continue
            score = (1 - dist_km / 50) * 0.4 \
                  + d.avg_acceptance_rate * 0.3 \
                  + d.avg_completion_rate * 0.3
            pairs.append((ti, di, -score))     # negative for min-cost

    # ---------- build cost matrix & solve ----------------------------------
    cost = np.full((len(trips), len(drivers)), 1e6)
    for ti, di, s in pairs:
        cost[ti, di] = s

    row_idx, col_idx = linear_sum_assignment(cost)
    assignments = [
        {
            "trip_id":   trips.loc[r, "trip_id"],
            "driver_id": drivers.loc[c, "driver_id"]
        }
        for r, c in zip(row_idx, col_idx) if cost[r, c] < 1e5
    ]

    out_fp = data_dir / f"{cc}_assignments.csv"
    pd.DataFrame(assignments).to_csv(out_fp, index=False)
    print(f"assignments → {out_fp}")
    return out_fp
