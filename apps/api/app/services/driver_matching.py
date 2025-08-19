
# Key ideas:
#   - Capacity gating: driver truck must be >= trip cargo size.
#   - Proximity gating: driver base must be within 50 km of pickup.
#   - Scoring: favor closer drivers and those with higher acceptance/completion.
#   - Optimization: use linear_sum_assignment (Hungarian) on a cost matrix.
#   - Infeasible pairs are represented by a large sentinel cost (1e6).
# -----------------------------------------------------------------------------

from pathlib import Path
import pandas as pd, numpy as np
from haversine import haversine                   
from scipy.optimize import linear_sum_assignment  

# Capacity ranking to compare driver vehicle size vs. trip cargo size
# (higher number == can carry more)
CAP_RANK = {"small": 1, "medium": 2, "large": 3}

def match_trips(country: str, booking=None):
    cc = country.lower()
    data_dir = Path("data/processed")

    drivers = (
        pd.read_csv(data_dir / "sample_drivers.csv")
        .query("country == @country.upper()")
        .reset_index(drop=True)         
    )

    if booking is None:
        # If no user input is provided, read the whole simulated bookings file for this country
        bookings = (
            pd.read_csv(data_dir / f"{cc}.csv")
            .reset_index(drop=True)
        )
    else:
        # Accept a single booking and normalize to DataFrame
        if isinstance(booking, dict):
            bookings = pd.DataFrame([booking])
        else:
            raise ValueError("Unsupported booking type. Use dict.")

        bookings = bookings.reset_index(drop=True)

        # Sanity check: ensure required columns exist
        required = {"booking_id", "move_size", "pickup_lat", "pickup_lon"}
        missing = required - set(bookings.columns)
        if missing:
            raise ValueError(f"Missing required fields in single booking: {sorted(missing)}")


    # ---------- build feasible pairs ---------------------------------------
    pairs = []  
    for ti, t in bookings.iterrows():        # ti is the row-position in 'trips'
        for di, d in drivers.iterrows():  # di is the row-position in 'drivers'
            # Capacity check: driver's vehicle must be >= trip's cargo size
            if CAP_RANK[d.capacity] < CAP_RANK[t.move_size]:
                continue
            # Proximity check: driver's base must be within 50 km of pickup
            dist_km = haversine(
                (d.base_location_lat, d.base_location_lon),
                (t.pickup_lat,        t.pickup_lon)
            )
            if dist_km > 50:
                continue
            # Score combines (closer is better) + driver quality metrics
            #   - distance term scaled to [0..1] over 0..50 km with weight 0.4
            #   - acceptance/completion rates weighted 0.3 each
            score = (1 - dist_km / 50) * 0.4 \
                  + d.avg_acceptance_rate * 0.3 \
                  + d.avg_completion_rate * 0.3
            # linear_sum_assignment minimizes cost → store negative score as cost
            pairs.append((ti, di, -score))     # negative for min-cost

    # ---------- build cost matrix & solve ----------------------------------
    # Create a dense cost matrix initialized with a large sentinel (infeasible)
    cost = np.full((len(bookings), len(drivers)), 1e6)
    # Populate feasible entries with computed costs
    for ti, di, s in pairs:
        cost[ti, di] = s

    # Solve the assignment problem (returns selected row/col indices)
    row_idx, col_idx = linear_sum_assignment(cost)

    # Build results only for entries whose cost is below the sentinel threshold.
    # This discards any forced matches to infeasible (1e6) slots.
    assignments = [
        {
            "trip_id":   bookings.loc[r, "booking_id"],
            "driver_id": drivers.loc[c, "driver_id"]
        }
        for r, c in zip(row_idx, col_idx) if cost[r, c] < 1e5
    ]

    out_fp = data_dir / f"{cc}_assignments.csv"
    pd.DataFrame(assignments).to_csv(out_fp, index=False)
    print(f"assignments → {out_fp}")
    return out_fp
