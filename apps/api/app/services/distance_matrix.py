from pathlib import Path
import pandas as pd
import numpy as np

def build_matrices(country: str,
                                agg: str = "median",
                                fill: str = "none") -> dict:
    """
    Build country-wide square matrices using the UNION of stop IDs

    Parameters
    ----------
    country : "mx" | "co" | "cr" | …
    agg     : aggregation for duplicates ("median", "mean", "min", "max")
    fill    : "none" → leave NaNs; "impute" → fill using haversine-based rules

    Outputs (in data/processed):
      <cc>_distance_matrix.csv
      <cc>_duration_matrix.csv
      (if fill="impute"):
      <cc>_distance_matrix_filled.csv
      <cc>_duration_matrix_filled.csv
    """
    cc = country.lower()
    out_dir = Path("data/processed")
    src = out_dir / f"{cc}.csv"

    usecols = [
        "pickup_stop_id","dropoff_stop_id",
        "pickup_lat","pickup_lon","dropoff_lat","dropoff_lon",
        "distance_m","duration_s"
    ]
    df = pd.read_csv(src, usecols=usecols)

    # Coerce numeric (blank cells -> NaN)
    df["distance_m"] = pd.to_numeric(df["distance_m"], errors="coerce")
    df["duration_s"] = pd.to_numeric(df["duration_s"], errors="coerce")

    # Ensure stop IDs are strings BEFORE union/pivot to avoid dtype mismatches
    df["pickup_stop_id"]  = df["pickup_stop_id"].astype(str).str.strip()
    df["dropoff_stop_id"] = df["dropoff_stop_id"].astype(str).str.strip()

    # Sanity: what does the function actually see?
    rows_total = len(df)
    rows_with_dist = int(df["distance_m"].notna().sum())
    pairs_all = int(df[["pickup_stop_id","dropoff_stop_id"]].astype(str).drop_duplicates().shape[0])
    pairs_with_dist = int(df[df["distance_m"].notna()][["pickup_stop_id","dropoff_stop_id"]]
                        .astype(str).drop_duplicates().shape[0])
    print(f"[{cc}] rows={rows_total} rows_with_distance={rows_with_dist} "
        f"unique_pairs={pairs_all} unique_pairs_with_distance={pairs_with_dist}", flush=True)


    # Build union of all IDs
    pu = df["pickup_stop_id"]
    do = df["dropoff_stop_id"]
    union_ids = sorted(pd.Index(pu.unique()).union(pd.Index(do.unique())))

    # Rectangular pivots on observed pairs
    dist = df.pivot_table(index="pickup_stop_id", columns="dropoff_stop_id",
                          values="distance_m", aggfunc=agg)
    dur  = df.pivot_table(index="pickup_stop_id", columns="dropoff_stop_id",
                          values="duration_s", aggfunc=agg)
    
    # How many non-null cells did the pivot produce *before* reindex?
    pivot_filled = int(dist.notna().to_numpy().sum())
    print(f"[{cc}] pivot_non_null_cells_before_reindex={pivot_filled}", flush=True)

    
    # Also force pivot axes to strings (belt-and-suspenders)
    dist.index   = dist.index.astype(str)
    dist.columns = dist.columns.astype(str)
    dur.index    = dur.index.astype(str)
    dur.columns  = dur.columns.astype(str)

    # Reindex to UNION × UNION (square)
    dist = dist.reindex(index=union_ids, columns=union_ids)
    dur  = dur.reindex(index=union_ids, columns=union_ids)

    # Diagonal = 0
    if dist.size: np.fill_diagonal(dist.values, 0.0)
    if dur.size:  np.fill_diagonal(dur.values,  0.0)

    # Write raw (possibly sparse) matrices
    dist_path = out_dir / f"{cc}_distance_matrix.csv"
    dur_path  = out_dir / f"{cc}_duration_matrix.csv"
    dist.to_csv(dist_path, float_format="%.3f")
    dur.to_csv(dur_path,  float_format="%.3f")

    filled_paths = {}

    if fill.lower() == "impute":
        # ---- Prepare coordinates per stop_id for haversine baseline ----
        stops_pick = df[["pickup_stop_id","pickup_lat","pickup_lon"]].drop_duplicates().rename(
            columns={"pickup_stop_id":"stop_id","pickup_lat":"lat","pickup_lon":"lon"}
        )
        stops_drop = df[["dropoff_stop_id","dropoff_lat","dropoff_lon"]].drop_duplicates().rename(
            columns={"dropoff_stop_id":"stop_id","dropoff_lat":"lat","dropoff_lon":"lon"}
        )
        stops = pd.concat([stops_pick, stops_drop]).drop_duplicates("stop_id").set_index("stop_id")
        # align to union order
        stops = stops.reindex(union_ids)

        # Vectorized haversine (meters) for all pairs we have coords for
        coords = stops[["lat","lon"]].to_numpy(float)
        lat = np.radians(coords[:,0])[:,None]
        lon = np.radians(coords[:,1])[:,None]
        latT, lonT = lat.T, lon.T
        dlat = latT - lat
        dlon = lonT - lon
        a = np.sin(dlat/2)**2 + np.cos(lat)*np.cos(latT)*np.sin(dlon/2)**2
        hav_m = 2 * 6371008.8 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))  # meters
        np.fill_diagonal(hav_m, 0.0)
        hav_df = pd.DataFrame(hav_m, index=union_ids, columns=union_ids)

        # Some stops may lack coords; mask those out
        valid_coord = stops[["lat","lon"]].notna().all(axis=1).values
        valid_pair_mask = np.outer(valid_coord, valid_coord)

        # ---- Distance imputation via learned detour factor ----
        dist_obs_mask = (~dist.isna()).values & valid_pair_mask & (hav_df.values > 0)
        ratios = (dist.where(dist_obs_mask) / hav_df.where(dist_obs_mask)).values.ravel()
        ratios = ratios[np.isfinite(ratios) & (ratios > 0)]
        detour = float(np.clip(np.median(ratios) if ratios.size else 1.25, 1.0, 2.0))

        dist_filled = dist.copy()
        fill_mask = dist_filled.isna().values & valid_pair_mask
        dist_filled.values[fill_mask] = (detour * hav_df.values)[fill_mask]
        # Optional: enforce symmetry (helps when data is sparse)
        sym = (dist_filled.values + dist_filled.values.T) / 2.0
        np.fill_diagonal(sym, 0.0)
        dist_filled = pd.DataFrame(sym, index=union_ids, columns=union_ids)

        # ---- Duration imputation via median speed ----
        speed_obs_mask = (~dur.isna()).values & (~dist.isna()).values & (dur.values > 0)
        speeds = (dist.where(speed_obs_mask) / dur.where(speed_obs_mask)).values.ravel()
        speeds = speeds[np.isfinite(speeds) & (speeds > 0)]
        median_speed_mps = float(np.median(speeds) if speeds.size else (30/3.6))

        dur_filled = dur.copy()
        dur_fill_mask = dur_filled.isna().values & np.isfinite(dist_filled.values)
        dur_filled.values[dur_fill_mask] = (
            dist_filled.values[dur_fill_mask] / median_speed_mps
        )
        np.fill_diagonal(dur_filled.values, 0.0)

        dist_path_f = out_dir / f"{cc}_distance_matrix_filled.csv"
        dur_path_f  = out_dir / f"{cc}_duration_matrix_filled.csv"
        dist_filled.to_csv(dist_path_f, float_format="%.3f")
        dur_filled.to_csv(dur_path_f,  float_format="%.3f")

        filled_paths = {"distance_filled": dist_path_f, "duration_filled": dur_path_f}

    print(f"{cc.upper()} union IDs: {len(union_ids)} ⇒ matrices {len(union_ids)}×{len(union_ids)} "
          f"(fill={fill})")
    return {
        "distance": dist_path,
        "duration": dur_path,
        **filled_paths
    }
    
