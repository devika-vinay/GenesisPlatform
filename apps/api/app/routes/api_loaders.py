from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
import requests
from fastapi import HTTPException
from haversine import haversine  
from scripts.run_etl import PIPELINES

SUPPORTED: set[str] = set(PIPELINES.keys())

# File locations
DATA_DIR = Path("data/processed")

# ---------------------------------------------------------------------------
# Bookings table loaders / candidate selectors
# ---------------------------------------------------------------------------
@lru_cache(maxsize=8)
def load_bookings_for_matching(cc: str) -> pd.DataFrame:
    """
    Load data/processed/{cc}.csv once and normalize useful columns.
    Expected (at least): pickup_lat,pickup_lon,dropoff_lat,dropoff_lon
    Optional (nice-to-have for matrix addressing): pickup_stop_id,dropoff_stop_id
    """
    cc = cc.lower()
    fp = DATA_DIR / f"{cc}.csv"
    if not fp.exists():
        raise FileNotFoundError(f"Missing bookings file: {fp}")
    df = pd.read_csv(fp)
    # normalize stop IDs if present 
    for col in ("pickup_stop_id", "dropoff_stop_id"):
        if col in df.columns:
            df[col] = df[col].astype(str)
    return df


def pickup_candidates(df: pd.DataFrame, lat: float, lon: float,
                      tol_m: float = 150.0, k: int = 10) -> pd.DataFrame:
    """
    Return rows whose pickup is within tol_m of (lat, lon), ordered by proximity.
    Adds a helper column '_pu_km' (distance to pickup).
    """
    required = {"pickup_lat", "pickup_lon"}
    if not required.issubset(df.columns):
        return pd.DataFrame([])
    d_km = df.apply(lambda r: haversine((lat, lon), (r["pickup_lat"], r["pickup_lon"])), axis=1)
    out = df.assign(_pu_km=d_km)
    return out[out["_pu_km"] <= tol_m / 1000.0].nsmallest(k, "_pu_km")


def dropoff_candidates(df: pd.DataFrame, lat: float, lon: float,
                       tol_m: float = 150.0, k: int = 10) -> pd.DataFrame:
    """
    Return rows whose dropoff is within tol_m of (lat, lon), ordered by proximity.
    Adds a helper column '_do_km' (distance to dropoff).
    """
    required = {"dropoff_lat", "dropoff_lon"}
    if not required.issubset(df.columns):
        return pd.DataFrame([])
    d_km = df.apply(lambda r: haversine((lat, lon), (r["dropoff_lat"], r["dropoff_lon"])), axis=1)
    out = df.assign(_do_km=d_km)
    return out[out["_do_km"] <= tol_m / 1000.0].nsmallest(k, "_do_km")

# ---------------------------------------------------------------------------
# Load distance matrix 
# ---------------------------------------------------------------------------
@lru_cache(maxsize=8)
def load_matrices(cc: str) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """
    Load distance & duration matrices for a country (if present).
      data/processed/{cc}_distance_matrix.csv  (meters)
      data/processed/{cc}_duration_matrix.csv  (seconds)
    Returns (dist_df, dur_df) or (None, None) if either missing.
    """
    cc = cc.lower()
    dist_p = DATA_DIR / f"{cc}_distance_matrix.csv"
    dur_p = DATA_DIR / f"{cc}_duration_matrix.csv"
    if not dist_p.exists() or not dur_p.exists():
        return None, None

    dist = pd.read_csv(dist_p, index_col=0)
    dur = pd.read_csv(dur_p, index_col=0)
    # matrices are labeled by stop_id strings
    dist.index = dist.index.astype(str); dist.columns = dist.columns.astype(str)
    dur.index = dur.index.astype(str);   dur.columns = dur.columns.astype(str)
    return dist, dur


def matrix_from_candidate_ids(
    dist_df: Optional[pd.DataFrame],
    dur_df: Optional[pd.DataFrame],
    pu_cands: pd.DataFrame,
    do_cands: pd.DataFrame
) -> Tuple[Optional[float], Optional[int], Optional[str], Optional[str]]:
    """
    Try all combinations of candidate pickup_stop_id × dropoff_stop_id to find a valid
    distance/duration cell in the matrices.

    Returns:
      (distance_km, duration_min, chosen_pickup_stop_id, chosen_dropoff_stop_id)
    or:
      (None, None, None, None) if no valid cell exists or stop IDs are absent.
    """
    if dist_df is None or dur_df is None:
        return None, None, None, None
    if "pickup_stop_id" not in pu_cands.columns or "dropoff_stop_id" not in do_cands.columns:
        return None, None, None, None

    # Prefer closer stops first if helper columns exist
    if "_pu_km" in pu_cands:
        pu_ids = pu_cands.sort_values("_pu_km")["pickup_stop_id"].astype(str).unique().tolist()
    else:
        pu_ids = pu_cands["pickup_stop_id"].astype(str).dropna().unique().tolist()
    if "_do_km" in do_cands:
        do_ids = do_cands.sort_values("_do_km")["dropoff_stop_id"].astype(str).unique().tolist()
    else:
        do_ids = do_cands["dropoff_stop_id"].astype(str).dropna().unique().tolist()

    for pu in pu_ids:
        if pu not in dist_df.index:
            continue
        for do in do_ids:
            if do not in dist_df.columns:
                continue
            try:
                d_val = dist_df.at[pu, do]
                t_val = dur_df.at[pu, do]
            except KeyError:
                continue
            if pd.notna(d_val) and pd.notna(t_val):
                d_km = round(float(d_val) / 1000.0, 2)
                t_min = int(round(float(t_val) / 60.0))
                return d_km, t_min, pu, do

    return None, None, None, None

# ---------------------------------------------------------------------------
# Geocoding (OpenRouteService)
# ---------------------------------------------------------------------------
def geocode(
    country: str,
    address: str,
    focus: Optional[Tuple[float, float]] = None,
    size: int = 5,
    prefer_layers: Optional[List[str]] = None,
    bbox: Optional[Tuple[float, float, float, float]] = None,  # (min_lon, min_lat, max_lon, max_lat)
) -> Tuple[float, float]:
    """
    Geocoding with:
      - multiple candidates (size>1)
      - layer preference (address/street > locality/region)
      - proximity bias using a focus point
      - optional bounding box

    Returns: (lat, lon)
    """
    key = os.getenv("ORS_API_KEY")
    if not key:
        raise HTTPException(status_code=500, detail="ORS_API_KEY not set; cannot geocode")

    url = "https://api.openrouteservice.org/geocode/search"

    params = {
        "api_key": key,
        "text": address,
        "boundary.country": country.strip().upper(),
        "size": max(1, int(size)),
    }
    if prefer_layers:
        params["layers"] = ",".join(prefer_layers)
    if focus:
        params["focus.point.lat"] = focus[0]
        params["focus.point.lon"] = focus[1]
    if bbox:
        min_lon, min_lat, max_lon, max_lat = bbox
        params["boundary.rect.min_lon"] = min_lon
        params["boundary.rect.min_lat"] = min_lat
        params["boundary.rect.max_lon"] = max_lon
        params["boundary.rect.max_lat"] = max_lat

    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Geocoding error: {str(e)}")

    feats = data.get("features", [])
    if not feats:
        raise HTTPException(status_code=400, detail=f"Could not geocode '{address}' in {country.upper()}")

    # Score each candidate
    def feat_score(f) -> float:
        p = f.get("properties", {})
        conf = float(p.get("confidence", 0.0))
        layer = p.get("layer", "")
        acc = p.get("accuracy", "")

        layer_w = {
            "address": 1.0, "street": 0.9, "venue": 0.85, "neighbourhood": 0.6,
            "locality": 0.4, "county": 0.3, "region": 0.2, "country": 0.0
        }.get(layer, 0.2)

        acc_w = {
            "point": 1.0, "address": 0.9, "street": 0.8, "intersection": 0.75,
            "centroid": 0.2, "centroid_locality": 0.2, "centroid_region": 0.1
        }.get(acc, 0.3)

        prox = 0.0
        if focus:
            try:
                lon, lat = f["geometry"]["coordinates"]
                d_km = haversine((focus[0], focus[1]), (lat, lon))
                # full credit at 0 km, linearly decays to 0 by 10 km
                prox = max(0.0, 1.0 - min(d_km, 10.0) / 10.0)
            except Exception:
                prox = 0.0

        # weights tuned to strongly prefer precise layers & proximity
        return (conf * 1.0) + (layer_w * 1.0) + (acc_w * 0.5) + (prox * 0.5)

    best = max(feats, key=feat_score)
    lon, lat = best["geometry"]["coordinates"]
    return float(lat), float(lon)
    

def build_stops(df: pd.DataFrame) -> pd.DataFrame:
    pu = df[["pickup_stop_id","pickup_lat","pickup_lon"]].rename(
        columns={"pickup_stop_id":"stop_id","pickup_lat":"lat","pickup_lon":"lon"})
    do = df[["dropoff_stop_id","dropoff_lat","dropoff_lon"]].rename(
        columns={"dropoff_stop_id":"stop_id","dropoff_lat":"lat","dropoff_lon":"lon"})
    stops = pd.concat([pu, do], ignore_index=True).drop_duplicates("stop_id")
    # ensure types
    stops["lat"] = stops["lat"].astype(float)
    stops["lon"] = stops["lon"].astype(float)
    return stops.reset_index(drop=True)

def nearest_stop(stops: pd.DataFrame, lat: float, lon: float, radii_m=(150, 500, 1500, 5000)):
    # try progressively wider radii; return (stop_id, distance_m) or (None, None)
    for R in radii_m:
        # quick prefilter by bounding box (~1 deg ~ 111km) to avoid computing haversine for everything
        deg = R / 111_000.0
        cand = stops[(stops.lat.between(lat - deg, lat + deg)) & (stops.lon.between(lon - deg, lon + deg))].copy()
        if cand.empty:
            continue
        # compute great-circle distance
        cand["d_m"] = cand.apply(lambda r: haversine((lat, lon), (r.lat, r.lon))*1000.0, axis=1)
        cand = cand.nsmallest(1, "d_m")
        if not cand.empty and cand.iloc[0]["d_m"] <= R:
            r = cand.iloc[0]
            return str(r["stop_id"]), float(r["d_m"])
    return None, None

