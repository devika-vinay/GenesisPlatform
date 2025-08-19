from __future__ import annotations
from pathlib import Path
from uuid import uuid4
from typing import Optional, Dict, Any

import pandas as pd
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from haversine import haversine  

from .api_loaders import (
    SUPPORTED, geocode,
    load_bookings_for_matching, pickup_candidates, dropoff_candidates,
    load_matrices, matrix_from_candidate_ids
)

from ..services.driver_matching import match_trips

router = APIRouter(prefix="/api", tags=["match"])

# Request/response schemas (expects a JSON dict body)
class MatchIn(BaseModel):
    country: str = Field(..., description="mx|co|cr")
    pickup_address: str
    dropoff_address: str
    vehicle_class: Optional[str] = Field(None, description="small|medium|large (optional)")

class MatchOut(BaseModel):
    pickup: Dict[str, Any]
    dropoff: Dict[str, Any]
    used_stops: Dict[str, Any]
    trip_estimate: Dict[str, Any]
    matched_driver: Dict[str, Any]

@router.post("/booking", response_model=MatchOut)
def book_and_match(body: MatchIn):
    cc = body.country.strip().lower()
    if cc not in SUPPORTED:
        raise HTTPException(status_code=400, detail=f"country must be one of {sorted(SUPPORTED)}")

    # 1) Geocode addresses -> coordinates
    pu_lat, pu_lon = geocode(cc, body.pickup_address)
    do_lat, do_lon = geocode(cc, body.dropoff_address)

    # 2) Load country bookings and find separate pickup/dropoff candidates near those coords
    df = load_bookings_for_matching(cc)
    # tolerance/limits are tunable; start with 150 m & top-10 candidates
    pu_cands = pickup_candidates(df, pu_lat, pu_lon, tol_m=150.0, k=10)
    do_cands = dropoff_candidates(df, do_lat, do_lon, tol_m=150.0, k=10)

    # 3) Try to form a matrix key from candidate stop IDs; else haversine fallback
    dist_df, dur_df = load_matrices(cc)
    d_km = t_min = None
    chosen_pu_stop = chosen_do_stop = None
    if not pu_cands.empty and not do_cands.empty:
        d_km, t_min, chosen_pu_stop, chosen_do_stop = matrix_from_candidate_ids(
            dist_df, dur_df, pu_cands, do_cands
        )

    if d_km is None or t_min is None:
        # No matrix pair found → fallback to haversine between the geocoded points
        d_km = round(haversine((pu_lat, pu_lon), (do_lat, do_lon)), 2)
        t_min = None
        source = "haversine"
    else:
        source = "matrix"

    # 4) Match a driver using your existing matcher (no changes to its logic)
    try:

        booking = {
            "booking_id": f"api-{uuid4()}",
            "move_size": (body.vehicle_class or "small").lower(),
            "pickup_lat":  pu_lat,
            "pickup_lon":  pu_lon,
            "dropoff_lat": do_lat,
            "dropoff_lon": do_lon,
        }
        assignments_fp = match_trips(cc, booking=booking)
        asg = pd.read_csv(assignments_fp)
        row = asg[asg["trip_id"] == booking["booking_id"]]
        if row.empty:
            raise HTTPException(status_code=404, detail="No feasible driver found")
        driver_id = str(row.iloc[0]["driver_id"])
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Driver matching failed: {e}")

    # 5) Load driver details for response
    drivers_fp = Path("data/processed") / "sample_drivers.csv"
    if not drivers_fp.exists():
        raise HTTPException(status_code=500, detail="sample_drivers.csv missing under data/processed")
    drv = pd.read_csv(drivers_fp)
    drow = drv[drv["driver_id"].astype(str) == driver_id]
    if drow.empty:
        raise HTTPException(status_code=500, detail="Matched driver not found in drivers CSV")
    d = drow.iloc[0].to_dict()

    return {
        "pickup":  {"address": body.pickup_address,  "lat": pu_lat, "lon": pu_lon},
        "dropoff": {"address": body.dropoff_address, "lat": do_lat, "lon": do_lon},
        "used_stops": {
            "pickup_stop_id": chosen_pu_stop,
            "dropoff_stop_id": chosen_do_stop
        },
        "trip_estimate": {
            "distance_km": d_km,
            "duration_min": t_min,
            "source": source
        },
        "matched_driver": {
            "driver_id": str(d.get("driver_id")),
            "country": d.get("country"),
            "city": d.get("city"),
            "base_location_lat": float(d.get("base_location_lat")),
            "base_location_lon": float(d.get("base_location_lon")),
            "capacity": d.get("capacity"),
            "avg_acceptance_rate": d.get("avg_acceptance_rate"),
            "avg_completion_rate": d.get("avg_completion_rate"),
        }
    }
