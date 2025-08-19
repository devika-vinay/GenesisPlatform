from pathlib import Path
import random
from datetime import timedelta
import pandas as pd
from haversine import haversine

CAP_RANK = {"small": 1, "medium": 2, "large": 3}

def generate_trip_logs(country: str) -> Path:
    """
    Create <country>_mock_trip_logs.csv in data/processed/ and return the file path.

    Parameters
    ----------
    country : str   # "mx", "co", "cr" (case-insensitive)
    days    : int   # how many days of history to fabricate
    seed    : int   # random-seed for reproducibility
    """
    days = 7
    seed = 42
    random.seed(seed)
    country = country.lower()

    data_dir = Path("data/processed")
    drivers  = pd.read_csv(data_dir / "sample_drivers.csv")\
                 .query("country == @country.upper()")
    bookings = pd.read_csv(data_dir / f"{country}.csv")

    start_date = pd.Timestamp.utcnow().normalize() - pd.Timedelta(days=days-1)

    trips, trip_counter = [], 1
    for day in range(days):                             
        bk_date = start_date + pd.Timedelta(days=day)
        for _, bk in bookings.iterrows():                
                ts = pd.to_datetime(bk.requested_time).time()
                requested_dt = pd.Timestamp.combine(bk_date, ts)
                # ----- pick driver --------------------------------------------------
                feas = drivers[
                    (drivers.city == bk.city) &
                    (drivers.capacity.apply(lambda c: CAP_RANK[c] >= CAP_RANK[bk.move_size]))
                ]
                if feas.empty:                      # fallback: any capacity-ok driver
                    feas = drivers[
                        drivers.capacity.apply(lambda c: CAP_RANK[c] >= CAP_RANK[bk.move_size])
                    ]
                row = feas.sample(1, random_state=seed).iloc[0]

                distance_km = None
                duration_min = None
                end_dt = ""
                # distance_km: use ORS value if present, otherwise haversine fallback
                if not pd.isna(bk["distance_m"]):
                    distance_km = round(bk["distance_m"] / 1000, 2)
                else:
                    distance_km = round(haversine(
                        (bk.pickup_lat, bk.pickup_lon),
                        (bk.dropoff_lat, bk.dropoff_lon)
                    ), 2)

                # duration_min + end_time only if ORS duration is available
                if not pd.isna(bk["duration_s"]):
                    duration_min = int(round(bk["duration_s"] / 60))
                    end_dt = (requested_dt + timedelta(minutes=duration_min)).strftime("%Y-%m-%d %H:%M")

                trip_id = f"{country.upper()}_T{trip_counter:05d}"
                trip_counter += 1
                status = "cancelled" if random.random() < 0.07 else "completed"

                trips.append({
                    "trip_id"     : trip_id,
                    "driver_id"   : row.driver_id,
                    "booking_id"  : bk.booking_id,
                    "pickup_lat"  : bk.pickup_lat,
                    "pickup_lon"  : bk.pickup_lon,
                    "dropoff_lat" : bk.dropoff_lat,
                    "dropoff_lon" : bk.dropoff_lon,
                    "distance_km" : distance_km,
                    "duration_min": duration_min,
                    "start_time"  : requested_dt.strftime("%Y-%m-%d %H:%M"),
                    "end_time"    : end_dt,
                    "status"      : status,
                    "vehicle_type": row.vehicle_type,
                    "capacity"    : row.capacity,
                    "move_size"  : bk.move_size,
                })

        out_fp = data_dir / f"{country}_mock_trip_logs.csv"
        pd.DataFrame(trips).to_csv(out_fp, index=False)
        print(f"saved → {out_fp}")        
        return out_fp
