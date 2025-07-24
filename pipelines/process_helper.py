# ---------------------------------------------------------------------
# Shared utilities
# 1. extract_gtfs_feeds()
#    • Unzips one or more GTFS feeds into tmp/<country>/<city>/.
#
# 2. generate_bookings()
#    • Creates synthetic jobs for a city.
#    • Uses classify_truck info (if present) to pick the smallest
#      vehicle that can serve both stops.
#    • Falls back to "small" when a stop has no class tag 
# ---------------------------------------------------------------------

from pathlib import Path
import zipfile
import os, random, pandas as pd
from datetime import datetime, timedelta


def extract_gtfs_feeds(feeds: dict[str, str], raw_root: Path, work_root: Path) -> None:

    work_root.mkdir(parents=True, exist_ok=True)

    for city, zip_name in feeds.items():
        zip_path = raw_root / zip_name          # full path to archive
        city_dir = work_root / city             # where we will unzip

        if not zip_path.exists():
            print(f"{zip_name} not found in {raw_root}")
            continue

        city_dir.mkdir(exist_ok=True)
        with zipfile.ZipFile(zip_path) as z:
            z.extractall(city_dir)

        print(f"{city.upper()} extracted → {city_dir}")
        print("   Files:", ", ".join(os.listdir(city_dir)))



MOVE_SIZES = ["small", "medium", "large"]          
TRUCK_PRIORITY = ["small", "medium", "large"]      
NUM_BOOKINGS = 500 # Number of synthetic bookings to be created


def _least_restrictive(pickup_cls: str, drop_cls: str) -> str:
    # Return the smallest truck class that can serve both stops.
    try:
        i = TRUCK_PRIORITY.index(pickup_cls)
        j = TRUCK_PRIORITY.index(drop_cls)
        return TRUCK_PRIORITY[min(i, j)]
    except ValueError:          # unknown tag → default to small
        return "small"
    

def generate_bookings(city_dir: Path, city_tag: str,
                      num_bookings: int = NUM_BOOKINGS) -> pd.DataFrame:
    
    stops_path = city_dir / "stops_truck_only.csv"
    if not stops_path.exists():
        raise FileNotFoundError(f"{stops_path} not found")

    df_stops = pd.read_csv(stops_path)
    stop_ids = df_stops["stop_id"].tolist()

    if "classify_truck" in df_stops.columns:
        stop_to_cls = dict(zip(df_stops["stop_id"], df_stops["classify_truck"]))
    else:
        stop_to_cls = {sid: "small" for sid in stop_ids}

    if len(stop_ids) < 2:
        print(f"{city_tag.upper()}: Not enough stops to generate bookings.")
        return pd.DataFrame()

    # Random timestamp helper 
    def rand_time():
        start = datetime(2025, 7, 1)
        end   = datetime(2025, 7, 31)
        delta = end - start
        return (start + timedelta(
            days=random.randint(0, delta.days),
            minutes=random.randint(0, 24 * 60)
        )).strftime("%Y-%m-%d %H:%M")

    # Generate rows 
    bookings = []
    for i in range(num_bookings):
        pickup, dropoff = random.sample(stop_ids, 2)
        pickup_cls  = stop_to_cls.get(pickup, "small")
        dropoff_cls = stop_to_cls.get(dropoff, "small")

        booking = {
            "booking_id": f"{city_tag.upper()}_BKG_{i+1:04d}",
            "pickup_stop_id":  pickup,
            "dropoff_stop_id": dropoff,
            "requested_time":  rand_time(),
            "move_size": random.choice(MOVE_SIZES),
            "pickup_truck_type":  pickup_cls,
            "dropoff_truck_type": dropoff_cls,
            "required_truck_type": _least_restrictive(pickup_cls, dropoff_cls),
            "city": city_tag,
        }
        bookings.append(booking)

    # Persist + return 
    out_csv = city_dir / "booking_requests.csv"
    pd.DataFrame(bookings).to_csv(out_csv, index=False)

    print(f"{city_tag.upper()}: Created {len(bookings)} bookings → {out_csv}")
    return pd.DataFrame(bookings)
