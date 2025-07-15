from pathlib import Path
import zipfile
import os, random, pandas as pd
from datetime import datetime, timedelta



def extract_gtfs_feeds(feeds: dict[str, str], raw_root: Path, work_root: Path) -> None:
    """
    Extract GTFS zip files into per-city folders.

    Parameters
    ----------
    feeds : dict
        {"cdmx": "gtfs.zip", "oaxaca": "semovi-oaxaca-mx.zip", ...}
    raw_root : Path
        Folder where the zip files live, e.g. data/raw/mx
    work_root : Path
        Scratch folder to unzip into, e.g. tmp/mx
    """
    work_root.mkdir(parents=True, exist_ok=True)

    for city, zip_name in feeds.items():
        zip_path = raw_root / zip_name
        city_dir = work_root / city

        if not zip_path.exists():
            print(f"{zip_name} not found in {raw_root}")
            continue

        city_dir.mkdir(exist_ok=True)
        with zipfile.ZipFile(zip_path) as z:
            z.extractall(city_dir)

        print(f"{city.upper()} extracted → {city_dir}")
        print("   Files:", ", ".join(os.listdir(city_dir)))


MOVE_SIZES = ["small", "medium", "large"]
NUM_BOOKINGS = 500

def _random_date(month: str = "2025-07") -> str:
    """Return a random YYYY-MM-DD HH:MM string inside the given month."""
    start = datetime.fromisoformat(f"{month}-01")
    end   = (start.replace(day=28) + timedelta(days=4)).replace(day=1)  # 1st of next month
    delta = end - start
    return (start + timedelta(days=random.randint(0, delta.days-1),
                              minutes=random.randint(0, 1439))
           ).strftime("%Y-%m-%d %H:%M")

def generate_bookings(city_dir: Path,
                             city_code: str,
                             num_bookings: int = NUM_BOOKINGS,
                             month: str = "2025-07") -> pd.DataFrame:
    """
      • reads <city_dir>/stops_truck_only.csv (must have stop_id column)
      • builds <city_dir>/booking_requests.csv
      • returns DataFrame of the simulated bookings
    """
    stops_csv = city_dir / "stops_truck_only.csv"
    if not stops_csv.exists():
        print(f"{city_code.upper()}: Truck stop file not found.")
        return pd.DataFrame()

    df_stops = pd.read_csv(stops_csv)
    if "stop_id" not in df_stops.columns:
        print(f"{city_code.upper()}: stop_id column missing.")
        return pd.DataFrame()

    stop_ids = df_stops["stop_id"].dropna().unique().tolist()
    if len(stop_ids) < 2:
        print(f"{city_code.upper()}: Not enough stops to generate bookings.")
        return pd.DataFrame()

    bookings = []
    for i in range(num_bookings):
        pickup, dropoff = random.sample(stop_ids, 2)
        bookings.append({
            "booking_id": f"{city_code.upper()}_BKG_{i+1:04d}",
            "pickup_stop_id": pickup,
            "dropoff_stop_id": dropoff,
            "requested_time": _random_date(month),
            "move_size": random.choice(MOVE_SIZES),
            "vehicle_type": "truck",
            "city": city_code,
        })

    out_csv = city_dir / "booking_requests.csv"
    pd.DataFrame(bookings).to_csv(out_csv, index=False)

    print(f"{city_code.upper()}: Created {len(bookings)} bookings → {out_csv}")
    return pd.DataFrame(bookings)
