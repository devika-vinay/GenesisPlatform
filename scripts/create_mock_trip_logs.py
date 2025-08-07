import pandas as pd
import random
from datetime import datetime, timedelta
from pathlib import Path

# === Paths ===
drivers_path = Path("data/processed/sample_drivers.csv")
output_dir = Path("data/processed")

# === Parameters ===
TRIPS_PER_DRIVER = 5
AVG_SPEED_KMPH = 30
TRIP_DATE = datetime(2025, 7, 24)  # You can randomize this if needed

# === Load drivers ===
drivers = pd.read_csv(drivers_path)

# === Get list of countries ===
countries = drivers["country"].unique()

for country_code in countries:
    print(f"Generating trips for country: {country_code}")

    country_drivers = drivers[drivers["country"] == country_code]
    trip_logs = []

    for i, row in country_drivers.iterrows():
        for trip_num in range(TRIPS_PER_DRIVER):
            driver_id = row["driver_id"]
            vehicle_type = row["vehicle_type"]
            capacity = row["capacity"]
            base_lat = row["base_location_lat"]
            base_lon = row["base_location_lon"]

            # Random pickup/dropoff points
            pickup_lat = round(base_lat + random.uniform(-0.03, 0.03), 6)
            pickup_lon = round(base_lon + random.uniform(-0.03, 0.03), 6)
            dropoff_lat = round(pickup_lat + random.uniform(0.005, 0.02), 6)
            dropoff_lon = round(pickup_lon + random.uniform(0.005, 0.02), 6)

            distance_km = round(random.uniform(5, 20), 2)
            duration_min = round((distance_km / AVG_SPEED_KMPH) * 60, 1)

            # Start time during day (6:00–18:00)
            start_time = TRIP_DATE + timedelta(minutes=random.randint(360, 1080))
            end_time = start_time + timedelta(minutes=duration_min)

            # Status
            status = random.choices(["completed", "cancelled"], weights=[90, 10])[0]

            # IDs
            trip_index = i * TRIPS_PER_DRIVER + trip_num + 1
            trip_id = f"{country_code}_T{str(trip_index).zfill(4)}"
            booking_id = f"{country_code}_BKG_{str(trip_index).zfill(4)}"

            trip_logs.append({
                "trip_id": trip_id,
                "driver_id": driver_id,
                "booking_id": booking_id,
                "pickup_lat": pickup_lat,
                "pickup_lon": pickup_lon,
                "dropoff_lat": dropoff_lat,
                "dropoff_lon": dropoff_lon,
                "distance_km": distance_km,
                "duration_min": duration_min,
                "start_time": start_time.strftime("%Y-%m-%d %H:%M"),
                "end_time": end_time.strftime("%Y-%m-%d %H:%M"),
                "status": status,
                "vehicle_type": vehicle_type,
                "capacity": capacity,
            })

    # Save to CSV
    df = pd.DataFrame(trip_logs)
    output_path = output_dir / f"trip_logs_{country_code.lower()}.csv"
    df.to_csv(output_path, index=False)
    print(f"Saved {len(df)} trips to → {output_path}")