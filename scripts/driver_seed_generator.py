import random
from pathlib import Path
import pandas as pd, random

# === CONFIGURATION ===
TOTAL_DRIVERS = 100  # total drivers to generate
OUTFILE = Path("data/processed/sample_drivers.csv")
RANDOM_SEED = 42  # change this for a different random dataset

# City bounding boxes (lat_min, lat_max, lon_min, lon_max)
city_bounds = {
    # Mexico
    ("cdmx", "MX"): (19.20, 19.50, -99.30, -98.90),
    ("guadalajara", "MX"): (20.55, 20.75, -103.45, -103.25),
    ("monterrey", "MX"): (25.60, 25.80, -100.40, -100.10),
    # Colombia
    ("bogota", "CO"): (4.50, 4.80, -74.20, -73.90),
    ("medellin", "CO"): (6.15, 6.35, -75.65, -75.50),
    ("cali", "CO"): (3.35, 3.55, -76.60, -76.45),
    # Costa Rica
    ("san_jose", "CR"): (9.85, 9.95, -84.15, -84.05),
    ("heredia", "CR"): (9.95, 10.05, -84.15, -84.05),
    ("alajuela", "CR"): (10.00, 10.10, -84.25, -84.15),
}

vehicle_types = ["truck", "van"]
capacities = ["small", "medium", "large"]


def driver_seed() -> Path:
    OUTFILE = Path("data/processed/sample_drivers.csv")
    OUTFILE.parent.mkdir(parents=True, exist_ok=True)
    random.seed(RANDOM_SEED)

    def random_name():
        first_names = ["Ana", "Carlos", "Jorge", "María", "Juan", "Luis", "Laura", "Pedro", "Lucía", "Miguel"]
        last_names = ["López", "Pérez", "Martínez", "Rodríguez", "Vargas", "González", "Hernández", "Ramírez", "Torres", "Flores"]
        return f"{random.choice(first_names)} {random.choice(last_names)}"

    # Distribute drivers evenly across cities
    cities_list = list(city_bounds.keys())
    drivers_per_city = TOTAL_DRIVERS // len(cities_list)
    extra_drivers = TOTAL_DRIVERS % len(cities_list)  # distribute remainder

    drivers = []
    driver_counter = 1

    for idx, (city, country) in enumerate(cities_list):
        count = drivers_per_city + (1 if idx < extra_drivers else 0)  # extras to first few cities
        lat_min, lat_max, lon_min, lon_max = city_bounds[(city, country)]

        for _ in range(count):
            driver_id = f"{country}_D{driver_counter:03d}"
            driver_name = random_name()
            vehicle_type = random.choice(vehicle_types)
            capacity = random.choice(capacities)
            base_lat = round(random.uniform(lat_min, lat_max), 6)
            base_lon = round(random.uniform(lon_min, lon_max), 6)
            years_exp = random.randint(1, 10)
            acceptance_rate = round(random.uniform(0.75, 0.98), 2)
            completion_rate = round(random.uniform(0.75, 0.98), 2)

            if country == "MX":
                base_fare = round(random.uniform(120, 160), 2)
                price_per_km = round(random.uniform(8, 12), 2)
            elif country == "CO":
                base_fare = round(random.uniform(110, 140), 2)
                price_per_km = round(random.uniform(7, 10), 2)
            else:  # CR
                base_fare = round(random.uniform(90, 120), 2)
                price_per_km = round(random.uniform(6, 9), 2)

            drivers.append([
                driver_id, driver_name, city, country, vehicle_type, capacity,
                base_lat, base_lon, years_exp, acceptance_rate, completion_rate,
                base_fare, price_per_km
            ])
            driver_counter += 1

    # Create DataFrame
    columns = [
        "driver_id", "driver_name", "city", "country", "vehicle_type", "capacity",
        "base_location_lat", "base_location_lon", "years_experience",
        "avg_acceptance_rate", "avg_completion_rate", "base_fare", "price_per_km"
    ]
    df_drivers = pd.DataFrame(drivers, columns=columns)

    # Save to CSV
    df_drivers.to_csv(OUTFILE, index=False)
    print(f"Driver dataset with {TOTAL_DRIVERS} drivers saved to {OUTFILE} using seed {RANDOM_SEED}")
    return OUTFILE
