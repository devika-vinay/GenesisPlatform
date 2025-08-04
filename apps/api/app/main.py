"""
1. Runs the business logic
"""

import os
from scripts.driver_seed_generator import driver_seed
from scripts.run_etl import PIPELINES           
from services.compute_distance import enrich_country
from scripts.trip_seed_generator import generate_trip_logs
from services.driver_matching import match_trips


def run_single(country: str):
     # 1) Build / refresh the driver sample 
    driver_seed()

    # 2) Run filtering logic for truck stops (preprocessing) 
    PIPELINES[country].run()

    # 3) Compute distances calling ORS wrapper 
    if os.getenv("ORS_API_KEY"):
        enrich_country(country)
    
    # 4) Build trip logs sample
    generate_trip_logs(country)

    # 5) Match drivers to trips
    match_trips(country)

def run_all():
    for cc in PIPELINES.keys():
        run_single(cc)

if __name__ == "__main__":
    country = os.getenv("COUNTRY")
    if country:
        run_single(country)
    else:
        run_all()

    print("All tasks completed", flush=True)
