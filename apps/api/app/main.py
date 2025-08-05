"""
1. Runs the business logic
"""

import os
from scripts.driver_seed_generator import driver_seed
from scripts.run_etl import PIPELINES           
from services.compute_distance import enrich_country
from services.distance_matrix import generate_distance_matrix

def run_single(country: str):
     # 1) Build / refresh the driver sample 
    driver_seed()

    # 2) Run filtering logic for truck stops (preprocessing) 
    PIPELINES[country].run()

    # 3) Compute distances calling ORS wrapper 
    if os.getenv("ORS_API_KEY"):
        enrich_country(country)
    
    # 4) Compute distance matrix
    generate_distance_matrix(country)

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
