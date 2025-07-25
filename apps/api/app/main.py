"""
1. Runs the business logic
"""

import os
from scripts.run_etl import PIPELINES           
from services.compute_distance import enrich_country

def run_single(country: str):
    PIPELINES[country].run() # Calling preprocess files of each country
    if os.getenv("ORS_API_KEY"):
        enrich_country(country) # Calling compute_distance.py to compute distances between stops

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
