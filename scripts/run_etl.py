#!/usr/bin/env python
"""Generic ETL launcher.

Usage:
    python scripts/run_etl.py mx
    python scripts/run_etl.py cr
"""

import argparse
from pipelines.mx_preprocess import MexicoPipeline
from pipelines.cr_preprocess import CostaRicaPipeline
from pipelines.co_preprocess import ColombiaPipeline

PIPELINES = {
    "co": ColombiaPipeline("co"),
    "mx": MexicoPipeline("mx"),
    "cr": CostaRicaPipeline("cr"),
}

def main() -> None:
    parser = argparse.ArgumentParser(description="Run country ETL")
    parser.add_argument("country", choices=PIPELINES.keys(), help="Country code to process")
    args = parser.parse_args()

    pipeline = PIPELINES[args.country]
    pipeline.run()
    print(f"Finished {args.country} pipeline")

if __name__ == "__main__":
    main()
