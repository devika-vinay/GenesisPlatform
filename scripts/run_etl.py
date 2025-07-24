# ---------------------------------------------------------------------
# This launcher is just a convenient “traffic controller” that picks 
# the right class and calls .run().
# • Called by "docker compose up genesis" via entrypoint.sh
# • Add a new country: 1) import its class 2) extend pipelines dict
# ---------------------------------------------------------------------

import argparse
from pipelines.mx_preprocess import MexicoPipeline
from pipelines.cr_preprocess import CostaRicaPipeline
from pipelines.co_preprocess import ColombiaPipeline

# Registry: country‑code → instantiated pipeline 
PIPELINES = {
    "co": ColombiaPipeline("co"),   # 🇨🇴 Colombia
    "mx": MexicoPipeline("mx"),     # 🇲🇽 Mexico
    "cr": CostaRicaPipeline("cr"),  # 🇨🇷 Costa Rica
}

# Parse CLI arg → run chosen pipeline → print summary
def main() -> None:
    parser = argparse.ArgumentParser(description="Run one country ETL")
    parser.add_argument(
        "country",
        choices=PIPELINES.keys(),   # limits input to co/mx/cr
        help="Country code to process (co | mx | cr)",
    )
    args = parser.parse_args()

    pipeline = PIPELINES[args.country]  
    pipeline.run()                      # E‑T‑L in one call
    print(f"Finished {args.country} pipeline")  # friendly log line


if __name__ == "__main__":
    main()
