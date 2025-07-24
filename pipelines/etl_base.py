# -------------------
# • Every ETL (Extract‑Transform‑Load) job follows the same three steps.
# • Instead of copy‑pasting that skeleton into each pipeline, we define
#   it once here.  Country‑specific subclasses (MexicoPipeline,
#   ColombiaPipeline, etc.) inherit this class and simply override
#   the parts that differ
# --------------------------------------------------------------------
from pathlib import Path
import pandas as pd


class ETLPipeline:

    def __init__(self, country_code: str):
        # Used only for naming the final CSV (data/processed/<cc>.csv)
        self.country = country_code

    # 1) Extract
    #    • Download, unzip, or otherwise collect raw data.
    #    • Must return something that the next step can work with. 
    def extract(self):
        raise NotImplementedError   # forces child class to implement

    # 2) Transform
    #    • Clean / filter / enrich the data produced by extract().
    def transform(self, df):
        return df

    # 3) Load
    #    • Persist the final DataFrame to output
    #    • The base class handles folder creation & logging so every
    #      pipeline gets a uniform output location.
    def load(self, df):
        out = Path(f"data/processed/{self.country}.csv")
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out, index=False)
        print(f"Saved output → {out}")

    # Convenience wrapper: run the full ETL in order, called by run_etl.py
    def run(self):
        df = self.extract()       # E
        df = self.transform(df)   # T
        self.load(df)             # L
