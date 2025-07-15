from pathlib import Path
import pandas as pd

class ETLPipeline:
    def __init__(self, country_code: str):
        self.country = country_code

    def extract(self):
        raise NotImplementedError

    def transform(self, df):
        return df

    def load(self, df):
        out = Path(f"data/processed/{self.country}.csv")
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out, index=False)       
        print(f"Saved output → {out}")

    def run(self):
        df = self.extract()
        df = self.transform(df)
        self.load(df)
