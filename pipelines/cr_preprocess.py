import pandas as pd
from .etl_base import ETLPipeline

class CostaRicaPipeline(ETLPipeline):
    def extract(self):
        return pd.read_csv('data/raw/cr/bookings.csv')

    def transform(self, df):
        # TODO: implement transformations
        return df
