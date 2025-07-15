from pathlib import Path
from .process_helper import extract_gtfs_feeds, generate_bookings
from .etl_base import ETLPipeline
import pandas as pd


class MexicoPipeline(ETLPipeline):
    feeds = {
        "cdmx": "gtfs.zip",
        "oaxaca": "semovi-oaxaca-mx.zip",
    }

    def __init__(self, country_code: str):
        super().__init__(country_code)
        self.tmp_root = Path("tmp") / country_code

    def extract(self):
        extract_gtfs_feeds(self.feeds,
                           Path("data/raw/mx"),
                           self.tmp_root)
        return None

    def transform(self, _: pd.DataFrame = None) -> pd.DataFrame:
        """
        1. For each city folder in tmp/mx,
        2. read stops.txt,
        3. filter to truck-navigable streets + wheelchair-accessible,
        4. write 'stops_truck_only.csv',
        5. generate booking requests CSV and return a combined DataFrame.
        """
        # Keywords that imply truck-navigable roads
        keywords = ["Av", "Avenida", "Calzada", "Boulevard", "Circuito", "Carretera", "Calle", "Eje", "Autopista"]

        combined = []

        for city_dir in self.tmp_root.iterdir():
            stops_txt = city_dir / "stops.txt"
            if not stops_txt.exists():
                print(f"{city_dir.name.upper()}: stops.txt not found.")
                continue

            df = pd.read_csv(stops_txt)

            #  Filter by street keywords
            filtered = df[df["stop_name"].str.contains(
                "|".join(keywords), case=False, na=False
            )]

            # Wheelchair boarding == 1  (if column exists)
            if "wheelchair_boarding" in filtered.columns:
                filtered = filtered[filtered["wheelchair_boarding"] == 1]

            df.loc[:, "vehicle_type"] = "truck"

            out_path = city_dir / "stops_truck_only.csv"
            filtered.to_csv(out_path, index=False)
            print(f"{city_dir.name.upper()}: Saved {len(filtered)} stops → {out_path.name}")

            # Booking request simulation 
            df_bkg = generate_bookings(city_dir, city_dir.name)
            if not df_bkg.empty:
                combined.append(df_bkg)

        return pd.concat(combined, ignore_index=True) if combined else pd.DataFrame()
        
