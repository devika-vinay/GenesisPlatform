import pandas as pd
from pathlib import Path
from scripts.run_etl import PIPELINES

def generate_distance_matrix(country:str) -> None:
    processed_dir = Path("data/processed")
    csv_path = processed_dir / f"{country}.csv"          

    if not csv_path.exists():                            
        raise FileNotFoundError(f"Expected file not found: {csv_path}")

    df = pd.read_csv(csv_path)

    # Convert metres ➔ kilometres if present
    if "distance_m" in df.columns:
        df["distance_km"] = df["distance_m"] / 1_000
        value_col = "distance_km"
    else:
        value_col = "distance_m"

    matrix_df = df.pivot(
        index="pickup_stop_id",
        columns="dropoff_stop_id",
        values=value_col,
    )

    out_path = processed_dir / f"{country}_distance_matrix.csv"
    matrix_df.to_csv(out_path, float_format="%.3f")
    print(f"Distance matrix saved → {out_path}", flush=True)

    