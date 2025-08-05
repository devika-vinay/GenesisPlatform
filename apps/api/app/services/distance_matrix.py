import pandas as pd
from pathlib import Path

def generate_distance_matrix(df: pd.DataFrame) -> pd.DataFrame:
    # Convert meters to km if needed
    if 'distance_m' in df.columns:
        df['distance_km'] = df['distance_m'] / 1000.0
    return df.pivot(index='pickup_stop_id', columns='dropoff_stop_id', values='distance_km')

if __name__ == "__main__":
    processed_dir = Path("data/processed")
    csv_files = [f for f in processed_dir.glob("*.csv") if f.stem in ["co", "mx", "cr"]]

    if not csv_files:
        print("No processed CSV files found in data/processed/.")
        exit()

    for csv_file in csv_files:
        print(f"\n=== Generating matrix for {csv_file.name} ===")
        df = pd.read_csv(csv_file)

        matrix_df = generate_distance_matrix(df)
        matrix_path = processed_dir / f"{csv_file.stem}_distance_matrix.csv"
        matrix_df.to_csv(matrix_path, float_format="%.3f")

        print(f"Saved distance matrix → {matrix_path}")
