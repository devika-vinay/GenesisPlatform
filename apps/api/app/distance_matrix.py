"""
Generate distance matrix from preprocessed booking data.
This script assumes that the input DataFrame `df` already contains:
- start_id: unique ID for pickup location
- end_id: unique ID for dropoff location
- distance_m: distance between the two points in kilometers
"""

import pandas as pd
import numpy as np

input_path = Path(f"data/processed/{cc}.csv")  # For cc in (mx, co, cr, …)
df = pd.read_csv(input_path)  #Load the preprocessed CSV containing stop-to-stop distances


def create_distance_matrix(df_filtered):
    stop_ids = pd.unique(df_filtered[['start_id', 'end_id']].values.ravel('K'))
    stop_ids.sort()  
    matrix = pd.DataFrame(np.nan, index=stop_ids, columns=stop_ids)  
    for _, row in df_filtered.iterrows():
        matrix.at[row['start_id'], row['end_id']] = row['distance_m']

    return matrix


matrix = distance_matrix(df)
out_csv = input_path.with_name(input_path.stem + "_matrix.csv")
matrix.to_csv(out_csv)

print(f"{input_path.name} → {out_csv.name} ✅ Saved distance matrix.")
