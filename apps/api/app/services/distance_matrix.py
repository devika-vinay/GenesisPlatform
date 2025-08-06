import pandas as pd
from pathlib import Path
from scripts.run_etl import PIPELINES

def generate_distance_matrix(country: str) -> None:
    processed_dir = Path("data/processed")
    csv_path = processed_dir / f"{country}.csv"          

    if not csv_path.exists():                            
        raise FileNotFoundError(f"Expected file not found: {csv_path}")

    df = pd.read_csv(csv_path)

    # === Newly added section ===
    from routes.openrouteservice_wrapper import OpenRouteServiceWrapper
    import time
    
    ORS = OpenRouteServiceWrapper()
    RATE_LIMIT_DELAY = 0.3  # seconds

    # Create a unique stop set by combining pickup and dropoff points
    stops = pd.concat([
        df[['pickup_stop_id', 'pickup_lat', 'pickup_lon']].rename(columns={
            'pickup_stop_id': 'stop_id',
            'pickup_lat': 'lat',
            'pickup_lon': 'lon'
        }),
        df[['dropoff_stop_id', 'dropoff_lat', 'dropoff_lon']].rename(columns={
            'dropoff_stop_id': 'stop_id',
            'dropoff_lat': 'lat',
            'dropoff_lon': 'lon'
        })
    ]).drop_duplicates(subset=['stop_id']).set_index('stop_id')

    stop_ids = stops.index.tolist()
    matrix_data = []

    for from_id in stop_ids:
        row_data = []
        from_coord = (stops.loc[from_id, 'lon'], stops.loc[from_id, 'lat'])
        for to_id in stop_ids:
            if from_id == to_id:
                row_data.append(0.0)  # same stop -> distance 0
                continue
            res = ORS.get_route(from_coord, (stops.loc[to_id, 'lon'], stops.loc[to_id, 'lat']))
            if "error" in res:
                print(f"Error {from_id}->{to_id}: {res['error']}")
                row_data.append(None)
            else:
                row_data.append(res["distance_m"] / 1000)  # in kilometers
            time.sleep(RATE_LIMIT_DELAY)
        matrix_data.append(row_data)

    matrix_df = pd.DataFrame(matrix_data, index=stop_ids, columns=stop_ids)
    # === End of newly added section ===

    out_path = processed_dir / f"{country}_distance_matrix.csv"
    matrix_df.to_csv(out_path, float_format="%.3f")
    print(f"Distance matrix saved → {out_path}", flush=True)
