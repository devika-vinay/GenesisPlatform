# Country‑specific ETL for cr Costa Rica
# ------------------------------------
# • Steps per city:
#     1. Extract GTFS feeds  → tmp/co/<city>/ (exists in the docker container)
#     2. Clip national OSM PBF to city BBOX (once) → <city>_clip.osm.pbf
#     3. Build / load road network cache  → <city>_roads.parquet
#     4. Classify each road edge into truck size  (small / medium / large)
#     5. Spatial‑join buffered GTFS stops to road edges
#     6. Keep only stops reachable by a truck; write stops_truck_only.csv
#     7. Generate 500 synthetic bookings per city  → booking_requests.csv
# • Returns a single DataFrame so the ETL “load” step can concatenate
#   everything into data/processed/co.csv
# ---------------------------------------------------------------------
from pathlib import Path
import subprocess
import pandas as pd, geopandas as gpd
from shapely.geometry import Point
from pyrosm import OSM
from .etl_base import ETLPipeline
from .process_helper import extract_gtfs_feeds, generate_bookings

class CostaRicaPipeline(ETLPipeline):
    # -----------------------------------------------------------------
    #   List_of_zip_files, BBOX (W,S,E,N), booking‑tag
    # -----------------------------------------------------------------
    CITY_META = {
        "costarica":    (["CR1.zip", "CR2.zip"],         
                         (-86.00, 5.25, -82.50, 11.50), "cor")}

    # National‑level OSM extract 
    RAW_PBF   = Path("data/raw/cr/costa-rica-latest.osm.pbf")

    # Noise columns we never use
    DROP  = {"stop_code","zone_id","stop_url","parent_station",
             "stop_desc","stop_timezone","level_id","platform_code"}

    # Only these road classes end up in the filtered stop file
    TRUCK_CLASSES = {"small","medium","large"}

    # Highway tags we consider drivable
    HIGHWAYS = ["trunk","trunk_link","primary","primary_link",
    "secondary","secondary_link","tertiary","tertiary_link",
    "services","residential","service","unclassified","track",
    "turning_circle","turning_loop","passing_place","rest_area"]

    # -----------------------------------------------------------------
    # Initialisation – make sure tmp/co/ exists
    # -----------------------------------------------------------------
    def __init__(self, cc: str = "cr"):
        super().__init__(cc)
        self.tmp = Path("tmp") / cc
        self.tmp.mkdir(parents=True, exist_ok=True)

    # -----------------------------------------------------------------
    # Extract step
    # -----------------------------------------------------------------
    def extract(self) -> None:
        raw_root = Path("data/raw/cr")

        for code, (zip_list, bbox, _) in self.CITY_META.items():
            # 1) unzip every GTFS archive into the same city folder
            for zip_name in zip_list:
                extract_gtfs_feeds({code: zip_name}, raw_root, self.tmp)

            # 2) clip national PBF once per city (expensive but cached for subsequent builds)
            clip = self.tmp / f"{code}_clip.osm.pbf"
            if not clip.exists():
                subprocess.run(
                    ["osmium", "extract",
                    "--bbox", ",".join(map(str, bbox)),
                    str(self.RAW_PBF), "-o", str(clip)],
                    check=True)
                
    # -----------------------------------------------------------------
    # Transform step
    # -----------------------------------------------------------------
    def transform(self, _) -> pd.DataFrame:
        all_booking_frames = []

        for code, (_, bbox, tag) in self.CITY_META.items():
            city_dir = self.tmp / code

            # 1) load GTFS stops
            stops_files = list(city_dir.rglob("**/stops.txt")) + \
                          list(city_dir.rglob("**/stops-*.txt"))
            if not stops_files:
                print(f"{code.upper()}: no stops.txt – skipped", flush=True)
                continue
            
            df = pd.concat(map(pd.read_csv, stops_files), ignore_index=True)
            df = df.drop(columns=[c for c in self.DROP if c in df.columns])

            # Buffer each stop 70 m in EPSG:3857 then back to 4326
            gdf = gpd.GeoDataFrame(
                df, geometry=[Point(xy) for xy in zip(df.stop_lon, df.stop_lat)],
                crs="EPSG:4326"
            ).to_crs(3857)
            gdf["geometry"] = gdf.buffer(70).to_crs(4326)

            # 2) build road network cache 
            clip = self.tmp / f"{code}_clip.osm.pbf"
            cache = self.tmp / f"{code}_roads.parquet"

            if cache.exists():
                roads = gpd.read_parquet(cache)
            else:
                roads = OSM(str(clip)).get_data_by_custom_criteria(
                            custom_filter={"highway": True},
                            filter_type="keep",
                            keep_nodes=False,
                            extra_attributes=["access","hgv","maxweight"])
                roads.to_parquet(cache)

            roads = roads.to_crs("EPSG:4326")
            roads = roads[roads["highway"].isin(self.HIGHWAYS)]

            # 3) classify each road based on specific conditions
            def classify(r):
                acc = str(r.get("access", "")).lower()
                hgv = str(r.get("hgv", "")).lower()
                acc = str(r.get("access", "")).lower()
                hgv = str(r.get("hgv", "")).lower()
                # Explicit bans
                if acc in {"no", "private", "agricultural"} or hgv == "no":
                    return "forbidden"

                # Weight‑based rule if tag exists
                try:
                    w = float(str(r.get("maxweight", "")).lower().replace("t", ""))
                    return "small" if w <= 3.5 else "medium" if w <= 7.5 else "large"
                except (ValueError, TypeError):
                    pass

                # Fallback: infer from highway class
                hw = str(r.get("highway", "")).lower()

                if hw in {"trunk", "trunk_link", "primary", "primary_link"}:
                    return "large"
                if hw in {"secondary", "secondary_link", "tertiary", "tertiary_link", "services"}:
                    return "medium"
                if hw in {"residential", "service", "track",
                        "turning_circle", "turning_loop", "passing_place", "rest_area"}:
                    return "small"

                return "forbidden"

            roads["classify_truck"] = roads.apply(classify, axis=1)

            # 4) spatial join stop ↔ road
            joined = gpd.sjoin(gdf, roads[["geometry","classify_truck"]],
                               predicate="intersects", how="left")
            filtered = (joined[joined["classify_truck"].isin(self.TRUCK_CLASSES)]
                        .drop_duplicates("stop_id"))

            # Persist filtered stops for re‑runs
            out_stops = city_dir / "stops_truck_only.csv"
            out_stops.write_bytes(filtered.to_csv(index=False).encode())

            # 5) generate synthetic bookings 
            bookings = generate_bookings(city_dir, tag)
            all_booking_frames.append(bookings)

            print(f"{code.upper()}: {len(filtered)} stops, "
                  f"{len(bookings)} bookings", flush=True)

        # 6) return combined DataFrame to load()
        if all_booking_frames:
            return pd.concat(all_booking_frames, ignore_index=True)

        # If we reach here something went wrong upstream
        raise RuntimeError("No cities processed")
