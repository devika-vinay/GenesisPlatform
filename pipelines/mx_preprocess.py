# pipelines/co_preprocess.py
from pathlib import Path
import subprocess
import pandas as pd, geopandas as gpd
from shapely.geometry import Point
from pyrosm import OSM
from .etl_base import ETLPipeline
from .process_helper import extract_gtfs_feeds, generate_bookings

class MexicoPipeline(ETLPipeline):
    CITY_META = {
    # code           zips                             (W,   S,    E,    N)     tag
    "cdmx":         (["gtfs.zip"],                    (-99.364, 19.020, -98.940, 19.592), "cdmx"),
    "oaxaca":       (["semovi-oaxaca-mx.zip"],        (-96.789, 16.990,	-96.638, 17.145), "oaxaca"),
}

    RAW_PBF   = Path("data/raw/mx/mexico-latest.osm.pbf")

    DROP  = {"stop_code","zone_id","stop_url","parent_station",
             "stop_desc","stop_timezone","level_id","platform_code"}

    TRUCK_CLASSES = {"small","medium","large"}
    HIGHWAYS = [
    "trunk","trunk_link","primary","primary_link",
    "secondary","secondary_link","tertiary","tertiary_link",
    "services","residential","service","unclassified","track",
    "turning_circle","turning_loop","passing_place","rest_area"]


    def __init__(self, cc: str = "mx"):
        super().__init__(cc)
        self.tmp = Path("tmp") / cc
        self.tmp.mkdir(parents=True, exist_ok=True)

    def extract(self) -> None:
        raw_root = Path("data/raw/mx")
        # Unzip GTFS or copy raw stops
        for code, (zip_list, bbox, _) in self.CITY_META.items():
            for zip_name in zip_list:
                extract_gtfs_feeds({code: zip_name}, raw_root, self.tmp)

            # Build city‑specific clip if missing
            clip = self.tmp / f"{code}_clip.osm.pbf"
            if not clip.exists():
                subprocess.run(
                    ["osmium", "extract",
                    "--bbox", ",".join(map(str, bbox)),
                    str(self.RAW_PBF), "-o", str(clip)],
                    check=True)

    def transform(self, _) -> pd.DataFrame:
        all_booking_frames = []

        for code, (_, bbox, tag) in self.CITY_META.items():
            city_dir = self.tmp / code

            # Gather stops
            stops_files = list(city_dir.rglob("**/stops.txt")) + \
                          list(city_dir.rglob("**/stops-*.txt"))
            if not stops_files:
                print(f"{code.upper()}: no stops.txt – skipped")
                continue
            
            df = pd.concat(map(pd.read_csv, stops_files), ignore_index=True)
            df = df.drop(columns=[c for c in self.DROP if c in df.columns])

            gdf = gpd.GeoDataFrame(
                df, geometry=[Point(xy) for xy in zip(df.stop_lon, df.stop_lat)],
                crs="EPSG:4326"
            ).to_crs(3857)
            gdf["geometry"] = gdf.buffer(70).to_crs(4326)

            # Load / build road cache for this city
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

            # Classify truck access
            def classify(r):
                acc = str(r.get("access", "")).lower()
                hgv = str(r.get("hgv", "")).lower()
                acc = str(r.get("access", "")).lower()
                hgv = str(r.get("hgv", "")).lower()
                # Explicit bans
                if acc in {"no", "private", "agricultural"} or hgv == "no":
                    return "forbidden"

                # Maxweight
                try:
                    w = float(str(r.get("maxweight", "")).lower().replace("t", ""))
                    return "small" if w <= 3.5 else "medium" if w <= 7.5 else "large"
                except (ValueError, TypeError):
                    pass

                # Infer from highway tag
                hw = str(r.get("highway", "")).lower()

                if hw in {"trunk", "trunk_link", "primary", "primary_link"}:
                    return "large"
                if hw in {"secondary", "secondary_link", "tertiary", "tertiary_link", "services"}:
                    return "medium"
                if hw in {"residential", "service", "track",
                        "turning_circle", "turning_loop", "passing_place", "rest_area"}:
                    return "small"

                # Non‑drivable → forbidden
                return "forbidden"

            roads["classify_truck"] = roads.apply(classify, axis=1)

            joined = gpd.sjoin(gdf, roads[["geometry","classify_truck"]],
                               predicate="intersects", how="left")
            filtered = (joined[joined["classify_truck"].isin(self.TRUCK_CLASSES)]
                        .drop_duplicates("stop_id"))

            out_stops = city_dir / "stops_truck_only.csv"
            out_stops.write_bytes(filtered.to_csv(index=False).encode())


            # Simulate bookings
            bookings = generate_bookings(city_dir, tag)
            all_booking_frames.append(bookings)

            print(f"{code.upper()}: {len(filtered)} stops, "
                  f"{len(bookings)} bookings")

        # Concat all cities → one DataFrame for ETL load()
        if all_booking_frames:
            return pd.concat(all_booking_frames, ignore_index=True)
        raise RuntimeError("No cities processed")
