# pipelines/co_preprocess.py
from pathlib import Path
import subprocess
import pandas as pd, geopandas as gpd
from shapely.geometry import Point
from pyrosm import OSM
from .etl_base import ETLPipeline
from .process_helper import extract_gtfs_feeds, generate_bookings

class ColombiaPipeline(ETLPipeline):
    feeds = {"bogota1": "Bogota1.zip", "bogota2": "Bogota2.zip"}

    RAW_PBF   = Path("data/raw/co/colombia-latest.osm.pbf")
    CLIP_PBF  = Path("data/raw/co/bogota_clip.osm.pbf")
    CACHE_PAR = Path("data/raw/co/bogota_roads.parquet")

    BBOX  = (-74.25, 4.47, -73.99, 4.84)            # W S E N
    DROP  = {"stop_code","zone_id","stop_url","parent_station",
             "stop_desc","stop_timezone","level_id","platform_code"}

    TRUCK_CLASSES = {"small","medium","large"}
    HIGHWAYS      = ["motorway","trunk","primary","secondary","tertiary",
                     "unclassified","residential","service"]

    def __init__(self, cc: str = "co"):
        super().__init__(cc)
        self.tmp = Path("tmp") / cc
        self.tmp.mkdir(parents=True, exist_ok=True)

    def extract(self) -> None:
        extract_gtfs_feeds(self.feeds, Path("data/raw/co"), self.tmp)

        if not self.CLIP_PBF.exists():
            subprocess.run(
                ["osmium","extract","--bbox", ",".join(map(str, self.BBOX)),
                 str(self.RAW_PBF), "-o", str(self.CLIP_PBF)],
                check=True
            )

    def transform(self, _) -> pd.DataFrame:
        # 1) GTFS stops -------------------------------------------------
        stops_files = list(self.tmp.rglob("**/stops.txt")) + \
                      list(self.tmp.rglob("**/stops-*.txt"))
        if not stops_files:
            raise FileNotFoundError("No stops.txt found in extracted GTFS")

        df = pd.concat(map(pd.read_csv, stops_files), ignore_index=True)
        df = df.drop(columns=[c for c in self.DROP if c in df.columns])

        gdf = gpd.GeoDataFrame(
            df,
            geometry=[Point(xy) for xy in zip(df.stop_lon, df.stop_lat)],
            crs="EPSG:4326"
        ).to_crs(3857)
        gdf["geometry"] = gdf.buffer(70).to_crs(4326)

        # 2) Bogotá roads (cached) -------------------------------------
        if self.CACHE_PAR.exists():
            roads = gpd.read_parquet(self.CACHE_PAR)
        else:
            roads = OSM(str(self.CLIP_PBF)).get_data_by_custom_criteria(
                custom_filter={"highway": True},
                filter_type="keep",
                keep_nodes=False,
                extra_attributes=["access","hgv","maxweight"]
            )
            roads.to_parquet(self.CACHE_PAR)

        roads = roads.to_crs("EPSG:4326")
        roads = roads[roads["highway"].isin(self.HIGHWAYS)]

        # classify truck access
        def classify(r):
            acc = str(r.get("access","")).lower()
            hgv = str(r.get("hgv","")).lower()
            if acc in {"no","private","agricultural"} or hgv == "no":
                return "forbidden"
            try:
                w = float(str(r.get("maxweight","")).lower().replace("t",""))
                return "small" if w<=3.5 else "medium" if w<=7.5 else "large"
            except:
                return "unknown"
        roads["classify_truck"] = roads.apply(classify, axis=1)

        # 3) spatial join ---------------------------------------------
        joined = gpd.sjoin(gdf, roads[["geometry","classify_truck"]],
                           predicate="intersects", how="left")
        filtered = (joined[joined["classify_truck"].isin(self.TRUCK_CLASSES)]
                    .drop_duplicates("stop_id"))

        (self.tmp / "stops_truck_only.csv").write_bytes(
            filtered.to_csv(index=False).encode()
        )

        # 4) simulate bookings ----------------------------------------
        bookings = generate_bookings(self.tmp, "bogota")
        print(f"Bogotá pipeline complete — {len(filtered)} stops, "
              f"{len(bookings)} bookings")
        return bookings
