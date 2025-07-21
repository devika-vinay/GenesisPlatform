# GÉNESIS Data Pipeline – Architecture & Contributor Guide

## 1.Folder Layout & Responsibilities

```text
genesis-platform/
├── data/
│ ├── raw/ # Not committed to repo
│ │ ├── mx/ # Source ZIP / PBF files live here
│ │ │ ├── gtfs.zip
│ │ │ └── semovi-oaxaca-mx.zip
│ │ └── co/
│ │ ├── Bogota1.zip
│ │ ├── Bogota2.zip
│ │ └── colombia-latest.osm.pbf
│ └── processed/ # Final outputs per country (CSV)
├── tmp/ # Disposable scratch created at runtime
│ ├── mx/
│ └── co/
├── pipelines/ # Country‑specific ETL logic
│ ├── etl_base.py
│ ├── mx_preprocess.py
│ ├── co_preprocess.py
│ └── process_helper.py
├── apps/worker/entrypoint.sh # Docker entry‑point; decides which pipeline(s) run
├── scripts/run_etl.py # CLI wrapper used by entrypoint
├── requirements.txt # All Python dependencies (Docker builds from this)
└── docker-compose.yml # Build/run “genesis” service


## 2.High‑Level Flow

```text
Dockerfile      :   Set of instructions used to build a Docker image including defining start point
     |
entrypoint.sh   :   Sets COUNTRY env → decides which pipelines run
     │ 
run_etl.py      :   Used as registration for any new countries
     |
etl_base.py     :   Common base steps (extract → transform → load)
     │
process_helper.py : Defines logic for common functionality like extraction and output
     |
<c>_preprocess.py : Defines country specific filtering logic
     │
data/processed/<c>.csv: Output artifacts


