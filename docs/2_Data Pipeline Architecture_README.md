# GÉNESIS Data Pipeline – Architecture & Contributor Guide

## 1. Folder Layout & Responsibilities

```text
genesis-platform/
├── apps/
     └── api/
          └── app/
               ├── main.py                 # Principal starting point for logic
               ├── routes/
               │   └── openrouteservice_wrapper.py # API calls
               ├── models/
               │   ├── db/
               │   │   └── booking.py      # SQLAlchemy model
               │   └── schemas/
               │       └── booking.py      # Pydantic request/response
               └── services/
                    └── compute_distance.py # business logic
          └── tests/
               ├── testcase.py             # test cases for api calls
     └── worker
          ├── Dockerfile                   # set of instructions to build application blueprint
          ├── entrypoint.sh                # Docker entry‑point; decides which pipeline(s) run
├── data/
│ ├── raw/ # Untouched external files, not committed to repo
│ │ ├── mx/ # Source ZIP / PBF files live here
│ │ │ ├── gtfs.zip
│ │ │ ├── semovi-oaxaca-mx.zip
      └── mexico-latest.osm.pbf
│ │ └── co/
│ │ ├── Bogota1.zip
│ │ ├── Bogota2.zip
│ │ └── colombia-latest.osm.pbf
│ │ └── cr/
│ │ ├── CR1.zip
│ │ ├── CR2.zip
│ │ └── costa-rica-latest.osm.pbf
│ └── processed/ # Final outputs per country (CSV)
├── tmp/ # Intermediate files created at runtime on Docker container
├── docs/ # Documentation of architecture
├── pipelines/ # Country‑specific ETL logic
│ ├── etl_base.py
│ ├── mx_preprocess.py
│ ├── co_preprocess.py
| ├── cr_preprocess.py   
│ └── process_helper.py
├── scripts/run_etl.py # CLI wrapper used by entrypoint
├── tests/ # end to end integration tests
├── .gitignore # Specifies files which will not be committed to github
├── docker-compose.yml # Build/run “genesis” service
├── .env # Stores secrets locally and are not committed to github to prevent misuse
└── requirements.txt # All Python dependencies (Docker builds from this)
```

## 2. High‑Level Flow

```text
               Dockerfile      :   Set of instructions used to build a Docker image including defining start point
                    |
               entrypoint.sh   :   Calls main.py file to start
                    │ 
---------------main.py         :   Sets COUNTRY env -> decides which pipelines run -> Calls further logic
|                   |
|               run_etl.py      :   Used as registration for any new countries
|                   |
|              etl_base.py     :   Common base steps (extract → transform → load)
|                   │
|              process_helper.py : Defines logic for common functionality like extraction and output
|                   |
|              <c>_preprocess.py : Defines country specific filtering logic
|                   
|
compute_distance.py               : Calls ORS wrapper to get route
|
openrouteservice_wrapper.py       : Returns route distance between point A and B
|        
data/processed/<c>.csv            : Output artifacts      
```





