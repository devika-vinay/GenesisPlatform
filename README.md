# GÉNESIS Platform

Community‑based freight & moving logistics platform (pilot: Mexico, Costa Rica, Colombia).

## 1. Set‑up

### Prerequisites (once per machine)
1. Git
2. Docker Desktop
3. Raw data files:
    - mx: gtfs.zip | semovi-oaxaca-mx.zip
    - co: Bogota1.zip | Bogota2.zip | colombia-latest.osm.pbf

## 2. Run pipeline
1. Start Docker Desktop 

2. Clone repository
    - https://github.com/devika-vinay/GenesisPlatform.git

3. Stop & remove any previous containers/images 
    - docker compose down --remove-orphans --volumes

4. Build the genesis image from scratch 
    - docker compose build --no-cache genesis

5. Build genesis image with previous cache
    - docker compose build genesis

6. Run all countries 
    - docker compose up genesis

7. Run specific countries 
    - COUNTRY=mx docker compose up genesis

## 3. Git Workflow
1. Get latest changes from github
    - git pull origin main

2. Create a feature branch
    - git checkout -b feature/your-topic

3. Stage changes
    - git add path/to/file.py

4. Commit
    - git commit -m "Meaningful message"

5. Push changes
    - git push origin <branch name>

6. Open a Pull Request on GitHub → team reviews → Merge.
    - main is protected: nobody can push directly without a PR + at least one approval.

## 4. Note
1. Please add any new package and exact version to requirements.txt

## 5. Approach to pipeline structure and build

### What this project is
```
“Take raw public‑transport data + road maps, filter only the places a moving
truck can actually go, and simulate bookings we can later match to real
requests.”

Our scripts do five things end‑to‑end:
1. Extract raw data from GTFS ZIP files (bus & metro schedules) and OpenStreetMap road network (osm.pbf)
2. Transform them by keeping stops on roads wide/strong enough for trucks.
3. Classify those roads into small / medium / large truck size.
4. Load the results into tidy CSVs for analysis.
5. Simulate 500 synthetic bookings per city so we can test the platform
before we have real customers.
```

### Folder layout 
```
We've kept the folder structure as modular as possible because:
1. Separating raw, processed, and code means you can delete tmp/ anytime without losing data
2. Large raw files live outside Git (so the repo stays <100 MB)
3. Each pipeline is only responsible for its own country.
```

### Why requirements.txt
```
Python projects need extra libraries (GeoPandas, PyROSM, …). Requirements.txt acts like a grocery list. It makes sure we install the exact versions, all laptops or systems run the same code

Further reading: https://pip.pypa.io/en/stable/user_guide/#requirements-files
```

### Why Docker
```
A Docker image is like a zip snapshot of an entire computer:
OS + Python + our libraries + our code.

A container is a running instance of that image (like a VM, but lighter).

We provide the docker commands on our CLI and it builds the image exactly once, mounts data/raw/ into the container, runs entrypoint.sh, which in turn calls our main.py file containing business logic.

Because the image already contains GDAL, GeoPandas, etc., nobody else has to install or compile them; the pipeline will run on Windows, macOS, Linux, CI, or a cloud VM the same way.

Helpful links:
https://www.docker.com/resources/what-container/
Docker Desktop download – https://docs.docker.com/desktop/
```

### Why environments & Docker secrets?
```
Usernames, API keys, etc. should never live in code. They can be passed at run‑time.

More:
https://docs.docker.com/engine/swarm/secrets/
```

### Maintenance
```
1. Drop NewCity.zip or newcountry-latest.osm.pbf in data/raw/<country code>/
2. Copy pipelines/mx_preprocess.py → new_preprocess.py, adapt logic.
3. Register in scripts/run_etl.py following existing format
```

### Further reading 
```
Docker “Get Started” – https://docs.docker.com/get-started/
```




