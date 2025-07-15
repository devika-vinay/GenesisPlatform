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
1. Create a feature branch
    - git checkout -b feature/your-topic

2. Stage changes
    - git add path/to/file.py

3. Commit
    - git commit -m "Meaningful message"

4. Push changes and raise PR
    - git push origin <branch name>

## 4. Note
1. Please add any new package and exact version to requirements.txt