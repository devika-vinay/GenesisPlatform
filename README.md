# GÉNESIS Platform

Community‑based freight & moving logistics platform (pilot: Mexico, Costa Rica, Colombia).

## Quick‑start

1. Clone repository
2. Download and spin up docker
3. Stop & remove any previous containers/images docker compose down --remove-orphans --volumes
4. Build the genesis image from scratch docker compose build --no-cache genesis
5. Run all countries docker compose up genesis
6. Run specific countries COUNTRY=mx docker compose up genesis


```bash
git clone <your-fork-url> genesis-platform
cd genesis-platform

# Install deps (Poetry)
poetry install

# Spin up local services
docker compose up -d

# Run tests
pytest -q
```

More docs are under `docs/`.
