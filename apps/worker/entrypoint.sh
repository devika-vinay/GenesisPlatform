#!/usr/bin/env sh
# --------------------------------------------------------------------
# Acts as start‑button for the Docker container

set -e  # abort on first error

if [ -z "$COUNTRY" ]; then
  echo "COUNTRY env not provided – running all pipelines"
  python scripts/run_etl.py mx   # 🇲🇽  Mexico
  python scripts/run_etl.py co   # 🇨🇴  Colombia
  python scripts/run_etl.py cr   # 🇨🇷  Costa Rica

else
  echo "Running pipeline for $COUNTRY"
  python scripts/run_etl.py "$COUNTRY"
fi

echo "ETL finished" 
