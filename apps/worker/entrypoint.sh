#!/usr/bin/env sh
set -e

# If COUNTRY isn't set, run *all* known pipelines
if [ -z "$COUNTRY" ]; then
  echo "COUNTRY env not provided – running all pipelines"
  python scripts/run_etl.py mx
  python scripts/run_etl.py co
else
  echo "Running pipeline for $COUNTRY"
  python scripts/run_etl.py "$COUNTRY"
fi

echo "ETL finished"

