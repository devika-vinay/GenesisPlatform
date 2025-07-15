FROM python:3.11-slim

WORKDIR /app
COPY . /app

# ––– system libs for GDAL / GEOS / PROJ + Osmium –––
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential gdal-bin libgdal-dev libgeos-dev libproj-dev \
        curl ca-certificates osmium-tool && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# ––– python deps –––
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/app
COPY apps/worker/entrypoint.sh /usr/local/bin/etl-entrypoint
RUN chmod +x /usr/local/bin/etl-entrypoint
ENTRYPOINT ["etl-entrypoint"]
