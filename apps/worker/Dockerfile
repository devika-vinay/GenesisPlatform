# ---------------------------------------------------------------------
# Dockerfile – GÉNESIS data/AI pilot
# Goal: Create a reproducible image that already contains every system 
# and Python dependency needed


# ---------- Base image ------------------------------------------------
FROM python:3.11-slim

# ---------- Create & switch to working directory ----------------------
# Everything that follows (COPY, RUN, etc.) happens relative to /app.
WORKDIR /app

# ---------- Copy entire project into image ---------------------------
# Copies code, requirements.txt, data‑free folders, etc.
COPY . /app

# ---------- Install Debian packages ----------------------------------
# We need native libraries
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential gdal-bin libgdal-dev libgeos-dev libproj-dev \
        curl ca-certificates osmium-tool && \
    # keep the image lean: remove apt cache once packages are installed
    apt-get clean && rm -rf /var/lib/apt/lists/*

# ---------- Install Python dependencies ------------------------------
# 1) Upgrade pip to latest 
# 2) Install exact versions from requirements.txt
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# ---------- Runtime environment --------------------------------------
# Add project root to Python module search path so `import pipelines`
# works no matter where scripts are executed inside the container.
ENV PYTHONPATH=/app
ENV PORT=8000
EXPOSE 8000

# ---------- Copy wrapper entry‑point ---------------------------------
# This tiny shell script decides which file to run
ENV PYTHONUNBUFFERED=1
COPY apps/worker/entrypoint.sh /usr/local/bin/etl-entrypoint
RUN chmod +x /usr/local/bin/etl-entrypoint

# ---------- Default command ------------------------------------------
# When someone runs `docker run … genesis`, Docker executes the script.
ENTRYPOINT ["etl-entrypoint"]
