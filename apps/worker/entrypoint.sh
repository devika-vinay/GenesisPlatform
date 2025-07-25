#!/usr/bin/env sh
# --------------------------------------------------------------------
# Acts as start‑button for the Docker container
# Delegates running to main.py

set -e  # abort on first error

python apps/api/app/main.py
