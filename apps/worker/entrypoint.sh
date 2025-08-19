#!/usr/bin/env sh
# Runs the batch first, then starts the API (default).
# Prevents re-running batch on restart by using a sentinel file.
# Uses APP_MODULE to tell uvicorn where the FastAPI app lives.

set -eu

: "${APP_MODE:=both}"                      # both | batch | api
: "${APP_MODULE:=apps.api.app.routes.router:app}"    # points to the file you just created
: "${PORT:=8000}"
: "${BATCH_ONCE_FLAG:=/app/.batch_done}"   # prevents batch re-running after success

run_batch() {
  echo "[entrypoint] Running batch pipeline..."
  # main.py should read COUNTRY from env and run single/all accordingly
  python apps/api/app/main.py
  echo "[entrypoint] Batch finished."
}

start_api() {
  echo "[entrypoint] Starting API on 0.0.0.0:${PORT} using ${APP_MODULE} ..."
  exec uvicorn "${APP_MODULE}" --host 0.0.0.0 --port "${PORT}"
}

case "${APP_MODE}" in
  batch)
    run_batch
    ;;
  api)
    start_api
    ;;
  both)
    if [ "${FORCE_BATCH:-0}" = "1" ] || [ ! -f "${BATCH_ONCE_FLAG}" ]; then
      run_batch
      : > "${BATCH_ONCE_FLAG}"   # write sentinel *after* successful batch
    else
      echo "[entrypoint] Skipping batch (found ${BATCH_ONCE_FLAG})."
    fi
    start_api
    ;;
  *)
    echo "[entrypoint] Unknown APP_MODE='${APP_MODE}' (expected both|batch|api)" >&2
    exit 1
    ;;
esac
