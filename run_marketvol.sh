#!/usr/bin/env bash
set -euo pipefail

DAG_ID="marketvol"
LOGICAL_DATE="${1:-$(date +%F)}"
RUN_ID="manual__${LOGICAL_DATE}_$(date +%s)"
MAX_WAIT_SECONDS=180
SLEEP_SECONDS=5

if ! date -d "$LOGICAL_DATE" +%F >/dev/null 2>&1; then
  echo "Invalid date '$LOGICAL_DATE'. Use YYYY-MM-DD, e.g. ./run_marketvol.sh 2026-02-20"
  exit 1
fi

weekday_num=$(date -d "$LOGICAL_DATE" +%u)
if [ "$weekday_num" -ge 6 ]; then
  echo "'$LOGICAL_DATE' is a weekend. Use a weekday date (Mon-Fri), e.g. ./run_marketvol.sh 2026-02-20"
  exit 1
fi

echo "Starting Airflow services..."
docker compose up -d

if ! docker compose exec -T airflow-scheduler airflow db check >/dev/null 2>&1; then
  echo "Airflow metadata DB not ready/initialized. Running one-time init..."
  docker compose --profile init up airflow-init
else
  echo "Airflow metadata DB already initialized. Skipping airflow-init."
fi

echo "Waiting for DAG registration in scheduler..."
elapsed=0
until docker compose exec -T airflow-scheduler airflow dags list | awk '{print $1}' | grep -Fxq "$DAG_ID"; do
  if [ "$elapsed" -ge "$MAX_WAIT_SECONDS" ]; then
    echo "DAG '$DAG_ID' not found after ${MAX_WAIT_SECONDS}s. Showing import errors:"
    docker compose exec -T airflow-scheduler airflow dags list-import-errors || true
    exit 1
  fi
  sleep "$SLEEP_SECONDS"
  elapsed=$((elapsed + SLEEP_SECONDS))
done

echo "DAG '$DAG_ID' is registered."

echo "Unpausing DAG if needed..."
docker compose exec -T airflow-scheduler airflow dags unpause "$DAG_ID" || true

echo "Triggering DAG run..."
docker compose exec -T airflow-scheduler airflow dags trigger "$DAG_ID" \
  --run-id "$RUN_ID" \
  --exec-date "${LOGICAL_DATE}T18:00:00"

echo "Recent DAG runs:"
docker compose exec -T airflow-scheduler airflow dags list-runs -d "$DAG_ID" --no-backfill | head -n 20

echo "Triggered DAG=$DAG_ID RUN_ID=$RUN_ID LOGICAL_DATE=$LOGICAL_DATE"
