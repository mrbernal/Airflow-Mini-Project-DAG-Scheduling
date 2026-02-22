# Airflow_DAG.py README

This project implements an Airflow DAG named `marketvol` in `Airflow_DAG.py`.

## Main DAG information

- DAG name: `marketvol`
- Schedule: 6 PM on weekdays (Mon-Fri)
- Retries: 2 retries with 5-minute delay
- Task flow: `t0 -> [t1, t2]`, `t1 -> t3`, `t2 -> t4`, `[t3, t4] -> t5`
- Uses `BashOperator` and `PythonOperator`
- Uses `CeleryExecutor` from docker-compose setup
- Loads symbol CSV files into HDFS and runs query from HDFS

## DAG Behavior

1. `t0`: Creates runtime directories for this execution date.
2. `t1`: Downloads 1-minute `AAPL` data to CSV.
3. `t2`: Downloads 1-minute `TSLA` data to CSV.
4. `t3`: Uploads `AAPL.csv` to HDFS target directory.
5. `t4`: Uploads `TSLA.csv` to HDFS target directory.
6. `t5`: Reads both HDFS CSV files and writes `summary.csv` back to HDFS.

## Associated Files in this project

- `Airflow_DAG.py`
- `run_marketvol.sh`
- `README.md`
- `Airflow_DAG_successful_command_line_execution.txt`

## Prerequisites

- Docker Desktop running
- WSL integration enabled
- Work from this directory
- `docker compose` available
- `hdfs` CLI available inside Airflow runtime containers
- HDFS namenode reachable from Airflow runtime

## Run Steps

1. Make script executable (one-time setup, or after permission resets):

```bash
chmod +x run_marketvol.sh
```

2. One-time init only (first setup, or if DB is reset):

```bash
docker compose --profile init up airflow-init
```

3. Build image once (or after Dockerfile changes):

```bash
docker compose build
```

4. Trigger the DAG manually:

```bash
./run_marketvol.sh
```

Optional date:

```bash
./run_marketvol.sh 2026-02-20
```

Use a weekday date (Mon-Fri). Weekend or market-holiday dates can fail because 1-minute market data may be unavailable.

## Verify Result

- Confirm latest DAG run state:

```bash
docker compose exec -T airflow-scheduler airflow dags list-runs -d marketvol --no-backfill | head -n 20
```

#Confirm output files exist in HDFS (today’s date):

```bash
docker compose exec -T airflow-worker hdfs dfs -ls /tmp/data/marketvol/$(date +%F)
```

#Open the Airflow UI:

- Airflow UI: `http://localhost:8080`
- Login: `admin / admin`
- Check DAG `marketvol` run status is `success`
- Check task instances `t0` to `t5` are successful

## Notes

- Download staging is local (`/tmp/data/{{ ds }}`), then files are uploaded to HDFS.
- Query in `t5` reads from HDFS and writes `summary.csv` to HDFS.