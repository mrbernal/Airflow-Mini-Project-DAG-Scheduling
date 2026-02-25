"""This DAG downloads AAPL and TSLA stock data and makes a simple summary report."""

import io
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pendulum
import yfinance as yf
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "magenta",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


LOCAL_TZ = pendulum.timezone("America/Chicago")
START_DATE_ENV = os.getenv("MARKETVOL_START_DATE")
if START_DATE_ENV:
    START_DATE = pendulum.parse(START_DATE_ENV, tz=LOCAL_TZ)
else:
    START_DATE = pendulum.now(LOCAL_TZ).subtract(days=7).replace(hour=18, minute=0, second=0, microsecond=0)


def download_market_data(symbol: str, execution_date: str, output_dir: str) -> None:
    """Download one day of 1-minute market data for a stock symbol."""

    start_dt = datetime.strptime(execution_date, "%Y-%m-%d").date()
    if start_dt.weekday() >= 5:
        raise ValueError(
            f"{execution_date} is a weekend (Saturday/Sunday). "
            "Use a weekday logical date (Mon-Fri), e.g. ./run_marketvol.sh 2026-02-20."
        )
    end_dt = start_dt + timedelta(days=1)

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    data = yf.download(
        symbol,
        start=start_dt,
        end=end_dt,
        interval="1m",
        progress=False,
    )

    if data.empty:
        raise ValueError(
            f"No downloaded market data for {symbol} on {execution_date}. "
            "This is commonly a market holiday or unsupported intraday date range."
        )

    file_path = output_path / f"{symbol}.csv"
    data.to_csv(file_path)


def run_market_query(execution_date: str, target_dir: str) -> None:
    """Read the downloaded CSV files and create a summary CSV."""

    base_path = target_dir.rstrip("/")
    symbols = ["AAPL", "TSLA"]

    rows = []
    for symbol in symbols:
        hdfs_csv_path = f"{base_path}/{symbol}.csv"
        csv_content = subprocess.check_output(
            ["hdfs", "dfs", "-cat", hdfs_csv_path],
            text=True,
        )
        df = pd.read_csv(io.StringIO(csv_content))

        close_series = pd.to_numeric(df.get("Close"), errors="coerce")
        high_series = pd.to_numeric(df.get("High"), errors="coerce")
        volume_series = pd.to_numeric(df.get("Volume"), errors="coerce")

        rows.append(
            {
                "symbol": symbol,
                "rows": len(df),
                "avg_close": round(close_series.mean(skipna=True), 4),
                "max_high": round(high_series.max(skipna=True), 4),
                "total_volume": float(volume_series.fillna(0).sum()),
            }
        )

    summary = pd.DataFrame(rows)
    local_summary_path = Path(f"/tmp/summary_{execution_date}.csv")
    summary.to_csv(local_summary_path, index=False)
    subprocess.run(
        ["hdfs", "dfs", "-put", "-f", str(local_summary_path), f"{base_path}/summary.csv"],
        check=True,
    )
    print(summary.to_string(index=False))


with DAG(
    dag_id="marketvol",
    default_args=default_args,
    description="Download and analyze weekday intraday market data",
    start_date=START_DATE,
    schedule="0 18 * * 1-5",
    catchup=False,
    tags=["market", "yfinance", "magenta"],
) as dag:
    t0 = BashOperator(
        task_id="t0",
        bash_command="mkdir -p /tmp/data/{{ ds }} && hdfs dfs -mkdir -p /tmp/data/marketvol/{{ ds }}",
    )

    t1 = PythonOperator(
        task_id="t1",
        python_callable=download_market_data,
        op_kwargs={
            "symbol": "AAPL",
            "execution_date": "{{ ds }}",
            "output_dir": "/tmp/data/{{ ds }}",
        },
    )

    t2 = PythonOperator(
        task_id="t2",
        python_callable=download_market_data,
        op_kwargs={
            "symbol": "TSLA",
            "execution_date": "{{ ds }}",
            "output_dir": "/tmp/data/{{ ds }}",
        },
    )

    t3 = BashOperator(
        task_id="t3",
        bash_command="hdfs dfs -put -f /tmp/data/{{ ds }}/AAPL.csv /tmp/data/marketvol/{{ ds }}/AAPL.csv",
    )

    t4 = BashOperator(
        task_id="t4",
        bash_command="hdfs dfs -put -f /tmp/data/{{ ds }}/TSLA.csv /tmp/data/marketvol/{{ ds }}/TSLA.csv",
    )

    t5 = PythonOperator(
        task_id="t5",
        python_callable=run_market_query,
        op_kwargs={
            "execution_date": "{{ ds }}",
            "target_dir": "/tmp/data/marketvol/{{ ds }}",
        },
    )

    t0 >> [t1, t2]
    t1 >> t3
    t2 >> t4
    [t3, t4] >> t5
