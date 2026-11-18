"""Run acquisition, Spark normalization and HDFS reconciliation in order."""
from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import sys

from airflow.sdk import dag, task

WORKSPACE = Path(__file__).resolve().parents[1]
SPARK = "/opt/spark/bin/spark-submit"
OPTIONS = ["--master", "local[2]", "--driver-memory", "1g", "--conf", "spark.ui.enabled=false"]


@dag(schedule=None, start_date=datetime(2025, 1, 1, tzinfo=timezone.utc), catchup=False, max_active_runs=1)
def nyc_tlc_backfill():
    @task
    def acquire():
        subprocess.run([sys.executable, "fetch_tlc.py"], cwd=WORKSPACE, check=True)

    @task
    def lakehouse():
        snapshot = os.environ.get("TLC_OUTPUT", "runs/airflow")
        subprocess.run([SPARK, *OPTIONS, "lakehouse.py", "--output", snapshot], cwd=WORKSPACE, check=True)
        return snapshot

    @task
    def reconcile_hdfs(snapshot):
        subprocess.run(
            [SPARK, *OPTIONS, "publish_hdfs.py", "--source", snapshot, "--receipt", f"{snapshot}-hdfs.json"],
            cwd=WORKSPACE, check=True,
        )

    built = lakehouse()
    acquire() >> built
    reconcile_hdfs(built)


backfill = nyc_tlc_backfill()
