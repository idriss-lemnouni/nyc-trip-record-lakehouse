"""Check the vendor schema boundary and first-match quarantine rules in Spark."""
from decimal import Decimal
from pathlib import Path
import sys

from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lakehouse import normalize

FIELDS = [
    "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID",
    "trip_distance", "total_amount",
]

spark = SparkSession.builder.appName("NYC TLC schema fixture").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
try:
    old = spark.createDataFrame([
        ("2024-12-31 23:59:00", "2025-01-01 00:14:00", 12, 24, 1.5, 19.25),
        ("2025-01-01 00:00:00", "2025-01-01 00:14:00", 12, 24, 1.5, 19.25),
        ("2024-12-31 23:59:00", "2024-12-31 23:58:00", 12, 24, 1.5, 19.25),
        ("2024-12-31 23:59:00", "2025-01-01 00:14:00", 266, 24, 1.5, 19.25),
        ("2024-12-31 23:59:00", "2025-01-01 00:14:00", 12, 24, float("inf"), 19.25),
        ("2024-12-31 23:59:00", "2025-01-01 00:14:00", 12, 24, 1.5, -19.25),
        ("not-a-time", "2025-01-01 00:14:00", 12, 24, 1.5, 19.25),
    ], FIELDS)
    new = spark.createDataFrame([
        ("2025-01-01 00:00:00", "2025-01-01 00:14:00", 12, 24, 1.5, 20.0, 0.75),
    ], FIELDS + ["cbd_congestion_fee"])
    records = normalize(old, "2024-12").unionByName(normalize(new, "2025-01")).collect()
    assert sorted(r.outcome for r in records) == [
        "accepted", "accepted", "duration_outside_0_to_6_hours", "invalid_distance",
        "invalid_timestamp", "invalid_zone", "missing_or_negative_total", "pickup_outside_source_month",
    ]
    accepted = {r.source_month: r for r in records if r.outcome == "accepted"}
    assert accepted["2024-12"].cbd_fee_usd is None
    assert accepted["2025-01"].cbd_fee_usd == Decimal("0.75")
    assert accepted["2024-12"].service_day.isoformat() == "2024-12-31"
    print("Spark schema, quarantine and month boundary checks passed")
finally:
    spark.stop()
