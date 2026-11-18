"""Build a checksummed, quality-audited NYC yellow taxi Parquet snapshot."""
import argparse
from datetime import date
import hashlib
import json
from pathlib import Path
from statistics import median
from tempfile import TemporaryDirectory
from time import perf_counter

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as f

from fetch_tlc import verify_source

REQUIRED_COLUMNS = {
    "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID",
    "trip_distance", "total_amount",
}


def normalize(raw, month):
    if missing := REQUIRED_COLUMNS.difference(raw.columns):
        raise ValueError(f"Missing required TLC fields: {sorted(missing)}")
    fee = f.col("cbd_congestion_fee") if "cbd_congestion_fee" in raw.columns else f.lit(None)
    trips = raw.select(
        f.col("tpep_pickup_datetime").cast("timestamp_ntz").alias("pickup"),
        f.col("tpep_dropoff_datetime").cast("timestamp_ntz").alias("dropoff"),
        f.col("PULocationID").cast("int").alias("pickup_zone"),
        f.col("DOLocationID").cast("int").alias("dropoff_zone"),
        f.col("trip_distance").cast("double").alias("distance_mi"),
        f.col("total_amount").cast("decimal(13,2)").alias("total_usd"),
        fee.cast("decimal(13,2)").alias("cbd_fee_usd"),
        f.lit(month).alias("source_month"),
    )
    duration = f.expr("timestampdiff(SECOND, pickup, dropoff)")
    reason = (
        f.when(f.col("pickup").isNull() | f.col("dropoff").isNull(), "invalid_timestamp")
        .when(f.date_format("pickup", "yyyy-MM") != month, "pickup_outside_source_month")
        .when((duration <= 0) | (duration > 6 * 3600), "duration_outside_0_to_6_hours")
        .when(f.col("pickup_zone").isNull() | f.col("dropoff_zone").isNull()
              | ~f.col("pickup_zone").between(1, 265) | ~f.col("dropoff_zone").between(1, 265), "invalid_zone")
        .when(f.col("distance_mi").isNull() | f.isnan("distance_mi")
              | (f.col("distance_mi") < 0) | (f.abs("distance_mi") == float("inf")), "invalid_distance")
        .when(f.col("total_usd").isNull() | (f.col("total_usd") < 0), "missing_or_negative_total")
        .otherwise(f.lit("accepted"))
    )
    return trips.withColumn("service_day", f.to_date("pickup")).withColumn("outcome", reason)


def files_in(path):
    files = list(path.rglob("*.parquet"))
    return {"files": len(files), "bytes": sum(file.stat().st_size for file in files)}


def daily_pickups(spark, location, day):
    spark.read.parquet(str(location)).createOrReplaceTempView("eligible_trips")
    sql = (Path(__file__).resolve().parent / "queries/daily_pickups.sql").read_text(encoding="utf-8")
    return spark.sql(sql, args={"service_day": day})


def examine(spark, path, day, stage, label):
    times = []
    result = None
    for _ in range(3):
        query = daily_pickups(spark, path, day)
        start = perf_counter()
        got = query.collect()
        times.append(perf_counter() - start)
        if result is not None and got != result:
            raise ValueError("The same layout returned inconsistent results")
        result = got
    (stage / f"plan-{label}.txt").write_text(query._jdf.queryExecution().executedPlan().toString(), encoding="utf-8")
    candidates = files_in(path / f"service_day={day}") if label == "partitioned" else files_in(path)
    return result, {
        "stored": files_in(path), "candidate_files_for_day": candidates,
        "local_query_seconds": [round(t, 4) for t in times], "local_median_seconds": round(median(times), 4),
    }


def build(spark, manifest, raw, output, day):
    spec = json.loads(manifest.read_text(encoding="utf-8"))
    inputs = spec["snapshots"]
    months = [item["month"] for item in inputs]
    if len(months) != len(set(months)) or day.isoformat()[:7] not in months:
        raise ValueError("Snapshot must list distinct months and cover the benchmark date")
    for item in inputs:
        verify_source(raw / item["name"], item)
    version = hashlib.sha256(manifest.read_bytes()).hexdigest()
    manifest_key = {"source_manifest_sha256": version, "benchmark_day": day.isoformat()}
    if output.exists():
        previous = json.loads((output / "metrics.json").read_text(encoding="utf-8"))
        if (previous["identity"] != manifest_key
                or any(not (output / name / "_SUCCESS").is_file()
                       for name in ("facts-flat", "facts-by-day", "quarantine"))):
            raise ValueError("Output exists for another contract or has incomplete Parquet artifacts")
        print("Matching snapshot already committed")
        return previous
    output.parent.mkdir(parents=True, exist_ok=True)

    details = []
    stream = None
    for item in inputs:
        source = spark.read.parquet(str(raw / item["name"]))
        part = normalize(source, item["month"])
        stream = part if stream is None else stream.unionByName(part)
        details.append({"month": item["month"], "bytes": item["bytes"],
                        "congestion_fee_column_present": "cbd_congestion_fee" in source.columns})
    stream.persist(StorageLevel.DISK_ONLY)
    try:
        counts = {(r["source_month"], r["outcome"]): r["count"]
                  for r in stream.groupBy("source_month", "outcome").count().collect()}
        for item in details:
            by_outcome = {reason: count for (month, reason), count in counts.items() if month == item["month"]}
            item["source_rows"] = sum(by_outcome.values())
            item["eligible_rows"] = by_outcome.pop("accepted", 0)
            item["quarantine"] = by_outcome
        clean = stream.where(f.col("outcome") == "accepted").drop("outcome")
        with TemporaryDirectory(dir=output.parent) as temporary:
            stage = Path(temporary) / "snapshot"
            flat, partitioned = stage / "facts-flat", stage / "facts-by-day"
            clean.coalesce(4).write.parquet(str(flat))
            clean.repartition("service_day").write.partitionBy("service_day").parquet(str(partitioned))
            stream.where(f.col("outcome") != "accepted").coalesce(2).write.parquet(str(stage / "quarantine"))
            actual, flat_metrics = examine(spark, flat, day, stage, "flat")
            partition_result, partition_metrics = examine(spark, partitioned, day, stage, "partitioned")
            if not actual or actual != partition_result:
                raise ValueError("Layout results disagree or the benchmark day is empty")
            proof = {
                "identity": manifest_key, "sources": details,
                "benchmark": {"rows": sum(r["trips"] for r in actual), "zones": len(actual),
                              "flat": flat_metrics, "partitioned": partition_metrics},
            }
            (stage / "metrics.json").write_text(json.dumps(proof, indent=2) + "\n", encoding="utf-8")
            stage.rename(output)
        return proof
    finally:
        stream.unpersist()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=Path("data/manifest.json"))
    parser.add_argument("--raw", type=Path, default=Path("data/raw"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--day", type=date.fromisoformat, default=date(2025, 1, 15))
    args = parser.parse_args()
    spark = (SparkSession.builder.appName("NYC TLC Lakehouse")
             .config("spark.sql.shuffle.partitions", "8").config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.adaptive.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        print(json.dumps(build(spark, args.manifest, args.raw, args.output, args.day), indent=2))
    finally:
        spark.stop()
