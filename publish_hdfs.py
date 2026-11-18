"""Commit and reconcile a versioned snapshot in isolated local HDFS."""
from argparse import ArgumentParser
from datetime import date
import hashlib
import json
from pathlib import Path
from uuid import uuid4

from pyspark.sql import SparkSession

from lakehouse import daily_pickups

ARTIFACTS = ("facts-flat", "facts-by-day", "quarantine", "metrics.json")


def publish(spark, source, base):
    metrics = json.loads((source / "metrics.json").read_text(encoding="utf-8"))
    identity = metrics["identity"]
    encoded = json.dumps(identity, sort_keys=True).encode()
    version = hashlib.sha256(encoded).hexdigest()
    location = f"{base.rstrip('/')}/{version}"
    day = date.fromisoformat(identity["benchmark_day"])
    sc = spark.sparkContext
    conf = sc._jsc.hadoopConfiguration()
    conf.setBoolean("dfs.client.use.datanode.hostname", True)
    hadoop = sc._jvm.org.apache.hadoop
    path = hadoop.fs.Path
    target = path(location)
    fs = target.getFileSystem(conf)
    committed = False
    if not fs.exists(target):
        stage = path(f"{base.rstrip('/')}/.staging-{uuid4().hex}")
        fs.mkdirs(stage)
        try:
            for name in ARTIFACTS:
                fs.copyFromLocalFile(False, False, path((source / name).resolve().as_uri()), path(stage, name))
            context = hadoop.fs.FileContext.getFileContext(target.toUri(), conf)
            options = sc._gateway.new_array(hadoop.fs.Options.Rename, 1)
            options[0] = hadoop.fs.Options.Rename.NONE
            try:
                context.rename(stage, target, options)
                committed = True
            except Exception:
                if not fs.exists(target):
                    raise
        finally:
            if fs.exists(stage):
                fs.delete(stage, True)

    stored_metrics = json.loads(spark.read.text(f"{location}/metrics.json", wholetext=True).first()[0])
    if stored_metrics["identity"] != identity:
        raise ValueError("HDFS snapshot identity differs from local input")
    for name in ARTIFACTS[:3]:
        if not fs.exists(path(target, name + "/_SUCCESS")):
            raise ValueError(f"HDFS snapshot missing Spark commit marker for {name}")
    expected = daily_pickups(spark, source / "facts-by-day", day).collect()
    for name in ARTIFACTS[:2]:
        if daily_pickups(spark, f"{location}/{name}", day).collect() != expected:
            raise ValueError(f"HDFS daily pickup mart differs for {name}")
    total_eligible = sum(item["eligible_rows"] for item in metrics["sources"])
    total_quarantined = sum(sum(item["quarantine"].values()) for item in metrics["sources"])
    eligible = spark.read.parquet(f"{location}/facts-by-day").count()
    quarantined = spark.read.parquet(f"{location}/quarantine").count()
    if (eligible, quarantined) != (total_eligible, total_quarantined):
        raise ValueError("HDFS row totals differ from local manifest")
    return {
        "identity": identity, "hdfs_snapshot": location, "created": committed,
        "eligible_rows": eligible, "quarantined_rows": quarantined,
        "daily_mart_rows": sum(row["trips"] for row in expected), "daily_mart_zones": len(expected),
        "daily_mart_equal_to_local_for_both_layouts": True,
    }


if __name__ == "__main__":
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--base", default="hdfs://namenode:8020/nyc-tlc")
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()
    spark = (SparkSession.builder.appName("NYC TLC HDFS verification")
             .config("spark.sql.shuffle.partitions", "8").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        receipt = json.dumps(publish(spark, args.source, args.base), indent=2) + "\n"
        args.receipt.write_text(receipt, encoding="utf-8")
        print(receipt)
    finally:
        spark.stop()
