"""Store latency: the 95th-percentile latency per store, a holistic aggregate.

A percentile has no constant-size summary, so Spark runs it through its object-hash
aggregate. One store has readings of 1000 that make up exactly its top 5%, which lifts
its 95th percentile. A reading's influence is the change in the percentile when that
reading is removed.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "python"))

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from prism import udf
import gen

DATA = os.environ.get("DISC_DATA", "tpcds/data/disc")


@udf(DoubleType())
def latency_ms(raw):
    return float(raw)


def build(spark, p=None, mode="column"):
    path = os.path.join(DATA, "latency.parquet")
    if not gen.exists(spark, path):
        gen.gen_one(spark, "latency")
    readings = spark.read.parquet(path)
    if p is not None:
        p.enable_udf_aware(latency_ms, mode=mode)
        p.enable_influence()
    result = (readings.withColumn("ms", latency_ms("raw"))
              .groupBy("store")
              .agg(F.percentile("ms", F.lit(0.95)).alias("p95")))

    def select(rows):
        return [max((ri for ri in rows if ri[0]["p95"] is not None),
                    key=lambda ri: ri[0]["p95"])[1]]
    return readings, result, select, "p95"
