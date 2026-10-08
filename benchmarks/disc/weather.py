#!/usr/bin/env python3
"""Weather analysis: maximum snowfall in millimeters per month.

The normalize UDF treats any unit other than mm as feet and multiplies by 304.8, so an
inch reading becomes huge. Prism traces the anomalously large output and explains it at
field granularity.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "python"))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from prism import Prism, udf
import gen

DATA = os.environ.get("DISC_DATA", "tpcds/data/disc")


@udf(DoubleType())
def to_mm(value, unit):
    # Bug: any unit other than "mm" is treated as feet.
    return float(value if unit == "mm" else value * 304.8)


def build(spark, p=None, mode="column"):
    path = os.path.join(DATA, "weather.parquet")
    if not gen.exists(spark, path):
        gen.gen_one(spark, "weather")
    readings = spark.read.parquet(path)
    if p is not None:
        p.enable_udf_aware(to_mm, mode=mode)
        p.enable_influence("max")

    result = (readings.withColumn("mm", to_mm("value", "unit"))
              .groupBy("month").agg(F.max("mm").alias("max_mm")))

    def select(rows):
        return [max((ri for ri in rows if ri[0]["max_mm"] is not None),
                    key=lambda ri: ri[0]["max_mm"])[1]]
    return readings, result, select, "max_mm"


def main():
    spark = (SparkSession.builder.appName("weather").master("local[4]")
             .config("spark.sql.extensions", "org.apache.spark.sql.lineage.PrismSQLExtension")
             .config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    p = Prism(spark)
    p.enable_capture()
    src, result, select, agg = build(spark, p)
    rows = p.collect_with_lineage(result)
    bad = select(rows)
    cur = p.trace(result, bad).go_back()
    w = cur.show()
    print("traced to %d source records" % len(w))
    if w:
        print("  field lineage: data=%s control=%s regime=%s"
              % (w[0].get("_data"), w[0].get("_control"), w[0].get("_regime")))
    print("  ranked by influence:", cur.influence(agg).ranked()[:3])
    spark.stop()


if __name__ == "__main__":
    main()