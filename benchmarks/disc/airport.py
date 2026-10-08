#!/usr/bin/env python3
"""Aircraft transit: total flight duration per departure hour.

The duration UDF subtracts departure from arrival minutes without adding 1440 for a
flight that crosses midnight, so that flight gets a negative duration. Prism traces the
negative output and explains it at field granularity with influence.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))          # gen
sys.path.insert(0, os.path.join(                                        # prism
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "python"))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from prism import Prism, udf
import gen

DATA = os.environ.get("DISC_DATA", "tpcds/data/disc")


@udf(DoubleType())
def duration(arr_min, dep_min):
    # Bug: a flight crossing midnight needs 1440 minutes added, which this omits.
    return float(arr_min - dep_min)


def build(spark, p=None, mode="column"):
    path = os.path.join(DATA, "airport.parquet")
    if not gen.exists(spark, path):
        gen.gen_one(spark, "airport")
    flights = spark.read.parquet(path)
    if p is not None:
        p.enable_udf_aware(duration, mode=mode)
        p.enable_influence("sum")
    result = (flights.withColumn("dur", duration("arr_min", "dep_min"))
              .groupBy("dep_hr").agg(F.sum("dur").alias("total")))

    def select(rows):
        return [i for r, i in rows if r["total"] is not None and r["total"] < 0]
    return flights, result, select, "total"


def main():
    spark = (SparkSession.builder.appName("aircraft").master("local[4]")
             .config("spark.sql.extensions", "org.apache.spark.sql.lineage.PrismSQLExtension")
             .config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    p = Prism(spark)
    p.enable_capture()
    flights, result, select, agg = build(spark, p)
    rows = p.collect_with_lineage(result)
    bad = select(rows)
    print("suspicious outputs (negative total):", len(bad))

    cur = p.trace(result, bad).go_back()
    witnesses = cur.show()
    print("traced to %d source records via go_back()" % len(witnesses))
    if witnesses:
        w = witnesses[0]
        print("  field lineage of the measure: data=%s control=%s regime=%s"
              % (w.get("_data"), w.get("_control"), w.get("_regime")))
    top = cur.influence().ranked()[:3]
    print("  witnesses ranked by influence (most responsible first):", top)
    spark.stop()


if __name__ == "__main__":
    main()
