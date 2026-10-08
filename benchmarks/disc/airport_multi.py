#!/usr/bin/env python3
"""Aircraft transit with two UDFs and three aggregates per departure hour.

It has the same midnight bug as airport.py. The query computes sum and average of the
duration UDF and the maximum of a second offset UDF in one aggregate. Prism merges the
field lineage of both UDFs on a witness, and influence can explain any one aggregate by
its output column.
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
def duration(arr_min, dep_min):
    # Bug: a flight crossing midnight needs 1440 minutes added, which this omits.
    return float(arr_min - dep_min)


@udf(DoubleType())
def offset(dep_min):
    # Minutes past the hour of departure, from a second independent UDF.
    return float(dep_min % 60)


def build(spark, p=None, mode="column"):
    path = os.path.join(DATA, "airport.parquet")
    if not gen.exists(spark, path):
        gen.gen_one(spark, "airport")
    flights = spark.read.parquet(path)
    if p is not None:
        p.enable_udf_aware(duration, offset, mode=mode)
        p.enable_influence()                              # all aggregates
    result = (flights
              .withColumn("dur", duration("arr_min", "dep_min"))
              .withColumn("offset", offset("dep_min"))
              .groupBy("dep_hr")
              .agg(F.sum("dur").alias("total"),
                   F.avg("dur").alias("mean_dur"),
                   F.max("offset").alias("max_off")))

    def select(rows):
        return [i for r, i in rows if r["total"] is not None and r["total"] < 0]
    return flights, result, select, "total"


def main():
    spark = (SparkSession.builder.appName("aircraft-multi").master("local[4]")
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
        print("  merged field lineage (both UDFs): data=%s control=%s regime=%s"
              % (w.get("_data"), w.get("_control"), w.get("_regime")))
    # Choose the aggregate to explain by its output column.
    print("  ranked by influence on total (sum dur): ", cur.influence("total").ranked()[:3])
    print("  ranked by influence on mean_dur (avg):  ", cur.influence("mean_dur").ranked()[:3])
    spark.stop()


if __name__ == "__main__":
    main()
