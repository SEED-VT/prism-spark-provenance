#!/usr/bin/env python3
"""TPC-DS q96: count of late-evening sales by store."""
import os
import sys

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "python"))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from prism import Prism, udf

DATA = os.environ.get("BENCH_DATA", "tpcds/data/sf0.2")


@udf(DoubleType())
def measure(ss_sold_time_sk):
    # Each qualifying sale counts as 1. The measure reads a fact-table column only to
    # anchor the trace there.
    return 1.0


def build(spark, p=None, mode="column"):
    if p is not None:
        p.enable_udf_aware(measure, mode=mode)
        p.enable_influence("sum")

    ss = spark.read.parquet(DATA + "/store_sales.parquet").withColumn("m", measure("ss_sold_time_sk"))
    td = spark.read.parquet(DATA + "/time_dim.parquet").select("t_time_sk", "t_hour", "t_minute")
    hd = spark.read.parquet(DATA + "/household_demographics.parquet").select("hd_demo_sk", "hd_dep_count")
    st = spark.read.parquet(DATA + "/store.parquet").select("s_store_sk", "s_store_name")
    sales = (ss
             .join(td, ss["ss_sold_time_sk"] == td["t_time_sk"])
             .join(hd, ss["ss_hdemo_sk"] == hd["hd_demo_sk"])
             .join(st, ss["ss_store_sk"] == st["s_store_sk"])
             .filter((F.col("t_hour") == 20) & (F.col("hd_dep_count") == 7)))
    result = (sales
              .groupBy("s_store_name").agg(F.sum("m").alias("agg")))

    def select(rows):
        return [max((ri for ri in rows if ri[0]["agg"] is not None),
                    key=lambda ri: ri[0]["agg"])[1]]
    return ss, result, select, "agg"


def main():
    spark = (SparkSession.builder.appName("q96").master("local[4]")
             .config("spark.sql.autoBroadcastJoinThreshold", "-1")
             .config("spark.sql.extensions", "org.apache.spark.sql.lineage.PrismSQLExtension")
             .config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    p = Prism(spark)
    p.enable_capture()
    src, result, select, agg = build(spark, p)
    rows = p.collect_with_lineage(result)
    bad = select(rows)
    cur = p.trace(result, bad).go_back()
    while not cur.at_scan:
        cur = cur.go_back()
    w = cur.show()
    print("traced to %d source records" % len(w))
    if w:
        print("  field lineage: data=%s control=%s regime=%s"
              % (w[0].get("_data"), w[0].get("_control"), w[0].get("_regime")))
    print("  ranked by influence:", cur.influence("agg").ranked()[:5])
    spark.stop()


if __name__ == "__main__":
    main()