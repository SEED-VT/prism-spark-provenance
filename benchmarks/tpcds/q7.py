#!/usr/bin/env python3
"""TPC-DS q7: average quantity by item for one demographic and promotion segment."""
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
def measure(ss_quantity):
    return float(ss_quantity) if ss_quantity is not None else None


def build(spark, p=None, mode="column"):
    if p is not None:
        p.enable_udf_aware(measure, mode=mode)
        p.enable_influence("avg")

    ss = spark.read.parquet(DATA + "/store_sales.parquet").withColumn("m", measure("ss_quantity"))
    dt = spark.read.parquet(DATA + "/date_dim.parquet").select("d_date_sk", "d_year")
    it = spark.read.parquet(DATA + "/item.parquet").select("i_item_sk", "i_item_id")
    cd = spark.read.parquet(DATA + "/customer_demographics.parquet").select("cd_demo_sk", "cd_gender", "cd_marital_status", "cd_education_status")
    pr = spark.read.parquet(DATA + "/promotion.parquet").select("p_promo_sk", "p_channel_email", "p_channel_event")
    sales = (ss
             .join(dt, ss["ss_sold_date_sk"] == dt["d_date_sk"])
             .join(it, ss["ss_item_sk"] == it["i_item_sk"])
             .join(cd, ss["ss_cdemo_sk"] == cd["cd_demo_sk"])
             .join(pr, ss["ss_promo_sk"] == pr["p_promo_sk"])
             .filter((F.col("cd_gender") == "M") & (F.col("cd_marital_status") == "S") & (F.col("cd_education_status") == "College") & ((F.col("p_channel_email") == "N") | (F.col("p_channel_event") == "N")) & (F.col("d_year") == 2000)))
    result = (sales
              .groupBy("i_item_id").agg(F.avg("m").alias("agg")))

    def select(rows):
        return [max((ri for ri in rows if ri[0]["agg"] is not None),
                    key=lambda ri: ri[0]["agg"])[1]]
    return ss, result, select, "agg"


def main():
    spark = (SparkSession.builder.appName("q7").master("local[4]")
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