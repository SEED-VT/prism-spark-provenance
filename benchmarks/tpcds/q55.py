#!/usr/bin/env python3
"""TPC-DS q55: total ext sales price by brand (manager 28, November 1999)."""
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
def measure(ss_ext_sales_price):
    return float(ss_ext_sales_price) if ss_ext_sales_price is not None else None


def build(spark, p=None, mode="column"):
    if p is not None:
        p.enable_udf_aware(measure, mode=mode)
        p.enable_influence("sum")

    ss = spark.read.parquet(DATA + "/store_sales.parquet").withColumn("m", measure("ss_ext_sales_price"))
    dt = spark.read.parquet(DATA + "/date_dim.parquet").select("d_date_sk", "d_year", "d_moy")
    it = spark.read.parquet(DATA + "/item.parquet").select("i_item_sk", "i_manager_id", "i_brand_id", "i_brand")
    sales = (ss
             .join(dt, ss["ss_sold_date_sk"] == dt["d_date_sk"])
             .join(it, ss["ss_item_sk"] == it["i_item_sk"])
             .filter((F.col("i_manager_id") == 28) & (F.col("d_moy") == 11) & (F.col("d_year") == 1999)))
    result = (sales
              .groupBy(["i_brand_id", "i_brand"]).agg(F.sum("m").alias("agg")))

    def select(rows):
        return [max((ri for ri in rows if ri[0]["agg"] is not None),
                    key=lambda ri: ri[0]["agg"])[1]]
    return ss, result, select, "agg"


def main():
    spark = (SparkSession.builder.appName("q55").master("local[4]")
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