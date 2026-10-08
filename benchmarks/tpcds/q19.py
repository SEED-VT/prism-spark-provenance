#!/usr/bin/env python3
"""TPC-DS q19: total ext sales price by brand and manufacturer where customer and store ZIP prefixes differ."""
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
    it = spark.read.parquet(DATA + "/item.parquet").select("i_item_sk", "i_manager_id", "i_brand_id", "i_brand", "i_manufact_id", "i_manufact")
    cu = spark.read.parquet(DATA + "/customer.parquet").select("c_customer_sk", "c_current_addr_sk")
    ca = spark.read.parquet(DATA + "/customer_address.parquet").select("ca_address_sk", "ca_zip")
    st = spark.read.parquet(DATA + "/store.parquet").select("s_store_sk", "s_zip")
    sales = (ss
             .join(dt, ss["ss_sold_date_sk"] == dt["d_date_sk"])
             .join(it, ss["ss_item_sk"] == it["i_item_sk"])
             .join(cu, ss["ss_customer_sk"] == cu["c_customer_sk"])
             .join(ca, cu["c_current_addr_sk"] == ca["ca_address_sk"])
             .join(st, ss["ss_store_sk"] == st["s_store_sk"])
             .filter((F.col("i_manager_id") == 8) & (F.col("d_moy") == 11) & (F.col("d_year") == 1998) & (F.substring("ca_zip", 1, 5) != F.substring("s_zip", 1, 5))))
    result = (sales
              .groupBy(["i_brand_id", "i_brand", "i_manufact_id", "i_manufact"]).agg(F.sum("m").alias("agg")))

    def select(rows):
        return [max((ri for ri in rows if ri[0]["agg"] is not None),
                    key=lambda ri: ri[0]["agg"])[1]]
    return ss, result, select, "agg"


def main():
    spark = (SparkSession.builder.appName("q19").master("local[4]")
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