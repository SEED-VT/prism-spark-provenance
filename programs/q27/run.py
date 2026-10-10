import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType

from prism import Prism, udf


@udf(DoubleType())
def measure(ss_quantity):
    return float(ss_quantity) if ss_quantity is not None else None


def job(spark, data, p=None):
    if p is not None:
        p.enable_capture()
        p.enable_udf_aware(measure, mode="lazy-cell")
        p.enable_influence()
    ss = spark.read.parquet(f"{data}/store_sales.parquet").withColumn("m", measure("ss_quantity"))
    dt = spark.read.parquet(f"{data}/date_dim.parquet").select("d_date_sk", "d_year")
    it = spark.read.parquet(f"{data}/item.parquet").select("i_item_sk", "i_item_id")
    cd = spark.read.parquet(f"{data}/customer_demographics.parquet").select("cd_demo_sk", "cd_gender", "cd_marital_status", "cd_education_status")
    st = spark.read.parquet(f"{data}/store.parquet").select("s_store_sk", "s_state")
    sales = (ss
             .join(dt, ss["ss_sold_date_sk"] == dt["d_date_sk"])
             .join(it, ss["ss_item_sk"] == it["i_item_sk"])
             .join(cd, ss["ss_cdemo_sk"] == cd["cd_demo_sk"])
             .join(st, ss["ss_store_sk"] == st["s_store_sk"])
             .filter((F.col("cd_gender") == "M") & (F.col("cd_marital_status") == "S") & (F.col("cd_education_status") == "College") & (F.col("d_year") == 2001)))
    return sales.groupBy("i_item_id").agg(F.avg("m").alias("agg"))


def suspicious(rows):
    return max((r for r in rows if r[0]["agg"] is not None), key=lambda r: r[0]["agg"])


AGG = "agg"

if __name__ == "__main__":
    spark = SparkSession.builder.appName("q27").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    data = sys.argv[1]
    if "--prism" in sys.argv:
        p = Prism(spark)
        result = job(spark, data, p)
        rows = p.collect_with_lineage(result)
        row, row_id = suspicious(rows)
        print("suspicious output:", row)
        cur = p.trace(result, [row_id])
        while not cur.at_scan:
            cur = cur.go_back()
        witnesses = cur.show()
        print(len(witnesses), "source records traced")
        print("first:", witnesses[0])
        print("top influence:", p.trace(result, [row_id]).influence(AGG).top(5))
    else:
        for row in job(spark, data).collect():
            print(row)
    spark.stop()
