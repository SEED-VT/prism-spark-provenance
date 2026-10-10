import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType

from prism import Prism, udf


@udf(DoubleType())
def measure(ss_sales_price):
    return float(ss_sales_price) if ss_sales_price is not None else None


def job(spark, data, p=None):
    if p is not None:
        p.enable_capture()
        p.enable_udf_aware(measure, mode="lazy-cell")
        p.enable_influence()
    ss = spark.read.parquet(f"{data}/store_sales.parquet").withColumn("m", measure("ss_sales_price"))
    dt = spark.read.parquet(f"{data}/date_dim.parquet").select("d_date_sk", "d_year")
    st = spark.read.parquet(f"{data}/store.parquet").select("s_store_sk", "s_store_name")
    sales = (ss
             .join(dt, ss["ss_sold_date_sk"] == dt["d_date_sk"])
             .join(st, ss["ss_store_sk"] == st["s_store_sk"])
             .filter((F.col("d_year") == 2000)))
    return sales.groupBy("s_store_name").agg(F.sum("m").alias("agg"))


def suspicious(rows):
    return max((r for r in rows if r[0]["agg"] is not None), key=lambda r: r[0]["agg"])


AGG = "agg"

if __name__ == "__main__":
    spark = SparkSession.builder.appName("q43").getOrCreate()
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
