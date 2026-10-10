import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType

from prism import Prism, udf


@udf(DoubleType())
def amount(raw):
    return float(raw)


def job(spark, data, p=None):
    if p is not None:
        p.enable_capture()
        p.enable_udf_aware(amount, mode="lazy-cell")
        p.enable_influence()
    sales = spark.read.parquet(f"{data}/sales.parquet")
    daily = (sales.withColumn("a", amount("raw"))
             .groupBy("store", "day").agg(F.sum("a").alias("daily")))
    return daily.groupBy("store").agg(F.avg("daily").alias("avg_daily"))


def suspicious(rows):
    return min((r for r in rows if r[0]["avg_daily"] is not None), key=lambda r: r[0]["avg_daily"])


AGG = "avg_daily"

if __name__ == "__main__":
    spark = SparkSession.builder.appName("nested").getOrCreate()
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
