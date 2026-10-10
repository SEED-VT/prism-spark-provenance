import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType

from prism import Prism, udf


@udf(DoubleType())
def clean(visibility):
    return float(0.0 if visibility is None else visibility)


def job(spark, data, p=None):
    if p is not None:
        p.enable_capture()
        p.enable_udf_aware(clean, mode="lazy-cell")
        p.enable_influence()
    accidents = spark.read.parquet(f"{data}/accidents.parquet")
    return (accidents.withColumn("v", clean("visibility"))
            .groupBy("severity").agg(F.avg("v").alias("avg_vis")))


def suspicious(rows):
    return min((r for r in rows if r[0]["avg_vis"] is not None), key=lambda r: r[0]["avg_vis"])


AGG = "avg_vis"

if __name__ == "__main__":
    spark = SparkSession.builder.appName("accidents").getOrCreate()
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
