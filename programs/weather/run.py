import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType

from prism import Prism, udf


@udf(DoubleType())
def to_mm(value, unit):
    return float(value if unit == "mm" else value * 304.8)


def job(spark, data, p=None):
    if p is not None:
        p.enable_capture()
        p.enable_udf_aware(to_mm, mode="lazy-cell")
        p.enable_influence()
    readings = spark.read.parquet(f"{data}/weather.parquet")
    return (readings.withColumn("mm", to_mm("value", "unit"))
            .groupBy("month").agg(F.max("mm").alias("max_mm")))


def suspicious(rows):
    return max((r for r in rows if r[0]["max_mm"] is not None), key=lambda r: r[0]["max_mm"])


AGG = "max_mm"

if __name__ == "__main__":
    spark = SparkSession.builder.appName("weather").getOrCreate()
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
