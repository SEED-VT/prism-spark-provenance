import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType

from prism import Prism, udf


@udf(DoubleType())
def duration(arr_min, dep_min):
    return float(arr_min - dep_min)


@udf(DoubleType())
def offset(dep_min):
    return float(dep_min % 60)


def job(spark, data, p=None):
    if p is not None:
        p.enable_capture()
        p.enable_udf_aware(duration, offset, mode="lazy-cell")
        p.enable_influence()
    flights = spark.read.parquet(f"{data}/airport.parquet")
    return (flights.withColumn("dur", duration("arr_min", "dep_min"))
            .withColumn("offset", offset("dep_min"))
            .groupBy("dep_hr")
            .agg(F.sum("dur").alias("total"), F.avg("dur").alias("mean_dur"),
                 F.max("offset").alias("max_off")))


def suspicious(rows):
    return min((r for r in rows if r[0]["total"] is not None), key=lambda r: r[0]["total"])


AGG = "total"

if __name__ == "__main__":
    spark = SparkSession.builder.appName("airport_multi").getOrCreate()
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
