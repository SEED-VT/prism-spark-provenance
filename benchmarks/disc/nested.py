"""Nested aggregation: daily sales totals per store and day, then the average daily
total per store.

One store recorded a day of refunds as -1000 each, a sign error that drives its average
below every other store's. The trace walks through both aggregates to that store's
sales. Influence on the average ranks the store's days, and influence on a daily total
ranks that day's sales.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "python"))

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from prism import udf
import gen

DATA = os.environ.get("DISC_DATA", "tpcds/data/disc")


@udf(DoubleType())
def amount(raw):
    return float(raw)


def build(spark, p=None, mode="column"):
    path = os.path.join(DATA, "sales.parquet")
    if not gen.exists(spark, path):
        gen.gen_one(spark, "sales")
    sales = spark.read.parquet(path)
    if p is not None:
        p.enable_udf_aware(amount, mode=mode)
        p.enable_influence()
    daily = (sales.withColumn("a", amount("raw"))
             .groupBy("store", "day").agg(F.sum("a").alias("daily")))
    result = daily.groupBy("store").agg(F.avg("daily").alias("avg_daily"))

    def select(rows):
        return [min((ri for ri in rows if ri[0]["avg_daily"] is not None),
                    key=lambda ri: ri[0]["avg_daily"])[1]]
    return sales, result, select, "avg_daily"
