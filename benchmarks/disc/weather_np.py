"""Weather analysis with a numpy UDF: maximum snowfall in millimeters per month.

The unit conversion has the same bug as weather.py, and it calls compiled numpy routines,
which a taint wrapper cannot enter because they need a real numeric type. Prism keeps the
values plain and attributes each numpy result to its arguments, so value is data lineage
and unit is control lineage.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "python"))

import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from prism import udf
import gen

DATA = os.environ.get("DISC_DATA", "tpcds/data/disc")


@udf(DoubleType())
def to_mm(value, unit):
    # Bug: any unit other than "mm" is treated as feet.
    v = np.float64(value)
    return float(v if unit == "mm" else np.multiply(v, 304.8))


def build(spark, p=None, mode="column"):
    path = os.path.join(DATA, "weather.parquet")
    if not gen.exists(spark, path):
        gen.gen_one(spark, "weather")
    readings = spark.read.parquet(path)
    if p is not None:
        p.enable_udf_aware(to_mm, mode=mode)
        p.enable_influence("max")
    result = (readings.withColumn("mm", to_mm("value", "unit"))
              .groupBy("month").agg(F.max("mm").alias("max_mm")))

    def select(rows):
        return [max((ri for ri in rows if ri[0]["max_mm"] is not None),
                    key=lambda ri: ri[0]["max_mm"])[1]]
    return readings, result, select, "max_mm"
