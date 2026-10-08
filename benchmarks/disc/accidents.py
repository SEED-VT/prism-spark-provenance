#!/usr/bin/env python3
"""US car accidents: average visibility per severity.

The cleaning UDF maps a missing visibility reading to 0.0 instead of dropping it, which
drags one severity's average down. Prism traces the low average and surfaces the NULL
source.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "python"))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from prism import Prism, udf
import gen

DATA = os.environ.get("DISC_DATA", "tpcds/data/disc")


@udf(DoubleType())
def clean(visibility):
    # Bug: a missing visibility reading is mapped to 0.0 instead of being dropped.
    return float(0.0 if visibility is None else visibility)


def build(spark, p=None, mode="column"):
    path = os.path.join(DATA, "accidents.parquet")
    if not gen.exists(spark, path):
        gen.gen_one(spark, "accidents")
    acc = spark.read.parquet(path)
    if p is not None:
        p.enable_udf_aware(clean, mode=mode)
        p.enable_influence("avg")

    result = (acc.withColumn("v", clean("visibility"))
              .groupBy("severity").agg(F.avg("v").alias("avg_vis")))

    def select(rows):
        return [min((ri for ri in rows if ri[0]["avg_vis"] is not None),
                    key=lambda ri: ri[0]["avg_vis"])[1]]
    return acc, result, select, "avg_vis"


def main():
    spark = (SparkSession.builder.appName("accidents").master("local[4]")
             .config("spark.sql.extensions", "org.apache.spark.sql.lineage.PrismSQLExtension")
             .config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    p = Prism(spark)
    p.enable_capture()
    src, result, select, agg = build(spark, p)
    rows = p.collect_with_lineage(result)
    bad = select(rows)
    cur = p.trace(result, bad).go_back()
    w = cur.show()
    print("traced to %d source records" % len(w))
    if w:
        print("  field lineage: data=%s control=%s regime=%s"
              % (w[0].get("_data"), w[0].get("_control"), w[0].get("_regime")))
    print("  ranked by influence:", cur.influence(agg).ranked()[:3])
    spark.stop()


if __name__ == "__main__":
    main()