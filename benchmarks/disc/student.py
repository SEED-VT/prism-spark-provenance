#!/usr/bin/env python3
"""Student GPA: average GPA per grade.

The percent-to-GPA conversion has a miscalibrated divisor, so a mistyped percent produces
a GPA far outside [0, 4]. Prism traces the anomalously high average and explains it.
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
def gpa(pct):
    # Bug: with this divisor a mistyped percent yields an out-of-range GPA.
    return float(pct / 25.0)


def build(spark, p=None, mode="column"):
    path = os.path.join(DATA, "student.parquet")
    if not gen.exists(spark, path):
        gen.gen_one(spark, "student")
    scores = spark.read.parquet(path)
    if p is not None:
        p.enable_udf_aware(gpa, mode=mode)
        p.enable_influence("avg")

    result = (scores.withColumn("g", gpa("pct"))
              .groupBy("grade").agg(F.avg("g").alias("avg_gpa")))

    def select(rows):
        return [max((ri for ri in rows if ri[0]["avg_gpa"] is not None),
                    key=lambda ri: ri[0]["avg_gpa"])[1]]
    return scores, result, select, "avg_gpa"


def main():
    spark = (SparkSession.builder.appName("student").master("local[4]")
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