# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Distributed data generator for the debugging benchmark programs.

Each program gets one anomalous group of ``FG`` rows that holds the fault records, and the
remaining rows fill the other groups. The faulty output therefore stays faulty at any
scale. A ``__fault`` flag marks the ground-truth fault records.

Normal rows come from ``spark.range`` and column expressions, so generation scales without
building rows on the driver. The small faulty group is built on the driver and unioned in.
Normal rows never use the anomalous group key.

Environment variables:
  BENCH_ROWS  total rows per table (default 200000).
  DISC_OUT    output directory, local or an HDFS URI.
  GEN_PARTS   output partitions (default one per 4M rows, at least 8).
  GEN_ONLY    comma-separated table names to generate.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

N = int(os.environ.get("BENCH_ROWS", "200000"))
OUT = os.environ.get("DISC_OUT", "tpcds/data/disc")
FG = 300                        # size of the faulty group, independent of scale


def exists(spark, path):
    """Return whether ``path`` exists on HDFS or the local file system."""
    jvm = spark._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hpath.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs.exists(hpath)


def _parts(n):
    if int(os.environ.get("GEN_PARTS", "0")):
        return int(os.environ["GEN_PARTS"])
    return max(8, int(n // 4_000_000) + 1)


# Normal rows use group keys pmod(id, K), which never equal the anomalous key. Data
# columns come from rand(seed), and their exact values do not matter.

def _normal_airport(spark, n):
    return (spark.range(0, n, numPartitions=_parts(n))
            .withColumn("dep_hr", F.pmod(F.col("id"), F.lit(23)).cast("int"))   # 0..22
            .withColumn("dep_min", (F.col("dep_hr") * 60 + (F.rand(1) * 60).cast("int")).cast("int"))
            .withColumn("arr_min", (F.col("dep_min") + 20 + (F.rand(2) * 160).cast("int")).cast("int"))
            .withColumn("__fault", F.lit(False))
            .selectExpr("id as rid", "dep_min", "arr_min", "dep_hr", "__fault"))


def _faulty_airport(spark):
    rows = []
    for i in range(FG):                                     # anomalous hour 23
        dep = 23 * 60 + (i % 60)
        if i < 20:                                          # crosses midnight, arr < dep
            rows.append((-(i + 1), dep, i % 41, 23, True))
        else:
            rows.append((-(i + 1), dep, dep + 1 + (i % 16), 23, False))
    return spark.createDataFrame(rows, "rid long, dep_min int, arr_min int, dep_hr int, __fault boolean")


def _normal_weather(spark, n):
    return (spark.range(0, n, numPartitions=_parts(n))
            .withColumn("value", (F.rand(3) * 300).cast("double"))
            .withColumn("unit", F.lit("mm"))
            .withColumn("month", (F.pmod(F.col("id"), F.lit(11)) + 1).cast("int"))  # 1..11
            .withColumn("__fault", F.lit(False))
            .selectExpr("id as rid", "value", "unit", "month", "__fault"))


def _faulty_weather(spark):
    rows = []
    for i in range(FG):                                     # anomalous month 12
        if i < 1:                                           # one "in" reading
            rows.append((-(i + 1), 90.0, "in", 12, True))
        else:
            rows.append((-(i + 1), float(i % 300), "mm", 12, False))
    return spark.createDataFrame(rows, "rid long, value double, unit string, month int, __fault boolean")


def _normal_student(spark, n):
    return (spark.range(0, n, numPartitions=_parts(n))
            .withColumn("pct", (50 + (F.rand(4) * 45)).cast("double"))            # 50..95
            .withColumn("grade", (F.pmod(F.col("id"), F.lit(11)) + 1).cast("int"))  # 1..11
            .withColumn("__fault", F.lit(False))
            .selectExpr("id as rid", "pct", "grade", "__fault"))


def _faulty_student(spark):
    rows = []
    for i in range(FG):                                     # anomalous grade 12
        if i < 20:                                          # mistyped percent
            rows.append((-(i + 1), 1000.0, 12, True))
        else:
            rows.append((-(i + 1), float(50 + i % 45), 12, False))
    return spark.createDataFrame(rows, "rid long, pct double, grade int, __fault boolean")


def _normal_accidents(spark, n):
    # Severities 1, 3, and 4, disjoint from the anomalous severity 2.
    return (spark.range(0, n, numPartitions=_parts(n))
            .withColumn("visibility", (5 + (F.rand(5) * 5)).cast("double"))       # 5..10
            .withColumn("severity", F.element_at(F.array(F.lit(1), F.lit(3), F.lit(4)),
                                                 (F.pmod(F.col("id"), F.lit(3)) + 1).cast("int")))
            .withColumn("__fault", F.lit(False))
            .selectExpr("id as rid", "visibility", "severity", "__fault"))


def _faulty_accidents(spark):
    rows = []
    for i in range(FG):                                     # anomalous severity 2
        if i < 120:                                         # missing visibility
            rows.append((-(i + 1), None, 2, True))
        else:
            rows.append((-(i + 1), float(5 + i % 5), 2, False))
    return spark.createDataFrame(rows, "rid long, visibility double, severity int, __fault boolean")


def _normal_sales(spark, n):
    # Stores 0..98 and days 0..29 with small amounts, disjoint from the anomalous store 99.
    return (spark.range(0, n, numPartitions=_parts(n))
            .withColumn("store", F.pmod(F.col("id"), F.lit(99)).cast("int"))
            .withColumn("day", F.pmod(F.floor(F.col("id") / 99), F.lit(30)).cast("int"))
            .withColumn("raw", (1 + F.rand(6) * 9).cast("double"))
            .withColumn("__fault", F.lit(False))
            .selectExpr("id as rid", "store", "day", "raw", "__fault"))


def _faulty_sales(spark):
    rows = []
    for i in range(FG):                                     # anomalous store 99
        day = i % 10
        if day == 3 and i < 200:                            # 20 refunds entered as -1000
            rows.append((-(i + 1), 99, day, -1000.0, True))
        else:
            rows.append((-(i + 1), 99, day, float(1 + i % 9), False))
    return spark.createDataFrame(rows, "rid long, store int, day int, raw double, __fault boolean")


def _normal_latency(spark, n):
    # stores 0..98 with whole-millisecond latencies 1..9; the anomalous store 99 is disjoint
    return (spark.range(0, n, numPartitions=_parts(n))
            .withColumn("store", F.pmod(F.col("id"), F.lit(99)).cast("int"))
            .withColumn("day", F.pmod(F.floor(F.col("id") / 99), F.lit(30)).cast("int"))
            .withColumn("raw", (1 + F.floor(F.rand(7) * 9)).cast("double"))
            .withColumn("__fault", F.lit(False))
            .selectExpr("id as rid", "store", "day", "raw", "__fault"))


def _faulty_latency(spark):
    rows = []
    for i in range(FG):                                     # anomalous store 99
        if i < 15:                                          # top 5%, lifts the p95
            rows.append((-(i + 1), 99, i % 30, 1000.0, True))
        else:
            rows.append((-(i + 1), 99, i % 30, float(1 + i % 9), False))
    return spark.createDataFrame(rows, "rid long, store int, day int, raw double, __fault boolean")


_GEN = {
    "airport":   (_normal_airport, _faulty_airport),
    "weather":   (_normal_weather, _faulty_weather),
    "student":   (_normal_student, _faulty_student),
    "accidents": (_normal_accidents, _faulty_accidents),
    "sales":     (_normal_sales, _faulty_sales),
    "latency":   (_normal_latency, _faulty_latency),
}


def gen_one(spark, pid, n=None, out=None):
    """Generate one program's table with ``n`` total rows, ``FG`` of them in the faulty group."""
    n = N if n is None else n
    out = OUT if out is None else out
    normal_fn, faulty_fn = _GEN[pid]
    normal = normal_fn(spark, max(0, n - FG))
    faulty = faulty_fn(spark)
    # Normal rids are non-negative and faulty rids are negative, so rids stay unique.
    df = normal.unionByName(faulty)
    path = os.path.join(out, pid + ".parquet")
    df.write.mode("overwrite").parquet(path)
    print("wrote ~%d %s rows (FG=%d faulty group) -> %s" % (n, pid, FG, path))


def main():
    spark = (SparkSession.builder.master(os.environ.get("SPARK_MASTER", "local[4]"))
             .config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    only = os.environ.get("GEN_ONLY")
    for pid in (only.split(",") if only else _GEN):
        gen_one(spark, pid)
    spark.stop()


if __name__ == "__main__":
    main()
