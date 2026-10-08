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

"""Distributed generator for a BigBench-style product_reviews table.

Each review has an item, a rating, a token list, and a per-token sentiment score from the
lexicon. Most tokens are filler with score 0, and about 20% carry sentiment. Reviews come
from ``spark.range`` and a per-row UDF seeded by the review id, so generation scales
without building rows on the driver.

A review's sentiment data lineage is exactly its tokens with a nonzero score, and the
filler is control lineage only. The word lists and lexicon are copied into the UDF
closure so executors need no import.

Environment variables:
  BB_ROWS    number of reviews (default 40000).
  BB_ITEMS   number of distinct items (default 1500).
  BB_OUT     output path, local or HDFS.
  GEN_PARTS  output partitions (default one per 4M rows, at least 8).
"""

import os
import random

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, ArrayType, StringType,
                               DoubleType, LongType)
import lexicon as bbq

N = int(os.environ.get("BB_ROWS", "40000"))
ITEMS = int(os.environ.get("BB_ITEMS", "1500"))
OUT = os.environ.get("BB_OUT", "tpcds/data/bb/product_reviews.parquet")


def _parts(n):
    if int(os.environ.get("GEN_PARTS", "0")):
        return int(os.environ["GEN_PARTS"])
    return max(8, int(n // 4_000_000) + 1)


def exists(spark, path):
    jvm = spark._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    return hpath.getFileSystem(spark._jsc.hadoopConfiguration()).exists(hpath)


def gen(spark, n=None, out=None):
    n = N if n is None else n
    out = OUT if out is None else out
    # Copy the word lists and lexicon into closure locals so the UDF is self-contained.
    sent_words = list(bbq.SENT_WORDS)
    filler = list(bbq.FILLER)
    lex = dict(bbq.LEXICON)
    ret = StructType([StructField("tokens", ArrayType(StringType())),
                      StructField("scores", ArrayType(DoubleType()))])

    def mk(sk):
        rng = random.Random(sk)                 # deterministic per review id
        if rng.random() < 0.01:                 # 1% of reviews have no content
            return (None, None)
        k = rng.randint(8, 16)
        toks = [rng.choice(sent_words) if rng.random() < 0.2 else rng.choice(filler)
                for _ in range(k)]
        scores = [float(lex.get(t, 0.0)) for t in toks]
        return (toks, scores)

    mk_udf = F.udf(mk, ret)
    df = (spark.range(0, n, numPartitions=_parts(n))
          .withColumn("pr_review_sk", F.col("id").cast(LongType()))
          .withColumn("pr_item_sk", (F.pmod(F.col("id"), F.lit(ITEMS)) + 1).cast("int"))
          .withColumn("pr_review_rating", (F.pmod(F.col("id"), F.lit(5)) + 1).cast("int"))
          .withColumn("_ts", mk_udf(F.col("id")))
          .select("pr_review_sk", "pr_item_sk", "pr_review_rating",
                  F.col("_ts.tokens").alias("tokens"), F.col("_ts.scores").alias("scores")))
    df.write.mode("overwrite").parquet(out)
    print("wrote ~%d reviews -> %s" % (n, out))


def main():
    spark = (SparkSession.builder.master(os.environ.get("SPARK_MASTER", "local[4]"))
             .config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    gen(spark)
    spark.stop()


if __name__ == "__main__":
    main()
