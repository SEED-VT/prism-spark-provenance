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

"""Trace one output of the BigBench sentiment query at token granularity.

The query reports the average review sentiment per item. The script takes the
lowest-rated item, traces its most negative review through the sentiment UDF, and
reports the tokens that carried the sentiment, the dominant token by influence, and how
much smaller the field-level trace is than a record-level trace.
"""

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(os.path.dirname(_HERE))
sys.path.insert(0, _HERE)
sys.path.insert(0, os.path.join(_ROOT, "python"))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

import lexicon as bbq
from fineprov import trace, tag_row, taglist, lineage, regime
from influence import influence

DATA = os.environ.get("BB_DATA", "tpcds/data/bb/product_reviews.parquet")


def main():
    spark = (SparkSession.builder.master(os.environ.get("SPARK_MASTER", "local[4]")).appName("prism-bb")
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    rev = spark.read.parquet(DATA)
    n_scanned = rev.count()
    # Spark-side sentiment is the sum of token scores, the input to the aggregate.
    sent = F.udf(lambda sc: float(sum(sc)) if sc else None, DoubleType())
    enr = rev.withColumn("__m", sent("scores"))

    # Average sentiment per item, then pick the lowest item.
    per_item = (enr.groupBy("pr_item_sk")
                .agg(F.avg("__m").alias("avg"), F.count("*").alias("n"))
                .filter("n >= 10").orderBy("avg"))
    worst = per_item.first()
    item, avg, nrev = worst["pr_item_sk"], worst["avg"], worst["n"]

    members = [r.asDict() for r in
               enr.filter(F.col("pr_item_sk") == item)
               .select("pr_review_sk", "__m", "tokens", "scores").collect()]
    valid = [m for m in members if m["scores"] is not None]
    worst_rev = min(valid, key=lambda m: m["__m"])
    toks, scores = worst_rev["tokens"], worst_rev["scores"]

    # Trace the sentiment UDF over the tagged tokens of this review.
    tsent = trace(bbq.sentiment)
    row = tag_row({}, prefix="")
    row.value["scores"] = taglist(scores, prefix="t")
    out = tsent(row)

    data_ids = lineage(out)                     # tokens whose value flowed in
    def tok(idx_id):
        return toks[int(idx_id[1:])]
    data_tokens = sorted(((tok(i), scores[int(i[1:])]) for i in data_ids),
                         key=lambda kv: abs(kv[1]), reverse=True)

    # The influence of each token on the sum is its own score.
    inf = influence([("t%d" % i, scores[i]) for i in range(len(scores))], "sum")
    top_id, top_val = inf.top(1)[0]
    total = sum(abs(s) for s in scores) or 1.0
    n_null = sum(1 for m in members if m["scores"] is None)

    print("=== BigBench sentiment (TPCx-BB q10/q18 style) on %s ===\n" % DATA)
    print("[BB-sentiment] average review sentiment per item")
    print("  product_reviews: %d reviews scanned" % n_scanned)
    print("  investigated item: pr_item_sk=%d  ->  avg sentiment = %.2f over %d reviews"
          % (item, avg, nrev))
    print("  most negative review: pr_review_sk=%d  sentiment = %.1f over %d tokens"
          % (worst_rev["pr_review_sk"], worst_rev["__m"], len(toks)))
    print("  data-lineage tokens (carry the sentiment): %s   [%d of %d tokens]"
          % (", ".join("%s(%+.1f)" % (w, s) for w, s in data_tokens),
             len(data_ids), len(toks)))
    print("  control: all %d tokens gate the result through the v!=0 test" % len(toks))
    print("  dominant contributor: '%s' (%+.1f), %.0f%% of the review's sentiment"
          % (tok(top_id), top_val, 100 * abs(top_val) / total))
    print("  regime: %s   |   NULL reviews in the item's group: %d"
          % (regime(out), n_null))
    print("\n  Compared with a record-level trace, which returns all %d reviews and"
          " every\n  token of this one, the field-level trace returns the %d "
          "sentiment-bearing\n  tokens that explain the score."
          % (nrev, len(data_ids)))
    spark.stop()


if __name__ == "__main__":
    main()
