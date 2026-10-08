"""Times influence at each aggregate level of the nested and holistic programs and checks
that the faulty records rank first.

For the nested program the outer level ranks a store's days by their effect on the
average, and the inner level ranks sales by their effect on their day's total. For the
holistic program a reading's influence is the 95th percentile minus the percentile
without that reading. The nested levels are ranked by magnitude and the percentile by
sign. Prints one LEVEL JSON line per level.
"""
import json
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path[:0] = [os.path.join(HERE, "disc"), os.path.join(HERE, "..", "python")]

from pyspark.sql import SparkSession

from prism import Prism
import holistic
import nested

# (program module, [(output column, number of faulty records it should rank first)])
LEVELS = {
    "nested": (nested, [("avg_daily", 1, abs), ("daily", 20, abs)]),
    "holistic": (holistic, [("p95", 15, float)]),
}


def main():
    spark = (SparkSession.builder.appName("influence-levels")
             .master(os.environ.get("SPARK_MASTER", "local[4]"))
             .config("spark.sql.extensions", "org.apache.spark.sql.lineage.PrismSQLExtension")
             .config("spark.sql.autoBroadcastJoinThreshold", "-1")
             .config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    for name in sys.argv[1:] or LEVELS:
        mod, levels = LEVELS[name]
        p = Prism(spark)
        p.enable_capture()
        _, result, select, _ = mod.build(spark, p, "column")
        rows = p.collect_with_lineage(result)
        bad = select(rows)
        for column, planted, key in levels:
            t = time.time()
            scores = sorted(p.trace(result, bad).influence(column).scores.items(),
                            key=lambda kv: -key(kv[1]))
            secs = time.time() - t
            top, rest = scores[:planted], scores[planted:]
            print("LEVEL " + json.dumps(dict(
                program=name, column=column, scale=os.environ.get("SCALE_LABEL", "-"),
                influence_s=round(secs, 3), ranked=len(scores), planted=planted,
                ranking="magnitude" if key is abs else "signed",
                top_min=round(min(key(v) for _, v in top), 3),
                rest_max=round(max((key(v) for _, v in rest), default=0.0), 3),
                separated=bool(not rest or
                               min(key(v) for _, v in top) > max(key(v) for _, v in rest)))))
    spark.stop()


if __name__ == "__main__":
    main()
