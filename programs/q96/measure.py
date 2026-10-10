import json
import sys
import time

from pyspark.sql import SparkSession

from prism import Prism
import run

spark = SparkSession.builder.appName("q96-measure").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.sparkContext.addPyFile(run.__file__)
data = sys.argv[1]

run.job(spark, data).collect()

t = time.time()
run.job(spark, data).collect()
native_s = time.time() - t

p = Prism(spark)
result = run.job(spark, data, p)
t = time.time()
rows = p.collect_with_lineage(result)
prism_s = time.time() - t
lineage_mb = sum(p.lineage_size(result)) / 1e6

row, row_id = run.suspicious(rows)
t = time.time()
cur = p.trace(result, [row_id])
while not cur.at_scan:
    cur = cur.go_back()
traced = len(cur.show())
trace_s = time.time() - t

t = time.time()
p.trace(result, [row_id]).influence(run.AGG).top(5)
influence_s = time.time() - t

print(json.dumps(dict(program="q96", native_s=round(native_s, 2), prism_s=round(prism_s, 2),
                      overhead=round(prism_s / native_s, 3), lineage_mb=round(lineage_mb, 3),
                      trace_s=round(trace_s, 2), traced=traced, influence_s=round(influence_s, 2))))
spark.stop()
