#!/usr/bin/env bash
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
. "$ROOT/benchmarks/_env.sh"
SPARK_CONF="${SPARK_CONF/local\[4\]/${SPARK_MASTER:-local[4]}}"
prism_submit \
  --conf spark.executor.extraClassPath="$JAR:$FASTUTIL" \
  --conf "spark.executor.extraJavaOptions=$OPENS" \
  --py-files "$ROOT/python/prism.py,$ROOT/python/recordlineage.py,$ROOT/python/fineprov.py,$ROOT/python/influence.py,$ROOT/python/influence_spark.py,$(dirname "$1")/run.py" \
  "$@"
