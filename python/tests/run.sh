#!/bin/sh
# Runs the Python unit tests, the provenance micro-benchmark, and the PySpark end-to-end
# tests against the local build. Requires `sbt package` and SPARK_HOME.
set -e
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
. "$ROOT/benchmarks/_env.sh"

# Tests of the UDF rewrite and of influence, which do not need Spark.
"$PYSPARK_PYTHON" "$ROOT/python/tests/test_fineprov.py"
"$PYSPARK_PYTHON" "$ROOT/python/tests/test_influence.py"
"$PYSPARK_PYTHON" "$ROOT/benchmarks/microbench/provenance_bench.py"

export PYTHONPATH="$ROOT/python:$PYTHONPATH"
prism_submit --py-files "$ROOT/python/recordlineage.py" \
  "$ROOT/python/tests/test_lineage_pyspark.py"
