#!/bin/sh
# Controlled provenance micro-benchmark (no Spark): precision/recall vs a
# perturbation oracle, the two provenance modes, and influence localization.
set -e
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PY="${PYSPARK_PYTHON:-python3}"
exec "$PY" "$ROOT/benchmarks/microbench/provenance_bench.py" "$@"
