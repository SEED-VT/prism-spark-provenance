#!/bin/sh
# Run one TPC-DS query program, or all of them. Each query is a standalone PySpark
# script, and data is generated on first use. BENCH_SF selects the scale factor.
#   sh benchmarks/tpcds/run.sh q42      # one query
#   sh benchmarks/tpcds/run.sh          # all queries
set -e
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
. "$ROOT/benchmarks/_env.sh"
HERE="$ROOT/benchmarks/tpcds"
SF="${BENCH_SF:-0.2}"
cd "$ROOT"
[ -d "tpcds/data/sf$SF" ] || "$PYSPARK_PYTHON" "$HERE/gen.py" "$SF"
export BENCH_DATA="tpcds/data/sf$SF"
export PYTHONPATH="$ROOT/python:$HERE:$PYTHONPATH"
QUERIES="q3 q42 q52 q55 q7 q26 q96 q19 q43 q98 q27 q20 q12"
runone() {
  prism_submit "$HERE/$1.py"
}
if [ -n "$1" ]; then runone "$1"; else
  for q in $QUERIES; do runone "$q"; echo; done
fi
