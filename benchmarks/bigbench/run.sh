#!/bin/sh
# Generate BigBench-style product_reviews and trace one sentiment output.
# Pass a query id as the first argument (default: sentiment).
set -e
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
. "$ROOT/benchmarks/_env.sh"
HERE="$ROOT/benchmarks/bigbench"
cd "$ROOT"
PYF="$ROOT/python/fineprov.py,$ROOT/python/influence.py,$HERE/lexicon.py"
export PYTHONPATH="$ROOT/python:$HERE:$PYTHONPATH"
[ -d "tpcds/data/bb/product_reviews.parquet" ] || \
  prism_submit --py-files "$PYF" "$HERE/gen.py"
prism_submit --py-files "$PYF" "$HERE/sentiment.py" "$@"
