#!/bin/sh
# Run every benchmark suite. Data is generated on first use.
#   sh benchmarks/run_all.sh            # scale factor 0.2
#   BENCH_SF=1 sh benchmarks/run_all.sh # larger scale
set -e
HERE="$(cd "$(dirname "$0")" && pwd)"

echo "============================================================"
echo " microbench  (controlled precision/recall, no Spark)"
echo "============================================================"
sh "$HERE/microbench/run.sh"

echo "\n============================================================"
echo " disc  (debugging programs: fault localization)"
echo "============================================================"
sh "$HERE/disc/run.sh"

echo "\n============================================================"
echo " tpcds  (explain one output per query)"
echo "============================================================"
sh "$HERE/tpcds/run.sh"

echo "\n============================================================"
echo " bigbench  (sentiment query, token-level trace)"
echo "============================================================"
sh "$HERE/bigbench/run.sh"
