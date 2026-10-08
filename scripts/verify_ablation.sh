#!/bin/sh
# Checks that every ablation flag preserves lineage. The Scala suite asserts exact
# witness sets and traces, so a passing run with a flag on means the ablated path gives
# the same lineage. Spark loads spark.* JVM system properties into SparkConf, so
# -Dspark.prism.ablation=<flags> reaches every test session.
#
#   sh scripts/verify_ablation.sh
set -e
cd "$(dirname "$0")/.."

CONFIGS="
none:
legacyHash:legacyHash
boxedCombiner:boxedCombiner
boxedJoinBuffer:boxedJoinBuffer
boxedBlocks:boxedBlocks
driverTrace:driverTrace
fully-legacy:legacyHash,boxedCombiner,boxedJoinBuffer,boxedBlocks,driverTrace
"

PASS=0; FAIL=0; SUMMARY=""
for entry in $CONFIGS; do
  name="${entry%%:*}"; flags="${entry#*:}"
  echo "=================================================================="
  echo "  verifying ablation config: $name  [flags: ${flags:-none}]"
  echo "=================================================================="
  if bin/sbt -batch "set Test/javaOptions += \"-Dspark.prism.ablation=$flags\"" test \
       > "/tmp/ablation-verify-$name.log" 2>&1; then
    line=$(grep -E 'Tests: succeeded' "/tmp/ablation-verify-$name.log" | tail -1)
    echo "  PASS  $line"
    SUMMARY="$SUMMARY\n  $name: PASS ($line)"
    PASS=$((PASS+1))
  else
    echo "  FAIL  (see /tmp/ablation-verify-$name.log)"
    grep -E 'FAILED|Exception' "/tmp/ablation-verify-$name.log" | head -3
    SUMMARY="$SUMMARY\n  $name: FAIL"
    FAIL=$((FAIL+1))
  fi
done

echo "\n================ ablation semantic verification ================"
printf "%b\n" "$SUMMARY"
echo "  configs passed: $PASS  failed: $FAIL"
[ "$FAIL" -eq 0 ] || exit 1
