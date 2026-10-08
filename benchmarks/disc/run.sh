#!/bin/sh
# Run one debugging program, or all of them. Each is a standalone PySpark job that
# enables Prism. BENCH_ROWS sets the number of rows.
#   sh benchmarks/disc/run.sh airport     # one program
#   sh benchmarks/disc/run.sh             # all programs
set -e
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
. "$ROOT/benchmarks/_env.sh"
HERE="$ROOT/benchmarks/disc"
cd "$ROOT"
export PYTHONPATH="$ROOT/python:$HERE:$PYTHONPATH"
PROGRAMS="airport airport_multi weather weather_np student accidents nested holistic"
runone() {
  prism_submit "$HERE/$1.py"
}
if [ -n "$1" ]; then runone "$1"; else
  for p in $PROGRAMS; do runone "$p"; echo; done
fi
