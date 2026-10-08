#!/usr/bin/env python3
"""Overhead ablation harness that measures each provenance layer separately.

For every benchmark program it runs six configurations, three capture levels (row,
row plus column-level UDF awareness, row plus lazy cell-level UDF awareness), each with
and without influence. Each row reports raw baseline and capture seconds, materialized
lineage, trace time, and lineage precision and recall.

Layers are disabled by replacing the matching ``enable_*`` method on the Prism instance,
so the program files need no changes. Cell mode mutates the UDF object, so the program
module is reloaded for each configuration.

Environment variables:
  ONLY_CONFIGS        comma-separated configuration labels to run.
  TRACE=0             skip the backward trace.
  TRACE_FIRST_ONLY=1  trace only the first configuration, since trace time does not
                      depend on the configuration.
  TRACE_HDFS          HDFS directory for witnesses. With it set, the trace picks a
                      driver-side or distributed walk by witness count.
  TRACE_THRESHOLD     witness count above which that choice goes distributed.
  TRACE_DIST=1        with TRACE_HDFS, always use the distributed walk.
  TRACE_PAR           output partitions for the distributed walk.
  TRACE_SHOW=0        skip collecting witnesses to the driver.
  FOCUS=0             skip the focused capture run.
  ORDER               native-first (default) or capture-first, the order of the two runs.
  SCALE_LABEL, REPEAT labels copied into each output row.
  SPARK_MASTER        Spark master URL (default local[4]).

  python3 benchmarks/measure_matrix.py [program ...]
"""
import importlib
import json
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "python"))
for fam in ("disc", "tpcds", "bigbench"):
    sys.path.insert(0, os.path.join(ROOT, "benchmarks", fam))

from pyspark.sql import SparkSession               # noqa: E402
from pyspark.sql import functions as F             # noqa: E402
from prism import Prism                            # noqa: E402
from measure import FAMILIES, GROUP_COL            # reuse the benchmark registry

# (label, udf_aware mode or None, influence). Lazy cell mode gives cell-level precision
# at close to column-level cost, so it stands in for eager cell mode.
CONFIGS = [
    ("row",               None,        False),
    ("row+column",        "column",    False),
    ("row+lazycell",      "lazy-cell", False),
    ("row+infl",          None,        True),
    ("row+column+infl",   "column",    True),
    ("row+lazycell+infl", "lazy-cell", True),
]


def _reset(spark):
    spark.conf.set("spark.prism.sql.capture", "false")
    spark.conf.set("spark.prism.sql.influence", "false")
    spark.conf.set("spark.prism.sql.focusExpr", "")


def run_one(spark, family, name, label, udf_mode, influence, do_trace=True):
    mod = importlib.import_module(name)
    importlib.reload(mod)                       # fresh UDF, since cell mode mutates it
    bmode = udf_mode or "cell"                  # mode only matters when udf_aware is on

    def native():
        _reset(spark)
        _, plain, _s, _a = mod.build(spark, None, bmode)
        t = time.time(); plain.collect()
        return time.time() - t

    # ORDER decides whether the uninstrumented run precedes or follows the captured one,
    # so that repeats can alternate and neither run always meets a warmer cache.
    capture_first = os.environ.get("ORDER", "native-first") == "capture-first"
    if not capture_first:
        base = native()

    # Instrumented run with exactly this configuration's layers. build() calls
    # enable_udf_aware and enable_influence, so disabled layers are patched to no-ops.
    p = Prism(spark); p.enable_capture(); p.disable_focus()
    if udf_mode is None:
        p.enable_udf_aware = lambda *a, **k: p
    if not influence:
        p.enable_influence = lambda *a, **k: p
    src, res, sel, agg = mod.build(spark, p, bmode)
    t = time.time(); rows = p.collect_with_lineage(res); cap = time.time() - t
    blanket_mb = sum(p.lineage_size(res)) / 1e6
    if capture_first:
        base = native()
        p.enable_capture()                      # native() switched capture off
        if influence:
            spark.conf.set("spark.prism.sql.influence", "true")

    # Trace the flagged output back to the scan. The trace walks row-level lineage, so
    # its time does not depend on the configuration.
    bad = sel(rows)
    w = []
    trace_ms = 0.0
    traced_n = 0
    if do_trace and os.environ.get("TRACE", "1") != "0":
        hdfs_base = os.environ.get("TRACE_HDFS")
        # The distributed walk keeps the frontier as an RDD across every hop, which avoids
        # rebuilding very large witness sets on the driver.
        if hdfs_base and os.environ.get("TRACE_DIST", "0") != "0":
            hp = "%s/%s_%s_%s" % (hdfs_base.rstrip("/"),
                                  os.environ.get("SCALE_LABEL", "x"), name, label)
            try:
                jvm = spark._jvm
                jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()) \
                    .delete(jvm.org.apache.hadoop.fs.Path(hp), True)
            except Exception:
                pass
            par = int(os.environ.get("TRACE_PAR", "0"))
            t = time.time()
            traced_n = p.trace(res, bad).write_witnesses_distributed(hp, par)
            trace_ms = (time.time() - t) * 1000.0
        elif hdfs_base:
            # Pick a driver-side or distributed walk by witness count. Small groups resolve
            # on the driver and large sets use the distributed walk.
            hp = "%s/%s_%s_%s" % (hdfs_base.rstrip("/"),
                                  os.environ.get("SCALE_LABEL", "x"), name, label)
            try:
                jvm = spark._jvm
                jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()) \
                    .delete(jvm.org.apache.hadoop.fs.Path(hp), True)
            except Exception:
                pass
            thr = int(os.environ.get("TRACE_THRESHOLD", "500000"))
            t = time.time()
            traced_n = p.trace(res, bad).write_witnesses_auto(hp, thr)
            trace_ms = (time.time() - t) * 1000.0
        else:
            t = time.time()
            cur = p.trace(res, bad).go_back()
            while not cur.at_scan:
                cur = cur.go_back()
            traced_n = len(cur.ids)            # forces the trace and gives the witness count
            if os.environ.get("TRACE_SHOW", "1") != "0":
                w = cur.show()                 # driver collect for field-level precision/recall
            trace_ms = (time.time() - t) * 1000.0

    # Focused capture measures the storage cost when only the suspicious group is materialized.
    focus_s = cap; focus_mb = blanket_mb
    gcol0 = GROUP_COL.get(name)
    if os.environ.get("FOCUS", "1") != "0" and gcol0 and bad and gcol0 in res.columns:
        try:
            sus = next(r for r, i in rows if i == bad[0])
            p.enable_focus("%s = %s" % (gcol0, sus[gcol0]))
            _, resF, _s, _a = mod.build(spark, p, bmode)
            t = time.time(); p.collect_with_lineage(resF); focus_s = time.time() - t
            focus_mb = sum(p.lineage_size(resF)) / 1e6
            p.disable_focus()
        except Exception:
            pass

    scanned = src.count()                          # one full-scan count, reused below

    # Lineage precision and recall against group membership as ground truth.
    gcol = GROUP_COL.get(name)
    lin_p = lin_r = 0.0
    if w and gcol and gcol in w[0]:
        anom = w[0].get(gcol)
        correct = sum(1 for r in w if r.get(gcol) == anom)
        true_n = src.filter(F.col(gcol) == anom).count()
        lin_p = correct / len(w)
        lin_r = correct / true_n if true_n else 0.0

    return dict(family=family, name=name, config=label,
                udf_mode=udf_mode or "-", influence=influence,
                scale=os.environ.get("SCALE_LABEL", "-"),
                repeat=int(os.environ.get("REPEAT", "0")),
                order="capture-first" if capture_first else "native-first",
                scanned=scanned, traced=traced_n,
                reduction=round(scanned / traced_n) if traced_n else 0,
                baseline_s=round(base, 3), capture_s=round(cap, 3),
                overhead=round(cap / base, 2) if base else 0.0,
                overhead_pct=round((cap / base - 1) * 100, 1) if base else 0.0,
                focus_s=round(focus_s, 3),
                blanket_mb=round(blanket_mb, 3), focus_mb=round(focus_mb, 3),
                storage_cut=round(blanket_mb / focus_mb, 1) if focus_mb else 0.0,
                trace_ms=round(trace_ms),
                lin_precision=round(lin_p, 3), lin_recall=round(lin_r, 3),
                lin_f1=round(2 * lin_p * lin_r / (lin_p + lin_r), 3) if (lin_p + lin_r) else 0.0)


def main():
    spark = (SparkSession.builder.appName("measure-matrix")
             .master(os.environ.get("SPARK_MASTER", "local[4]"))
             .config("spark.sql.autoBroadcastJoinThreshold", "-1")
             .config("spark.sql.extensions", "org.apache.spark.sql.lineage.PrismSQLExtension")
             .config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    only = sys.argv[1:] or None
    only_cfg = os.environ.get("ONLY_CONFIGS")
    cfgs = [c for c in CONFIGS if c[0] in only_cfg.split(",")] if only_cfg else CONFIGS
    for family, names in FAMILIES.items():
        for name in names:
            if only and name not in only:
                continue
            # Warm the data cache once per program so every configuration sees a warm baseline.
            try:
                m = importlib.import_module(name)
                m.build(spark, None, "cell")[1].collect()
            except Exception as e:
                print("WARMFAIL %s: %r" % (name, e)); continue
            first_only = os.environ.get("TRACE_FIRST_ONLY", "0") != "0"
            for ci, (label, udf_mode, infl) in enumerate(cfgs):
                try:
                    do_trace = (ci == 0) or not first_only
                    r = run_one(spark, family, name, label, udf_mode, infl, do_trace)
                    print("MATRIX " + json.dumps(r))
                except Exception as e:
                    print("FAIL %s/%s [%s]: %r" % (family, name, label, e))
    spark.stop()


if __name__ == "__main__":
    main()
