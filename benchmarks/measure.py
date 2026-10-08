#!/usr/bin/env python3
"""Measurement harness that runs each benchmark program and prints one metrics row per program.

Each program module exposes ``build(spark, prism=None, mode="cell")`` and returns
``(source_df, result_df, select_fn, agg_col)``. ``source_df`` is the scanned input,
``result_df`` the final DataFrame, ``select_fn`` maps ``collect_with_lineage`` rows to
the suspicious output ids, and ``agg_col`` is the output column that influence explains.
With ``prism=None`` the job runs uninstrumented as the overhead baseline.

Each row reports records scanned and traced, traced fields, NULL sources, the top
influence share, the regime, trace latency, capture overhead, materialized lineage
size, lineage precision and recall, and fault localization accuracy.

Environment variables:
  WARMUP=0       skip the untimed warm-up run.
  PRISM_MODE     influence mode passed to each program (default "cell").
  SPARK_MASTER   Spark master URL (default local[4]).

  python3 benchmarks/measure.py [program ...]
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

from pyspark.sql import SparkSession                    # noqa: E402
from pyspark.sql import functions as F                  # noqa: E402
from prism import Prism                                 # noqa: E402

FAMILIES = {
    "disc":  ["airport", "airport_multi", "weather", "student", "accidents",
              "nested", "holistic", "weather_np"],
    "tpcds": ["q3", "q7", "q12", "q19", "q20", "q26", "q27",
              "q42", "q43", "q52", "q55", "q96", "q98"],
}


def _reset(spark):
    spark.conf.set("spark.prism.sql.capture", "false")
    spark.conf.set("spark.prism.sql.influence", "false")
    spark.conf.set("spark.prism.sql.focusExpr", "")


# The group-by key of each program, used to build a focus predicate on the suspicious group.
GROUP_COL = {"airport": "dep_hr", "airport_multi": "dep_hr", "nested": "store",
             "holistic": "store", "weather_np": "month",
             "weather": "month", "student": "grade", "accidents": "severity"}

# Ground-truth fault oracle per program, recomputed from the captured witness fields to
# match the fault definition in gen.py. It returns True for a faulty record and None when
# a required field was not captured, which skips fault localization.
FAULT_FN = {
    "airport":       lambda r: (r.get("arr_min") - r.get("dep_min") < 0)
                     if r.get("arr_min") is not None and r.get("dep_min") is not None else None,
    "airport_multi": lambda r: (r.get("arr_min") - r.get("dep_min") < 0)
                     if r.get("arr_min") is not None and r.get("dep_min") is not None else None,
    "student":       lambda r: (r.get("pct") > 100.0) if r.get("pct") is not None else None,
    # Null fields are dropped from the witness JSON, so a missing visibility marks a fault.
    "accidents":     lambda r: r.get("visibility") is None,
    "weather":       lambda r: (r.get("unit") == "in") if "unit" in r else None,
    "nested":        lambda r: (r.get("raw") >= 1000.0) if r.get("raw") is not None else None,
    "weather_np":    lambda r: (r.get("unit") == "in") if "unit" in r else None,
    "holistic":      lambda r: (r.get("raw") >= 1000.0) if r.get("raw") is not None else None,
}


def measure(spark, family, name, mode="cell"):
    mod = importlib.import_module(name)

    # Run the job once untimed to warm the page cache and JIT. A cold first read would
    # otherwise slow only the baseline and distort the overhead ratio.
    if os.environ.get("WARMUP", "1") != "0":
        _reset(spark)
        mod.build(spark, None, mode)[1].collect()

    # Uninstrumented baseline for the overhead ratio.
    _reset(spark)
    src, res, _sel, _agg = mod.build(spark, None, mode)
    t = time.time(); res.collect(); off = time.time() - t

    # Blanket instrumented run that captures lineage for every group.
    p = Prism(spark); p.enable_capture(); p.disable_focus()
    src, res, sel, agg = mod.build(spark, p, mode)
    t = time.time(); rows = p.collect_with_lineage(res); on = time.time() - t
    bad = sel(rows)
    blanket_mb = sum(p.lineage_size(res)) / 1e6         # measured materialized lineage

    t = time.time()
    cur = p.trace(res, bad).go_back()
    while not cur.at_scan:
        cur = cur.go_back()
    w = cur.show()
    inf = cur.influence(agg) if agg else cur.influence()
    trace_ms = (time.time() - t) * 1000.0

    # Focused capture names the suspicious group by its key, which confines capture to
    # that group in one pass. It applies only when the group key is a pre-aggregate column.
    focus_col = GROUP_COL.get(name)
    if focus_col and bad and focus_col in res.columns:
        sus = next(r for r, i in rows if i == bad[0])
        pred = "%s = %s" % (focus_col, sus[focus_col])
        p.enable_focus(pred)
        _, resF, _selF, _ = mod.build(spark, p, mode)
        t = time.time(); _rowsF = p.collect_with_lineage(resF); focus_t = time.time() - t
        focus_mb = sum(p.lineage_size(resF)) / 1e6
        p.disable_focus()
    else:
        focus_t, focus_mb = on, blanket_mb              # focus not applicable

    scanned = src.count()
    traced = len(w)
    cols = len([c for c in src.columns if c != "m"])   # source columns, "m" is derived
    data = set(w[0].get("_data") or []) if w else set()
    control = set(w[0].get("_control") or []) if w else set()
    # A NULL source is a lineage field that is null in a traced witness record.
    null = any(any(r.get(f) is None for f in (r.get("_data") or []) + (r.get("_control") or []))
               for r in w)
    scores = inf.scores
    tot = sum(abs(v) for v in scores.values()) or 1.0
    top = max((abs(v) for v in scores.values()), default=0.0) / tot * 100.0

    # Lineage correctness. The correct lineage of the anomalous output group is exactly
    # the input records in that group, since gen.py gives it a key no normal row uses.
    # Precision is the share of returned witnesses in the group, and recall is the share
    # of group rows returned.
    gcol = GROUP_COL.get(name)
    lin_p = lin_r = lin_f1 = 0.0
    n_fault = 0
    fault_p1 = fault_r = 0.0
    if w and gcol and gcol in w[0]:
        anom = w[0].get(gcol)
        correct = sum(1 for r in w if r.get(gcol) == anom)
        true_n = src.filter(F.col(gcol) == anom).count()
        lin_p = correct / len(w)
        lin_r = correct / true_n if true_n else 0.0
        lin_f1 = (2 * lin_p * lin_r / (lin_p + lin_r)) if (lin_p + lin_r) else 0.0

        # Fault localization ranks witnesses by influence and checks them against the oracle.
        ff = FAULT_FN.get(name)
        if ff is not None:
            flags = [ff(r) for r in w]
            if None not in flags:                       # all oracle fields captured
                n_fault = sum(1 for f in flags if f)
                ids = list(cur.ids)
                if len(ids) == len(w) and n_fault:
                    # Use Prism's own ranking so the metric reflects the method as implemented.
                    fault_of = {ids[i]: flags[i] for i in range(len(w))}
                    ranked = [rid for rid in inf.ranked() if rid in fault_of]
                    if ranked:
                        fault_p1 = 1.0 if fault_of[ranked[0]] else 0.0
                        topk = ranked[:n_fault]         # recall@|faults|
                        fault_r = sum(1 for rid in topk if fault_of[rid]) / n_fault

    return dict(family=family, name=name, scanned=scanned, traced=traced,
                reduction=round(scanned / traced) if traced else 0,
                fields=len(data | control), cols=cols, null=int(null),
                top=round(top, 1), regime=w[0].get("_regime") if w else None,
                trace_ms=round(trace_ms), overhead=round(on / off, 1) if off else 0.0,
                focus_overhead=round(focus_t / off, 1) if off else 0.0,
                blanket_mb=round(blanket_mb, 3), focus_mb=round(focus_mb, 3),
                storage_cut=round(blanket_mb / focus_mb, 1) if focus_mb else 0.0,
                # Raw timings in seconds behind the overhead ratios.
                baseline_s=round(off, 3), capture_s=round(on, 3), focus_s=round(focus_t, 3),
                lin_precision=round(lin_p, 3), lin_recall=round(lin_r, 3),
                lin_f1=round(lin_f1, 3), n_fault=n_fault,
                fault_prec1=round(fault_p1, 3), fault_recall=round(fault_r, 3))


def main():
    spark = (SparkSession.builder.appName("measure").master(os.environ.get("SPARK_MASTER", "local[4]"))
             .config("spark.sql.autoBroadcastJoinThreshold", "-1")
             .config("spark.sql.extensions", "org.apache.spark.sql.lineage.PrismSQLExtension")
             .config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    only = sys.argv[1:] or None
    mode = os.environ.get("PRISM_MODE", "cell")
    for family, names in FAMILIES.items():
        for name in names:
            if only and name not in only:
                continue
            try:
                m = measure(spark, family, name, mode)
                print("METRICS " + json.dumps(m))
            except Exception as e:
                print("FAIL %s/%s: %r" % (family, name, e))
    spark.stop()


if __name__ == "__main__":
    main()
