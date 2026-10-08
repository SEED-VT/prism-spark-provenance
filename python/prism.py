# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Prism: record-level provenance extended with UDF-aware provenance and influence.

RecordLineage does the capture and the backward trace. Prism annotates the witnesses a
trace reaches with the fields each UDF read, split into data and control
dependencies, and ranks them by influence on the traced output.

    p = Prism(spark)
    p.enable_capture()              # record-level capture
    p.enable_udf_aware(duration)    # trace through this UDF, down to fields
    p.enable_influence("sum")       # rank witnesses by influence on the output

    result = <normal Spark job that uses the UDF>
    rows   = p.collect_with_lineage(result)         # [(row, lineage_id), ...]
    bad    = [i for r, i in rows if r["total"] < 0] # the suspicious output(s)

    cur = p.trace(result, bad).go_back()            # backward trace
    cur.show()        # witness source records, each with its field-level lineage
    cur.influence()   # the witnesses ranked by influence on the output

``trace().go_back()`` is the general lineage walk, and the annotation applies
to whatever witnesses it reaches. A UDF must be declared with ``@prism.udf`` so
Prism can analyze its source.
"""

import inspect
import os

from pyspark.sql.functions import udf as _spark_udf
from pyspark.sql.types import StructType, StructField, LongType

from recordlineage import RecordLineage
from fineprov import (trace as _ftrace, tag, lineage, control_prov, regime,
                      column_provenance as _colprov)
from influence import Influence

__all__ = ["Prism", "udf"]

# Regimes ordered coarsest first. A record is only as exact as its weakest measure.
_REGIME_ORDER = ("coarse", "model", "exact")
_REGIME_CODE = {"exact": 0, "model": 1, "coarse": 2}
_REGIME_NAME = ("exact", "model", "coarse")


def _coarsest(regimes):
    for r in _REGIME_ORDER:
        if r in regimes:
            return r
    return next(iter(regimes), "exact")


def _encode(params, data, control, reg):
    """Pack a row's field taint into one integer provcode.

    The low two bits hold the regime, followed by a data bitmask and a control
    bitmask over ``params``. The code decodes given only the parameter list, so it
    is stored in the lineage table like any other captured value."""
    n = len(params)
    dm = cm = 0
    for i, p in enumerate(params):
        if p in data:
            dm |= (1 << i)
        if p in control:
            cm |= (1 << i)
    return _REGIME_CODE.get(reg, 0) | (dm << 2) | (cm << (2 + n))


def _decode(code, params):
    """Decode a provcode from :func:`_encode` into ``(data_fields, control_fields, regime)``."""
    n = len(params)
    mask = (1 << n) - 1
    dm = (code >> 2) & mask
    cm = (code >> (2 + n)) & mask
    data = [params[i] for i in range(n) if dm & (1 << i)]
    control = [params[i] for i in range(n) if cm & (1 << i)]
    return data, control, _REGIME_NAME[code & 3]


def _make_worker(fn, params):
    """Wrap a measure UDF so each call returns ``(value, provcode)``.

    The wrapper runs the fineprov tracer alongside the UDF to find which parameters
    the output depended on as data and as control. A JVM tap above the Python
    evaluation stores the provcode in the distributed lineage table by row id."""
    traced = _ftrace(fn)                  # rewrite and compile once, not per row
    def worker(*args):
        val = fn(*args)
        out = traced(*[tag(a, p) for a, p in zip(args, params)])
        code = _encode(params, set(lineage(out)), set(control_prov(out)), regime(out))
        return (val, int(code))
    worker.__name__ = "prism$" + getattr(fn, "__name__", "udf")
    return worker


def _prov_udf(fn, params):
    """Build a Spark UDF that returns only the provcode of a measure.

    Lazy-cell mode uses it to run the per-row tracer over a large witness set on the
    cluster instead of the driver."""
    traced = _ftrace(fn)
    def w(*args):
        out = traced(*[tag(a, p) for a, p in zip(args, params)])
        return int(_encode(params, set(lineage(out)), set(control_prov(out)), regime(out)))
    w.__name__ = "prov$" + getattr(fn, "__name__", "udf")
    return _spark_udf(w, LongType())


class _PrismUDF:
    """A measure UDF that Prism can trace.

    It runs the plain function by default. In cell mode it switches to a worker that
    returns ``struct<v, p>`` with the provcode, and ``__call__`` projects out ``v``
    so the user's query is unchanged."""

    def __init__(self, fn, returnType):
        self.__prism_fn__ = fn
        self._fn = fn
        self._params = list(inspect.signature(fn).parameters)
        self._rt = returnType
        self._struct = False
        self._sudf = _spark_udf(fn, returnType)     # value only until tracing is enabled

    def __call__(self, *cols):
        c = self._sudf(*cols)
        return c.getField("v") if self._struct else c

    def _enable_tracing(self):
        st = StructType([StructField("v", self._rt), StructField("p", LongType())])
        self._sudf = _spark_udf(_make_worker(self._fn, self._params), st)
        self._struct = True


def udf(returnType):
    """Declare a measure UDF that Prism can trace to its input fields.

    It is used like :func:`pyspark.sql.functions.udf` with ``returnType``."""
    return lambda fn: _PrismUDF(fn, returnType)


class Prism(RecordLineage):
    """The record-level capture and trace API, extended with UDF-aware provenance and influence.

    Call :meth:`enable_capture`, :meth:`enable_udf_aware`, and :meth:`enable_influence`
    before the query runs, then trace with :meth:`trace` and ``go_back()``."""

    def __init__(self, spark):
        super().__init__(spark)
        self._measures = []
        self._params = []
        self._mode = "column"
        self._colprov = []
        self._traced = []                # compiled tracers for lazy-cell mode
        self._agg = "sum"

    def enable_udf_aware(self, *measures, mode="column"):
        """Report which input columns each ``@prism.udf`` measure read, as data and control.

        ``mode="column"`` (default) analyzes each measure's source once on the
        driver. It reports the union over all control-flow paths and adds no cost
        to the job.

        ``mode="cell"`` runs the per-row tracer in the workers as the measure
        executes and captures each row's code in the distributed lineage table.
        It gives the split each row took, at the cost of running the rewritten UDF
        on every row.

        ``mode="lazy-cell"`` runs the job like column mode and computes the
        per-row split in ``show()`` by running the tracer over the traced witnesses
        only."""
        self._measures = [getattr(m, "__prism_fn__", m) for m in measures]
        self._params = [list(inspect.signature(fn).parameters) for fn in self._measures]
        self._mode = mode
        if mode == "column":
            self._colprov = [_colprov(fn) for fn in self._measures]
        elif mode == "cell":
            for m in measures:
                if isinstance(m, _PrismUDF):
                    m._enable_tracing()
        elif mode == "lazy-cell":
            # The job runs the measure untouched. Keep the static column provenance
            # and the compiled tracers that show() runs over the witnesses.
            self._colprov = [_colprov(fn) for fn in self._measures]
            self._traced = [_ftrace(fn) for fn in self._measures]
        else:
            raise ValueError("mode must be 'column', 'cell', or 'lazy-cell', got %r" % mode)
        return self

    def enable_influence(self, agg=None, on=None):
        """Capture each input's contribution to every aggregate of its group.

        Contributions are captured on the map side for all aggregates the query
        computes. ``cursor.influence(column)`` later selects the output column and
        reads its aggregate type from the plan. ``agg`` is the fallback type when the
        plan has no recognized aggregate, and ``on`` is ignored. Call this before the
        query runs."""
        self._agg = agg or "sum"
        self._spark.conf.set("spark.prism.sql.influence", "true")
        return self

    def trace(self, df, output_ids):
        return _PrismCursor(super().trace(df, output_ids), self, df, list(output_ids))

    # ---- focused capture ----------------------------------------------------------
    # A predicate on the group key holds on every row before the aggregation, so the
    # map-side aggregate tap records lineage only for the named groups. The trace over
    # those groups matches full capture. A predicate on an aggregate output cannot be
    # evaluated before the aggregation, so capture stays full.

    def enable_focus(self, predicate):
        """Restrict capture to the groups selected by ``predicate``.

        ``predicate`` is a SQL string over the group-key columns, e.g. ``"region = 'east'"``.
        Call this before the job runs. Capture is unrestricted if the predicate cannot
        be evaluated before the aggregation."""
        self._spark.conf.set("spark.prism.sql.focusExpr", predicate)
        return self

    def disable_focus(self):
        self._spark.conf.set("spark.prism.sql.focusExpr", "")
        return self


class _PrismCursor:
    """Wraps a record-level :class:`TraceCursor`, delegating ``go_back``/``show`` to
    it and adding the field-level lineage and influence enrichment."""

    def __init__(self, cursor, prism, df, output_ids):
        self._c = cursor
        self._p = prism
        self._df = df                 # the traced DataFrame
        self._output_ids = output_ids # the output ids the trace started from

    def go_back(self, path=0):
        return _PrismCursor(self._c.go_back(path), self._p, self._df, self._output_ids)

    def go_next(self):
        return _PrismCursor(self._c.go_next(), self._p, self._df, self._output_ids)

    def write_witnesses(self, path):
        """Write the traced witnesses to ``path``.

        See :meth:`TraceCursor.write_witnesses`."""
        return self._c.write_witnesses(path)

    def write_witnesses_distributed(self, path, parallelism=0):
        """Write the witnesses with a distributed walk.

        See :meth:`TraceCursor.write_witnesses_distributed`."""
        return self._c.write_witnesses_distributed(path, parallelism)

    def write_witnesses_auto(self, path, threshold=500000):
        """Write the witnesses with a strategy chosen by size.

        See :meth:`TraceCursor.write_witnesses_auto`."""
        return self._c.write_witnesses_auto(path, threshold)

    @property
    def ids(self):
        return self._c.ids

    @property
    def at_scan(self):
        return self._c.at_scan

    def show(self, full=False):
        """Return the witness source records, annotated with UDF field provenance if enabled.

        Each record gains ``_data`` (fields its measures read as data), ``_control``
        (fields that gated them), and ``_regime``. Column mode applies the static
        per-UDF provenance to every witness whose scan has the measure's columns.
        Cell mode fetches these witnesses' captured provcodes from the lineage table."""
        records = self._c.show(full)
        if not self._p._measures or not records:
            return records
        if self._p._mode == "column":
            return self._show_column(records)
        if self._p._mode == "lazy-cell":
            return self._show_lazy_cell(records)
        ids = [int(i) for i in self._c.ids]
        spark = self._p._spark
        jl = spark._jvm.java.util.ArrayList()
        for i in ids:
            jl.add(spark._jvm.java.lang.Long(int(i)))
        jm = self._p._api.udfProvJava(self._df._jdf, jl)
        codes, it = {}, jm.entrySet().iterator()
        while it.hasNext():
            e = it.next()
            codes[int(e.getKey())] = [int(x) for x in e.getValue()]
        params = self._p._params                    # per measure, in evaluation order
        for rec, rid in zip(records, ids):
            vec = codes.get(rid)
            if not vec:
                continue
            data, control, regimes = set(), set(), set()
            for s, code in enumerate(vec):
                if s < len(params):
                    d, c, r = _decode(code, params[s])
                    data |= set(d); control |= set(c); regimes.add(r)
            rec["_data"] = sorted(data)
            rec["_control"] = sorted(control)
            rec["_regime"] = _coarsest(regimes)
        return records

    def _show_column(self, records):
        """Annotate every witness with the union of the applicable measures' columns.

        A measure applies when the scan's columns include all the columns it read."""
        cols = set().union(*(r.keys() for r in records))
        data, control, regimes, applicable = set(), set(), set(), False
        for d, c, reg in self._p._colprov:
            atoms = set(d) | set(c)
            if atoms and atoms <= cols:
                data |= set(d); control |= set(c); regimes.add(reg); applicable = True
        if applicable:
            for rec in records:
                rec["_data"] = sorted(data)
                rec["_control"] = sorted(control)
                rec["_regime"] = _coarsest(regimes)
        return records

    # ---- lazy-cell: per-row taint computed when the witnesses are shown ------------
    def _show_lazy_cell(self, records):
        """Annotate each witness by running the tracer over its field values.

        Up to ``LAZY_CELL_DRIVER_MAX`` witnesses (env, default 50000) run on the
        driver. Larger sets run on the cluster."""
        thr = int(os.environ.get("LAZY_CELL_DRIVER_MAX", "50000"))
        if len(records) <= thr:
            for rec in records:
                self._lazy_annotate(rec)
            return records
        return self._lazy_cell_distributed(records)

    def _lazy_annotate(self, rec):
        """Run the tracer on one witness's field values and annotate the record."""
        data, control, regimes = set(), set(), set()
        for prm, traced in zip(self._p._params, self._p._traced):
            if not all(p in rec for p in prm):
                continue
            try:
                out = traced(*[tag(rec[p], p) for p in prm])
                data |= set(lineage(out)); control |= set(control_prov(out)); regimes.add(regime(out))
            except Exception:
                continue
        if regimes:
            rec["_data"] = sorted(data)
            rec["_control"] = sorted(control)
            rec["_regime"] = _coarsest(regimes)
        return rec

    def _lazy_cell_distributed(self, records):
        """Compute one provcode column per measure on the cluster and decode it onto records."""
        from pyspark.sql import functions as _F
        spark = self._p._spark
        params_list, measures = self._p._params, self._p._measures
        needed = sorted({p for prm in params_list for p in prm})
        rows = [dict([("__i", i)] + [(c, r.get(c)) for c in needed])
                for i, r in enumerate(records)]
        df = spark.createDataFrame(rows)
        codecols = []
        for s, (fn, prm) in enumerate(zip(measures, params_list)):
            cname = "__code%d" % s
            df = df.withColumn(cname, _prov_udf(fn, prm)(*[_F.col(p) for p in prm]))
            codecols.append(cname)
        by_i = {row["__i"]: [row[c] for c in codecols]
                for row in df.select(["__i"] + codecols).collect()}
        for i, rec in enumerate(records):
            vec = by_i.get(i)
            if not vec:
                continue
            data, control, regimes = set(), set(), set()
            for s, code in enumerate(vec):
                if code is None:
                    continue
                d, c, r = _decode(code, params_list[s])
                data |= set(d); control |= set(c); regimes.add(r)
            if regimes:
                rec["_data"] = sorted(data)
                rec["_control"] = sorted(control)
                rec["_regime"] = _coarsest(regimes)
        return records

    def influence(self, column=None, agg=None):
        """Rank inputs by their influence on a traced output column.

        ``column`` names an aggregate output column, and influence is computed at the
        group-by that produced it. In a nested query this selects either the outer or
        the inner aggregate. Omitting ``column`` selects the outermost aggregate.
        ``agg`` overrides the aggregate type read from the plan. Returns an
        :class:`Influence` keyed by the packed lineage ids at that level. Requires
        :meth:`Prism.enable_influence` before the query ran."""
        from influence_spark import aggregate_levels, column_level
        levels = aggregate_levels(self._df)
        if column is not None:
            loc = column_level(self._df, column)
            if loc is None:
                names = [n for lvl in levels for n, _ in lvl]
                raise KeyError("no aggregate output column %r; have %s" % (column, names))
            depth, slot, aggt = loc
            if agg:
                aggt = agg
        else:
            depth, slot = 0, 0
            aggt = agg or (levels[0][0][1] if levels else self._p._agg)
        spark = self._p._spark
        jl = spark._jvm.java.util.ArrayList()
        for i in self._output_ids:
            jl.add(spark._jvm.java.lang.Long(int(i)))
        jm = self._p._api.influenceJava(self._df._jdf, jl, aggt, slot, depth)
        scores, it = {}, jm.entrySet().iterator()
        while it.hasNext():
            e = it.next()
            scores[int(e.getKey())] = float(e.getValue())
        return Influence(scores, "distributed-capture")
