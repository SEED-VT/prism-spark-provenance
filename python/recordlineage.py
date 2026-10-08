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

"""PySpark wrapper for Prism record-level SQL provenance.

Requires the Prism jar on the Spark classpath and
``spark.sql.extensions=org.apache.spark.sql.lineage.PrismSQLExtension``.

Example::

    from recordlineage import RecordLineage

    t = RecordLineage(spark)
    t.enable_capture()
    df = spark.sql("SELECT category, SUM(amount) t FROM sales GROUP BY category")
    rows_with_ids = t.collect_with_lineage(df)

    bad_id = next(i for r, i in rows_with_ids if r.category == "electronics")
    cursor = t.trace(df, [bad_id]).go_back()
    print(cursor.show())          # witness source records, as dicts
"""

import json


class TraceCursor:
    """A position in the backward/forward provenance walk."""

    def __init__(self, jcursor):
        self._j = jcursor

    @property
    def ids(self):
        """Packed (partition, rowIdx) lineage ids at the current level."""
        return list(self._j.ids())

    @property
    def at_scan(self):
        """True when the cursor is at a source (scan/cache) level."""
        return self._j.atScan()

    def go_back(self, path=0):
        """One step backward; ``path`` selects the join input at a join level."""
        return TraceCursor(self._j.goBack(path))

    def go_next(self):
        """One step forward, retracing the last go_back."""
        return TraceCursor(self._j.goNext())

    def show(self, full=False):
        """Witness source records as a list of dicts (only at a scan level).

        With ``full=True``, returns complete source records rather than the
        query's column-pruned view, when the scan shape allows it.
        """
        return [json.loads(s) for s in self._j.showJson(full)]

    def write_witnesses(self, path):
        """Write the traced source witnesses to ``path`` without collecting them.

        Returns the witness count. The backward walk still de-duplicates the frontier
        on the driver at every hop, so use :meth:`write_witnesses_distributed` when the
        frontier is too large for the driver."""
        return int(self._j.writeWitnesses(path))

    def write_witnesses_distributed(self, path, parallelism=0):
        """Write the witnesses with a fully distributed backward walk, and return the count.

        The frontier stays an RDD through every hop and is joined against the lineage
        taps, so it never reaches the driver. ``parallelism`` sets the shuffle width,
        and 0 uses the default."""
        return int(self._j.writeWitnessesDistributed(path, int(parallelism)))

    def write_witnesses_auto(self, path, threshold=500000):
        """Write the witnesses with a strategy chosen by size, and return the count.

        Up to ``threshold`` ids resolve on the driver in one pass. Larger sets use
        :meth:`write_witnesses_distributed`. It can be called on the result-level cursor."""
        return int(self._j.writeWitnessesAuto(path, int(threshold)))


class RecordLineage:
    """Driver-side API for capture and trace over PySpark DataFrames."""

    def __init__(self, spark):
        self._spark = spark
        self._api = spark._jvm.org.apache.spark.sql.lineage.PrismSQL

    def enable_capture(self):
        self._spark.conf.set("spark.prism.sql.capture", "true")

    def disable_capture(self):
        self._spark.conf.set("spark.prism.sql.capture", "false")

    def collect_with_lineage(self, df):
        """Collect ``df`` and return ``[(Row, lineage_id), ...]``."""
        rows = df.collect()
        ids = list(self._api.resultIds(df._jdf))
        if len(rows) != len(ids):
            raise RuntimeError(
                "capture mismatch: %d rows vs %d lineage ids" % (len(rows), len(ids)))
        return list(zip(rows, ids))

    def trace(self, df, output_ids):
        """Start a trace from packed result-row ids."""
        jlist = self._spark._jvm.java.util.ArrayList()
        for i in output_ids:
            jlist.add(self._spark._jvm.java.lang.Long(int(i)))
        return TraceCursor(self._api.traceJava(df._jdf, jlist))

    def release_lineage(self, df):
        """Drop all lineage blocks captured for this query."""
        self._api.releaseLineage(df._jdf)

    def lineage_size(self, df):
        """(memory_bytes, disk_bytes) currently materialized for this query's lineage."""
        t = self._api.lineageSize(df._jdf)
        return int(t._1()), int(t._2())
