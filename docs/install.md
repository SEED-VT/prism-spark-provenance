# Installing and running Prism

Prism runs on unmodified Apache Spark. Using it takes three things. The Prism jar must be on the class path of every Spark process, the Prism SQL extension must be enabled, and the Prism Python modules must be available to the driver and the executors. This guide covers building the jar, running locally, running on a cluster, and every setting Prism reads.

## Requirements

Prism targets Apache Spark 4.1.2 built for Scala 2.13 (the `spark-4.1.2-bin-hadoop3` distribution) on Java 17. PySpark jobs need Python 3.9 or later, and the driver and every executor must run the same interpreter version, because Prism ships each UDF's rewritten code to the executors. Building from source needs sbt 1.10.7.

## Building the jar

```bash
sbt package
sh scripts/fetch-fastutil.sh
```

The build writes `target/scala-2.13/prism_2.13-0.0.1.jar`. Prism has one dependency that Spark does not already provide, fastutil 8.5.15, which sbt downloads into its cache. `scripts/fetch-fastutil.sh` downloads it into `jars/fastutil-8.5.15.jar` and checks its checksum. Every other library Prism uses comes from the Spark distribution.

## Java options

Spark 4 on Java 17 needs a set of `--add-opens` flags for the driver and the executors. The list is in `conf/java-opens.txt`, and the shell line below turns it into one option string.

```bash
OPENS="$(tr '\n' ' ' < conf/java-opens.txt)"
```

## Running locally

In local mode the driver also runs the tasks, so the jar only needs to be on the driver's class path.

```bash
PRISM=$PWD
$SPARK_HOME/bin/spark-submit --master 'local[4]' \
  --driver-class-path "$PRISM/target/scala-2.13/prism_2.13-0.0.1.jar:$PRISM/jars/fastutil-8.5.15.jar" \
  --conf spark.sql.extensions=org.apache.spark.sql.lineage.PrismSQLExtension \
  --conf "spark.driver.extraJavaOptions=$OPENS" \
  --py-files "$PRISM/python/prism.py,$PRISM/python/recordlineage.py,$PRISM/python/fineprov.py,$PRISM/python/influence.py,$PRISM/python/influence_spark.py" \
  your_job.py
```

The driver imports the Python modules too, so add `$PRISM/python` to `PYTHONPATH` when running a notebook or a script outside `spark-submit`.

## Running on a cluster

On a cluster, copy the two jars to the same path on every node, for example `/opt/prism/jars`, and put them on the system class path of the driver and of the executors. Passing them with `--jars` is not enough. A trace sends code to the executors that Spark deserializes with the system class loader. A class that only `--jars` loaded is invisible to that loader, so the trace fails with a `ClassCastException` on `SerializedLambda`.

```bash
JARS=/opt/prism/jars/prism.jar:/opt/prism/jars/fastutil.jar
$SPARK_HOME/bin/spark-submit --master spark://<master>:7077 --deploy-mode client \
  --conf spark.driver.extraClassPath="$JARS" \
  --conf spark.executor.extraClassPath="$JARS" \
  --conf spark.sql.extensions=org.apache.spark.sql.lineage.PrismSQLExtension \
  --conf "spark.driver.extraJavaOptions=$OPENS" \
  --conf "spark.executor.extraJavaOptions=$OPENS" \
  --conf spark.dynamicAllocation.enabled=false \
  --py-files "$PRISM/python/prism.py,$PRISM/python/recordlineage.py,$PRISM/python/fineprov.py,$PRISM/python/influence.py,$PRISM/python/influence_spark.py" \
  your_job.py
```

The same settings can go into `spark-defaults.conf` instead of the command line. [demo/spark-defaults.conf](../demo/spark-defaults.conf) shows them in that form.

Two cluster settings matter for correctness. Lineage lives in the block managers of the executors that produced it, with no replica, so an executor that is removed takes its lineage with it. Turn off dynamic allocation for a provenance run, as the command above does. Second, a UDF that calls a library such as numpy needs that library installed in the executors' Python, as for any PySpark job.

To check an installation, `python/tests/test_lineage_pyspark.py` runs a capture and a trace end to end and prints `ALL 7 PYSPARK TESTS PASSED`. Submit it with the settings above.

## Settings

Prism reads the following settings from the Spark configuration. The Python API sets the first three itself, so a PySpark job rarely sets them by hand.

| Setting | Default | Effect |
|---|---|---|
| `spark.prism.sql.capture` | `false` | Inserts the lineage taps into each query's physical plan. `Prism.enable_capture()` sets it. |
| `spark.prism.sql.influence` | `false` | Also records each input record's contribution to every aggregate. `Prism.enable_influence()` sets it. |
| `spark.prism.sql.focusExpr` | empty | A predicate on the group key, such as `dep_hr = 23`, that confines capture to the groups it names. `Prism.enable_focus()` sets it. |
| `spark.prism.lineage.storageLevel` | `MEMORY_AND_DISK_SER` | The Spark storage level of the lineage blocks. |
| `spark.prism.trace.prunePartitions` | `true` | Reads only the source partitions that hold the traced records when a trace turns record identifiers back into rows. |
| `spark.prism.ablation` | empty | A comma-separated list that switches individual capture and trace optimizations back to their unoptimized form, for measuring what each contributes. The values are `legacyHash`, `boxedCombiner`, `boxedJoinBuffer`, `boxedBlocks`, and `driverTrace`. |

One environment variable tunes two-phase cell capture. `LAZY_CELL_DRIVER_MAX`, 50,000 by default, is the number of traced records up to which the per-record field tracer runs on the driver. Above it, the tracer runs as a distributed job.

## Supported operators

Prism inserts taps for file scans, cached tables, Python UDFs, projections, filters, and the hash, object-hash, and sort aggregates. It also covers broadcast, shuffled hash, and sort-merge joins, windows, unions, `ORDER BY ... LIMIT`, and the generators behind `explode`, rollups, and `COUNT(DISTINCT ...)`. A query with any other physical operator, such as a nested-loop join or a global sort, stops at planning time with `PrismUnsupportedOperatorException`. Prism stops rather than return lineage it cannot vouch for. Running the same query with `spark.prism.sql.capture` set to `false` runs it without provenance.
