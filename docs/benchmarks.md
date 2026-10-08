# Benchmarks

This guide describes the benchmark programs and how to generate their data at any scale. It also explains the harnesses that measure capture overhead, storage, trace time, lineage precision and recall, and influence. Every harness prints one JSON line per measurement, so its output can be collected with `grep` and loaded with any JSON reader.

## The programs

The debugging programs come from prior work on debugging dataflow programs. Each one runs a UDF over a table and aggregates the result, and its data contains a bounded group of 300 records that carries a known fault. That group is kept disjoint from every other group's key, so the correct lineage of the faulty output is the same 300 records at every scale. Precision and recall can therefore be computed exactly.

| Program | Computes | Fault |
|---|---|---|
| `airport` | the sum of flight durations per departure hour | a UDF subtracts minute-of-day fields, so flights that cross midnight get negative durations |
| `airport_multi` | the same sum through two chained UDFs on each row | as above |
| `weather` | the maximum snowfall in millimetres per month | a UDF treats any unit other than `mm` as feet, so one reading in inches becomes huge |
| `weather_np` | the same as `weather`, with the conversion written in numpy | as above |
| `student` | the average grade per class | 20 grades were entered as percentages on the wrong scale |
| `accidents` | the summed visibility per severity level | 120 missing visibilities are coerced to zero inside a branch |
| `nested` | daily sales totals per store and day, averaged per store | one store entered a day of 20 refunds as -1000 each |
| `holistic` | the 95th-percentile latency per store | 15 of a store's 300 readings are 1000, exactly the top 5% |

The TPC-DS programs render decision-support queries as PySpark, with the aggregated measure computed by a Python UDF on the fact table and the joins and group-by left as DataFrame operations. The set is `q3`, `q12`, `q20`, `q27`, `q42`, `q43`, `q52`, `q55`, `q96`, and `q98`, plus `q7`, `q19`, and `q26`. They contain no planted fault. They measure capture and trace over joins of three to five tables, where a single output draws on millions of records.

Two smaller suites complete the set. `benchmarks/bigbench` runs a review-sentiment program over a generated product-review table, and `benchmarks/microbench/provenance_bench.py` checks field lineage and influence against a perturbation oracle in plain Python, with no Spark.

## Generating data

The debugging programs share one generator, `benchmarks/disc/gen.py`. It writes the normal records with `spark.range` and column expressions, so it runs as a distributed job at any size, and it adds the 300-record faulty group on the driver.

```bash
DISC_OUT=hdfs:///prism/disc/g10 BENCH_ROWS=900000000 \
  spark-submit <Prism settings> benchmarks/disc/gen.py
```

| Variable | Default | Meaning |
|---|---|---|
| `DISC_OUT` | `tpcds/data/disc` | the output directory, local or on HDFS |
| `BENCH_ROWS` | 200000 | rows per table |
| `GEN_ONLY` | all tables | a comma-separated subset of `airport`, `weather`, `student`, `accidents`, `sales`, `latency` |
| `GEN_PARTS` | one per 4 million rows | the number of output files |

The tables are small per row, so 90 million rows take about 1 GB. Our evaluation used 0.9 to 9 billion rows per table for 10 to 100 GB. The `nested` program reads `sales`, `holistic` reads `latency`, and `airport_multi` and `weather_np` reuse the `airport` and `weather` tables. Each program also generates its own table on first use if it is missing.

TPC-DS data comes from the official `dsdgen` generator in two ways. For a small run on one machine, `benchmarks/tpcds/gen.py <sf> <dir>` generates every table through DuckDB's built-in `dsdgen` and needs only `pip install duckdb`. At cluster scale, `benchmarks/tpcds/gen_spark.py` runs the TPC-DS toolkit's `dsdgen` in parallel chunks across the executors, which requires the toolkit to be built at the same path on every node. It casts the raw text to the column types of a reference dataset, which is a small scale factor written by `gen.py`.

```bash
python3 benchmarks/tpcds/gen.py 1 /data/tpcds/sf1-local
hdfs dfs -put /data/tpcds/sf1-local hdfs:///prism/tpcds/sf1
DSDGEN_DIR=/opt/tpcds-kit/tools DSDGEN_PARALLEL=96 \
  spark-submit <Prism settings> benchmarks/tpcds/gen_spark.py 24 hdfs:///prism/tpcds/sf24 hdfs:///prism/tpcds/sf1
```

The first argument is the scale factor, and scale factor 24 is about 10 GB.

## Running the programs

The programs read their data from `DISC_DATA` for the debugging programs and `BENCH_DATA` for TPC-DS. Every harness builds its own Spark session, reads the master from `SPARK_MASTER`, and takes the program names as arguments. Submit it with the settings in [install.md](install.md), in client mode so that its output prints to the terminal.

### Overhead, storage, and trace time

`benchmarks/measure_matrix.py` runs each program in six capture configurations and prints one `MATRIX` line per configuration. The configurations add one capability at a time, so each one's cost can be read off the difference.

| Configuration | Captures |
|---|---|
| `row` | record-level lineage |
| `row+column` | plus one set of fields per UDF, computed statically |
| `row+lazycell` | plus the per-record field split, computed at trace time |
| `row+infl`, `row+column+infl`, `row+lazycell+infl` | each of the above plus influence contributions |

For each configuration, the harness first runs the job without Prism and then with it. It reports the two times and their ratio, and the lineage stored with full capture and with the assertion on the faulty group's key. It also reports the time to trace the selected output back to the scan.

```bash
DISC_DATA=hdfs:///prism/disc/g10 SCALE_LABEL=g10 \
  spark-submit <Prism settings> benchmarks/measure_matrix.py airport weather nested holistic
```

| Field | Meaning |
|---|---|
| `baseline_s`, `capture_s`, `overhead` | the job time without and with Prism, and their ratio |
| `blanket_mb`, `focus_mb`, `storage_cut` | the lineage stored with full capture and with the assertion, and their ratio |
| `scanned`, `traced`, `reduction` | the records read, the records the trace returns, and their ratio |
| `trace_ms` | the time to trace the selected output back to source rows |
| `lin_precision`, `lin_recall`, `lin_f1` | the traced records scored against the faulty group's members |

Several variables shape a run. `ONLY_CONFIGS` restricts it to a comma-separated list of configurations. `FOCUS=0` skips the assertion-directed capture. `TRACE=0` skips the trace, and `TRACE_FIRST_ONLY=1` traces only in the first configuration, since every configuration traces the same record-level lineage. By default the trace resolves small record sets on the driver. With `TRACE_HDFS` set to a directory, it writes the traced rows there instead and switches to a distributed trace once the record set passes `TRACE_THRESHOLD` (500,000 by default). `TRACE_DIST=1` makes the trace distributed from the start. `ORDER=capture-first` runs the job with Prism before the job without it, so that repeats can alternate the order and neither run always meets a warmer cache. `SCALE_LABEL` and `REPEAT` are copied into each output line for bookkeeping.

### Field lineage and fault localization

`benchmarks/measure.py` runs each program once with field lineage and influence turned on and prints one `METRICS` line per program. Beyond the fields above, it reports how many fields the trace returns out of the source columns (`fields`, `cols`) and whether a NULL source was traced (`null`). It also reports the granularity of the field lineage (`regime`). For the debugging programs it also scores influence against the faulty records. `fault_prec1` is 1 when the top-ranked record is faulty, and `fault_recall` is the share of faulty records among the top-ranked records, counting as many as there are faulty ones. `PRISM_MODE` selects the field-lineage mode, `cell` by default.

### Influence at each aggregate level

`benchmarks/influence_levels.py` traces the `nested` and `holistic` programs, computes influence at each aggregate level, and prints one `LEVEL` line per level. Each line gives the time the influence took, the number of records ranked, and whether the planted records rank strictly above all others (`separated`).

### Checks that need no cluster

`benchmarks/validate_influence.py` compares every closed-form influence against recomputation without each record on 20,000 random groups. `benchmarks/test_native_udf.py` runs four UDFs that call numpy and pandas through Prism and through a value-wrapping taint tracker. Both run with plain `python3`.

## Getting stable numbers

Each harness run starts a fresh JVM, and `measure_matrix.py` runs every program once without Prism before measuring, so that the first measured run does not pay for class loading and file caching. Run one harness at a time on an otherwise idle cluster, so that the overhead ratio reflects capture rather than contention. Keep dynamic allocation off, since an executor that is removed takes its lineage with it and the trace would return fewer records than it should. The record counts, field counts, and storage sizes are deterministic, while the times vary from run to run by a few percent.
