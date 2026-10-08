# Prism

[![CI](https://github.com/SEED-VT/prism-spark-provenance/actions/workflows/ci.yml/badge.svg)](https://github.com/SEED-VT/prism-spark-provenance/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/SEED-VT/prism-spark-provenance)](https://github.com/SEED-VT/prism-spark-provenance/releases)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

Prism is a data provenance library for Apache Spark that traces a suspicious output of a PySpark or Spark SQL job back through the job's user-defined functions (UDFs). For a given output, it returns the input records that produced it and the fields of those records that the UDFs read. It also ranks those records by how much each one moved the output. Prism attaches to unmodified Spark 4.1 as a SQL extension, so a job needs no rewriting beyond decorating its UDFs.


## Quick start with Docker

The fastest way to try Prism is the demo, which runs a two-worker Spark cluster and JupyterLab in Docker. It needs Docker with the Compose plugin and nothing else.

```bash
docker compose -f demo/docker-compose.yml up --build
```

Open <http://localhost:8888> and run the notebooks in order. The cluster scales out with `--scale spark-worker=N`, and `PRISM_SCALE` multiplies the size of the data the notebooks generate.

```bash
PRISM_SCALE=20 docker compose -f demo/docker-compose.yml up --scale spark-worker=4
```

[demo/README.md](demo/README.md) describes each notebook, the settings that size the workers, and how to check that all notebooks pass.

## Using Prism in a PySpark job

The Prism jar must be on the class path of the driver and of every executor, and the Prism extension must be enabled. [docs/install.md](docs/install.md) gives the build steps and the exact settings. With those in place, a job turns Prism on before it runs and traces afterwards.

```python
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from prism import Prism, udf

@udf(DoubleType())
def duration(arr_min, dep_min):
    return float(arr_min - dep_min)

p = Prism(spark)
p.enable_capture()                                   # record which inputs feed each output
p.enable_udf_aware(duration, mode="lazy-cell")       # record which fields the UDF read
p.enable_influence()                                 # record each input's contribution

result = (flights.withColumn("dur", duration("arr_min", "dep_min"))
          .groupBy("dep_hr").agg(F.sum("dur").alias("total")))
rows = p.collect_with_lineage(result)                # [(row, output id), ...]
bad = [i for r, i in rows if r["total"] < 0]

cur = p.trace(result, bad)
while not cur.at_scan:
    cur = cur.go_back()
cur.show()                                           # source rows, each with _data and _control
p.trace(result, bad).influence("total").top(5)       # the five most influential records
```

The `@udf` decorator from `prism` is a drop-in replacement for PySpark's decorator. It keeps the function's source so Prism can rewrite it.

## Using Prism from Scala or SQL

Prism is enabled per session through Spark's extension setting, with its jar on the class path. Start `spark-shell` the same way for Scala or SQL work.

```bash
spark-shell \
  --driver-class-path target/scala-2.13/prism_2.13-0.0.1.jar:jars/fastutil-8.5.15.jar \
  --conf spark.sql.extensions=org.apache.spark.sql.lineage.PrismSQLExtension \
  --conf spark.prism.sql.capture=true
```

With capture on, any DataFrame or SQL query runs with lineage. The query below is ordinary SQL, and the calls after it trace one of its outputs back to the source rows.

```scala
import org.apache.spark.sql.lineage.PrismSQL

val df = spark.sql("SELECT category, SUM(amount) AS total FROM sales GROUP BY category")
val output = PrismSQL.collectWithLineage(df)           // Array[(Row, output id)]
var cursor = PrismSQL.trace(df, Seq(output.head._2))
while (!cursor.atScan) cursor = cursor.goBack()
cursor.show().foreach(println)                          // the source rows behind that total
```

Capture can also be switched on and off within a session, with `SET spark.prism.sql.capture=true` in SQL or `spark.conf.set("spark.prism.sql.capture", "true")` in Scala. Queries run while it is off execute without provenance.

## Repository layout

| Path | Contents |
|---|---|
| `src/main/scala/org/apache/spark/sql/lineage` | The SQL extension, the taps it inserts into physical plans, and the trace |
| `src/main/scala/org/apache/spark/lineage` | Record-level lineage for the RDD API |
| `python/` | The Python API, the UDF rewrite (`fineprov.py`), and influence (`influence.py`) |
| `benchmarks/` | The evaluation programs and the harnesses that measure them |
| `demo/` | The Docker demo and its notebooks |
| `docs/` | Installation, design, and benchmark guides |

## Building and testing

Prism builds with sbt 1.10 on JDK 17 against Spark 4.1.2 and Scala 2.13.

```bash
sbt package                    # target/scala-2.13/prism_2.13-0.0.1.jar
sh scripts/fetch-fastutil.sh   # jars/fastutil-8.5.15.jar, the one dependency Spark lacks
SPARK_HOME=/path/to/spark-4.1.2-bin-hadoop3 sbt test
```

The Scala suites cover capture and trace for every supported operator, field lineage, holistic and nested aggregates, and partition-pruned traces. They also include a `local-cluster` run with separate executor processes, which is why `SPARK_HOME` must point at a Spark distribution. The UDF rewrite and influence tests in `python/tests` run with plain `python3`, and `python/tests/run.sh` runs the PySpark tests against a Spark distribution.

## Further reading

[docs/design.md](docs/design.md) explains how Prism captures and traces lineage. [docs/benchmarks.md](docs/benchmarks.md) explains how to generate the benchmark data and reproduce the measurements.

## Citation

If you use Prism in your research, please cite the following paper.

Muhammad Ali Gulzar. 2026. Fine-Grained Data Provenance Through User-Defined Functions in Spark. In ACM Symposium on Cloud Computing V.2 (SoCC '26), November 18–20, 2026, Singapore, Singapore. ACM, New York, NY, USA, 12 pages. <https://doi.org/10.1145/3828161.3857899>

```bibtex
@inproceedings{gulzar2026prism,
  author    = {Gulzar, Muhammad Ali},
  title     = {Fine-Grained Data Provenance Through User-Defined Functions in Spark},
  booktitle = {ACM Symposium on Cloud Computing (SoCC '26)},
  year      = {2026},
  month     = nov,
  address   = {Singapore, Singapore},
  publisher = {ACM},
  numpages  = {12},
  doi       = {10.1145/3828161.3857899},
  url       = {https://doi.org/10.1145/3828161.3857899}
}
```

## License

Prism is released under the Apache License 2.0. Files adapted from Apache Spark and fastutil keep their original license headers.
