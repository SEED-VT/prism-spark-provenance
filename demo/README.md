# Prism demo

This folder runs Prism on a small Spark cluster inside Docker, with JupyterLab as the driver. Three notebooks show what Prism adds to a Spark job, from a first trace on a simple pipeline to joins, nested aggregates, percentiles, and UDFs that call numpy. Nothing needs to be installed besides Docker with the Compose plugin.

## Start the cluster and JupyterLab

Run these commands from the repository root. The first build compiles the Prism jar from source and installs Spark through the `pyspark` package, which takes about five minutes.

```bash
docker compose -f demo/docker-compose.yml up --build
```

Compose starts a Spark master, two workers with two cores and 2 GB of memory each, and JupyterLab as the driver. Open <http://localhost:8888> for the notebooks and <http://localhost:8080> for the Spark master's page, which should list two workers. If either port is taken on your machine, set `JUPYTER_PORT` or `SPARK_UI_PORT` before the command, for example `JUPYTER_PORT=18888`.

JupyterLab runs without a token so that the demo opens with one click. Keep the ports on your own machine and do not expose them to a network.

## Scale out and scale up

The cluster scales out by adding workers and scales up by generating more data. `--scale spark-worker=N` starts N workers, and `WORKER_CORES` and `WORKER_MEMORY` size each one. The notebooks run one executor per worker with that worker's cores and memory. `PRISM_SCALE` multiplies the number of rows of every table the notebooks generate. The planted faults keep their size at every scale, as in the full benchmarks, so a larger scale means a smaller needle in a larger haystack.

```bash
WORKER_CORES=4 WORKER_MEMORY=6g PRISM_SCALE=20 \
  docker compose -f demo/docker-compose.yml up --scale spark-worker=4
```

This example starts four workers with four cores and 6 GB each, and the quickstart then generates 4 million flights instead of 200,000. The Spark master's page lists the workers, and each notebook prints the master and the scale it runs with.

## Check that everything works

The `validate` service runs every notebook from top to bottom against the same cluster and stops at the first failed cell. Each notebook asserts the results it describes, so a passing run means every claim in the notebooks held.

```bash
docker compose -f demo/docker-compose.yml --profile validate run --rm validate
```

At the default scale the run takes about two minutes and ends with the line `all notebooks passed`. The same scale settings apply to `validate`, for example `PRISM_SCALE=3 docker compose -f demo/docker-compose.yml --profile validate run --rm validate`. Shut everything down with `docker compose -f demo/docker-compose.yml --profile validate down`.

## The notebooks

All numbers below are from the default scale.

**01-quickstart.** A UDF computes each flight's duration by subtracting two minute-of-day fields, which is wrong for flights that land after midnight. The job sums the durations per departure hour, and hour 23 comes out at about minus 11 million minutes. Prism traces that total to exactly the 8,333 flights of hour 23 out of 200,000. It reports that the UDF read `arr_min` and `dep_min`, which rules out the other three columns before any value is inspected. It then ranks the midnight-crossing flights first by their influence on the sum, at minus 1,387 minutes each. The last section names the suspicious hour before the run, and the stored lineage falls from 0.82 MB to 0.035 MB.

**02-weather-control-lineage.** This notebook runs the weather benchmark on 500,000 readings. A UDF converts snowfall to millimetres but treats any unit other than `mm` as feet, so one reading recorded in inches becomes the month's maximum. Prism separates the two roles the fields play. The reading's `value` is data lineage, because it flows into the result, and its `unit` is control lineage, because it only chose the branch. The notebook also counts the tasks of the final source re-read. With the re-read restricted to the splits that hold the traced records, it launches 5 tasks instead of 28 and returns the same 300 rows.

**03-joins-nested-holistic-numpy.** This notebook covers four shapes that the first two do not.

- A sales table joins a store table, and revenue is summed per region. The trace crosses the join into both tables, and the one sale with a mistyped discount ranks first by influence.
- The `nested` benchmark sums sales per store and day and then averages the daily totals. One store entered a day of 20 refunds as -1000 each. Influence on the average ranks that day first, and influence on a daily total ranks the 20 refunds first.
- The `holistic` benchmark computes the 95th percentile of latency per store. A percentile needs every value, so Prism computes influence by removing each reading in turn. The 15 planted readings are the only ones whose removal lowers the percentile.
- The weather benchmark's conversion is rewritten with numpy. A wrapper that carries provenance inside each value cannot enter compiled numpy code, while Prism keeps values plain and reports the same field lineage as before.

## How the containers are wired

All containers run the same image. The Prism jar sits on the system class path of every Spark process at `/opt/prism/jars`, because Prism's lineage lookup sends code that Spark deserializes with the system class loader. The settings live in `spark-defaults.conf`, which also turns on the Prism extension and the Java 17 module options that Spark needs. A shared volume is mounted at `/data` in every container, so the executors read the tables that the notebooks write.

To run a notebook outside Compose, start the image on its own with `docker run --rm -p 8888:8888 prism-demo`. The notebooks then use Spark in local mode on that one container.
