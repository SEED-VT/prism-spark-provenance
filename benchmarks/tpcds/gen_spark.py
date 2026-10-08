#!/usr/bin/env python3
"""Distributed TPC-DS generator that runs dsdgen in parallel across the cluster and
writes typed Parquet to HDFS.

Column names and types come from a reference dataset, a small scale factor written by
gen.py, so the raw dsdgen text is cast to exactly the schema the benchmark queries
expect. The TPC-DS toolkit's dsdgen and tpcds.idx must be present at DSDGEN_DIR on
every node.

  gen_spark.py <sf> <out_dir> <reference_schema_dir>

Environment variables:
  DSDGEN_DIR       directory holding dsdgen and tpcds.idx (default /opt/tpcds-kit/tools).
  DSDGEN_PARALLEL  number of chunks for fact tables (default 96).
  TABLES           comma-separated tables to generate (default NEEDED).
"""
import glob
import os
import shutil
import subprocess
import sys
import tempfile

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

DSDGEN_DIR = os.environ.get("DSDGEN_DIR", "/opt/tpcds-kit/tools")
DSDGEN = DSDGEN_DIR + "/dsdgen"

FACTS = {"store_sales", "catalog_sales", "web_sales", "store_returns",
         "catalog_returns", "web_returns", "inventory"}
# Tables read by the TPC-DS benchmark programs.
NEEDED = ["store_sales", "catalog_sales", "web_sales", "item", "date_dim", "customer",
          "customer_address", "customer_demographics", "promotion", "store",
          "household_demographics", "time_dim"]


def chunk(child, table, sf, parallel):
    """Run dsdgen for one child slice on an executor and yield its rows as tuples."""
    d = tempfile.mkdtemp(prefix="dsd_")
    try:
        cmd = [DSDGEN, "-table", table, "-scale", str(sf), "-dir", d, "-force",
               "-distributions", DSDGEN_DIR + "/tpcds.idx"]
        if parallel > 1:
            cmd += ["-parallel", str(parallel), "-child", str(child)]
        subprocess.run(cmd, cwd=DSDGEN_DIR, check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        # dsdgen also emits the fact table's returns table, so read only this table's
        # files, "<table>.dat" or "<table>_<child>_<parallel>.dat".
        files = (glob.glob(os.path.join(d, table + ".dat"))
                 + glob.glob(os.path.join(d, table + "_[0-9]*.dat")))
        for f in files:
            with open(f, errors="replace") as fh:
                for line in fh:
                    line = line.rstrip("\n")
                    if line.endswith("|"):
                        line = line[:-1]          # dsdgen appends a trailing delimiter
                    yield tuple(line.split("|"))
    finally:
        shutil.rmtree(d, ignore_errors=True)


def main():
    sf = sys.argv[1]
    out = sys.argv[2].rstrip("/")
    if len(sys.argv) < 4:
        sys.exit("usage: gen_spark.py <sf> <out_dir> <reference_schema_dir>")
    ref = sys.argv[3].rstrip("/")
    P = int(os.environ.get("DSDGEN_PARALLEL", "96"))
    tables = os.environ.get("TABLES", ",".join(NEEDED)).split(",")

    spark = SparkSession.builder.appName("tpcds-gen-sf%s" % sf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    sc = spark.sparkContext

    for t in tables:
        schema = spark.read.parquet(ref + "/" + t + ".parquet").schema
        n = len(schema.fields)
        parallel = P if t in FACTS else 1
        rdd = (sc.parallelize(range(1, parallel + 1), parallel)
               .flatMap(lambda c, t=t, parallel=parallel: chunk(c, t, sf, parallel))
               .map(lambda r, n=n: tuple((list(r) + [None] * n)[:n])))   # pad or trim to n
        raw = StructType([StructField("c%d" % i, StringType()) for i in range(n)])
        df = spark.createDataFrame(rdd, raw)
        # Map empty strings to null and cast to the reference column's type and name.
        cols = [F.when(F.length(F.col("c%d" % i)) == 0, None)
                 .otherwise(F.col("c%d" % i)).cast(f.dataType).alias(f.name)
                for i, f in enumerate(schema.fields)]
        (df.select(*cols).write.mode("overwrite").parquet(out + "/" + t + ".parquet"))
        print("wrote %s (parallel=%d)" % (t, parallel))
    spark.stop()


if __name__ == "__main__":
    main()
