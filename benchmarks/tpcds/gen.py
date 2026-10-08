#!/usr/bin/env python3
"""Generate TPC-DS data as Parquet using DuckDB's dsdgen port (no C toolchain).

    pip install duckdb
    python3 benchmarks/tpcds/gen.py [scale_factor] [output_dir]

Defaults are scale factor 0.2 (about 200 MB raw) and output tpcds/data/sf<sf>/ under
the repository root.
It writes one Parquet file per table.

Environment variables:
  DUCKDB_TMP  spill directory for DuckDB (default: the system temporary directory).
  DUCKDB_MEM  DuckDB memory limit (default 40GB).
"""
import os
import sys
import tempfile

import duckdb


def main() -> None:
    sf = sys.argv[1] if len(sys.argv) > 1 else "0.2"
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    out = sys.argv[2] if len(sys.argv) > 2 else os.path.join(root, "tpcds", "data", f"sf{sf}")
    os.makedirs(out, exist_ok=True)

    con = duckdb.connect()
    con.execute("INSTALL tpcds; LOAD tpcds;")
    # Spill to disk and cap memory so large scale factors fit on a single node.
    con.execute("SET temp_directory='%s'" % os.environ.get("DUCKDB_TMP", tempfile.gettempdir()))
    con.execute("SET memory_limit='%s'" % os.environ.get("DUCKDB_MEM", "40GB"))
    con.execute("SET preserve_insertion_order=false")
    print(f"generating TPC-DS sf={sf} ...")
    con.execute(f"CALL dsdgen(sf={sf})")
    # DuckDB's dsdgen names this column with an _sk suffix, but the spec and Spark's
    # TPC-DS queries use the bare name.
    con.execute("ALTER TABLE customer "
                "RENAME COLUMN c_last_review_date_sk TO c_last_review_date")
    tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    for t in tables:
        path = os.path.join(out, f"{t}.parquet")
        con.execute(f"COPY {t} TO '{path}' (FORMAT PARQUET)")
        n = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        print(f"  {t:<24} {n:>10} rows -> {path}")
    print(f"done: {len(tables)} tables in {out}")


if __name__ == "__main__":
    main()
