#!/bin/sh
# Starts one role of the demo: a Spark master, a Spark worker, JupyterLab, or a
# headless run of every notebook that fails on the first error.
set -e
SPARK_HOME="$(python3 -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))')"
export SPARK_HOME

case "${1:-lab}" in
  master)
    exec "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.master.Master \
      --host 0.0.0.0 --port 7077 --webui-port 8080
    ;;
  worker)
    exec "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.worker.Worker \
      --cores "${WORKER_CORES:-2}" --memory "${WORKER_MEMORY:-2g}" "${SPARK_MASTER:?}"
    ;;
  lab)
    exec jupyter lab --ip 0.0.0.0 --port 8888 --no-browser --allow-root \
      --ServerApp.token='' --ServerApp.password='' --notebook-dir=/work/notebooks
    ;;
  validate)
    cd /work/notebooks
    for nb in *.ipynb; do
      echo "=== running $nb"
      jupyter nbconvert --to notebook --execute --stdout \
        --ExecutePreprocessor.timeout=900 "$nb" > /dev/null
      echo "=== $nb passed"
    done
    echo "all notebooks passed"
    ;;
  *)
    exec "$@"
    ;;
esac
