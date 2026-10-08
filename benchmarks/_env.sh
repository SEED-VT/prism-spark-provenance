# Sourced by the benchmark run scripts, which set $ROOT to the repository root first.
# Requires SPARK_HOME. PYSPARK_PYTHON defaults to python3.
: "${SPARK_HOME:?set SPARK_HOME to a Spark 4.1.2 distribution}"
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-python3}"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
JAR="$ROOT/target/scala-2.13/prism_2.13-1.0.0.jar"
FASTUTIL="$ROOT/jars/fastutil-8.5.15.jar"
[ -f "$FASTUTIL" ] || sh "$ROOT/scripts/fetch-fastutil.sh"
OPENS="$(tr '\n' ' ' < "$ROOT/conf/java-opens.txt")"
SPARK_CONF="--master local[4] --driver-memory 4g --conf spark.hadoop.fs.defaultFS=file:/// --conf spark.ui.enabled=false --conf spark.sql.autoBroadcastJoinThreshold=-1"

# spark-submit in local mode with Prism's jar, extension, and Java options.
prism_submit() {
  "$SPARK_HOME/bin/spark-submit" $SPARK_CONF \
    --driver-class-path "$JAR:$FASTUTIL" \
    --conf spark.sql.extensions=org.apache.spark.sql.lineage.PrismSQLExtension \
    --conf "spark.driver.extraJavaOptions=$OPENS" \
    "$@"
}
