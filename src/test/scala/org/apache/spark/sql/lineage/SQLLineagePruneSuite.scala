/*
 * Tests that resolving traced ids to source rows reads only the scan partitions that
 * hold them. Each test traces the same output with and without pruning, and checks
 * that both return the same rows while the pruned read launches fewer tasks.
 */
package org.apache.spark.sql.lineage

import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SQLLineagePruneSuite extends AnyFunSuite with BeforeAndAfterEach with Matchers {

  @transient private var spark: SparkSession = _
  private val tasks = new AtomicInteger(0)

  override def afterEach(): Unit = {
    if (spark != null) { spark.stop(); spark = null }
    System.clearProperty("spark.driver.port")
  }

  private def newSession(): SparkSession = {
    spark = SparkSession.builder()
      .master("local[4]")
      .appName("prism-sql-prune-test")
      .config("spark.sql.extensions", classOf[PrismSQLExtension].getName)
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.sql.files.maxPartitionBytes", "4096")
      .config("spark.sql.files.openCostInBytes", "0")
      .config("spark.prism.sql.capture", "true")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.addSparkListener(new SparkListener {
      override def onTaskEnd(e: SparkListenerTaskEnd): Unit = tasks.incrementAndGet()
    })
    spark
  }

  /** Writes `n` rows across `files` Parquet files, with key 7 only in the last file. */
  private def writeTable(s: SparkSession, n: Int, files: Int): String = {
    val dir = Files.createTempDirectory("prism-prune").resolve("t").toString
    s.range(0, n, 1, files)
      .select(col("id"),
        (col("id") >= n - 50).cast("int").multiply(7).plus(col("id") % 5).as("k"),
        (col("id") * 3).as("v"))
      .write.parquet(dir)
    dir
  }

  /** Traces the output whose key is `key` to the scan and returns the witness rows
   *  together with the number of tasks the final source read launched. */
  private def traceToScan(s: SparkSession, df: org.apache.spark.sql.DataFrame,
      key: Int, prune: Boolean): (Seq[Row], Int) = {
    s.conf.set("spark.prism.trace.prunePartitions", prune.toString)
    val output = PrismSQL.collectWithLineage(df)
    val target = output.filter(_._1.getLong(0) == key).map(_._2).toSeq
    var cursor = PrismSQL.trace(df, target)
    while (!cursor.atScan) cursor = cursor.goBack()
    s.sparkContext.listenerBus.waitUntilEmpty()
    tasks.set(0)
    val rows = cursor.show().toSeq
    s.sparkContext.listenerBus.waitUntilEmpty()
    (rows.sortBy(_.getLong(0)), tasks.get())
  }

  test("a single-scan trace reads only the partitions that hold its ids") {
    val s = newSession()
    s.read.parquet(writeTable(s, 4000, 16)).createOrReplaceTempView("t")
    val df = s.sql("SELECT k, SUM(v) AS total, MIN(id) AS lo FROM t GROUP BY k")
    val (pruned, prunedTasks) = traceToScan(s, df, key = 7, prune = true)
    val (full, fullTasks) = traceToScan(s, df, key = 7, prune = false)
    pruned should equal (full)
    pruned.map(_.getLong(0)) should equal ((3950L until 4000L).filter(_ % 5 == 0))
    prunedTasks should be < fullTasks
    prunedTasks should be <= 2
  }

  test("a trace through a union resolves both children with pruning") {
    val s = newSession()
    s.read.parquet(writeTable(s, 2000, 8)).createOrReplaceTempView("a")
    s.read.parquet(writeTable(s, 3000, 12)).createOrReplaceTempView("b")
    val df = s.sql(
      """SELECT k, SUM(v) AS total, MIN(id) AS lo FROM (SELECT * FROM a UNION ALL SELECT * FROM b)
        |GROUP BY k""".stripMargin)
    val (pruned, prunedTasks) = traceToScan(s, df, key = 7, prune = true)
    val (full, fullTasks) = traceToScan(s, df, key = 7, prune = false)
    pruned should equal (full)
    pruned should not be empty
    prunedTasks should be < fullTasks
  }
}
