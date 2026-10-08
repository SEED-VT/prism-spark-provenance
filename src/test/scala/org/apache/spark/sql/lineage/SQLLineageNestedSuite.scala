/*
 * Tests influence over nested aggregation, with daily sales totals per store and day,
 * then the average daily total per store. Influence at the outer level (which day moved
 * the average) and the inner level (which sale moved its day's total) is checked
 * against Spark recomputing that aggregate with one input removed.
 */
package org.apache.spark.sql.lineage

import java.nio.file.Files

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SQLLineageNestedSuite extends AnyFunSuite with BeforeAndAfterEach with Matchers {

  @transient private var spark: SparkSession = _

  override def afterEach(): Unit = {
    if (spark != null) { spark.stop(); spark = null }
    System.clearProperty("spark.driver.port")
  }

  // Store 1 has an anomalous day 3, whose sales of 900 inflate the store's average.
  private val sales = Seq(
    (1, 1, 10), (1, 1, 20), (1, 2, 15), (1, 2, 25), (1, 3, 900), (1, 3, 5),
    (2, 1, 30), (2, 2, 40), (2, 2, 10), (2, 3, 20))

  private def newSession(): SparkSession = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("prism-sql-nested-test")
      .config("spark.sql.extensions", classOf[PrismSQLExtension].getName)
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.prism.sql.capture", "false")
      .config("spark.prism.sql.influence", "true")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val dir = Files.createTempDirectory("prism-nested").resolve("sales").toString
    spark.createDataFrame(sales).toDF("store", "day", "amount").write.parquet(dir)
    spark.conf.set("spark.prism.sql.capture", "true")
    spark.read.parquet(dir).createOrReplaceTempView("sales")
    spark
  }

  private def nested(s: SparkSession): DataFrame = s.sql(
    """SELECT store, AVG(daily) AS store_avg FROM (
      |  SELECT store, day, SUM(amount) AS daily FROM sales GROUP BY store, day)
      |GROUP BY store""".stripMargin)

  /** Influence of the inputs of the aggregate `depth` levels below store 1's output. */
  private def influenceAt(s: SparkSession, df: DataFrame, agg: String, depth: Int)
      : Seq[Double] = {
    val output = PrismSQL.collectWithLineage(df)
    val target = output.filter(_._1.getInt(0) == 1).map(_._2).toSeq
    val ids = new java.util.ArrayList[java.lang.Number]()
    target.foreach(id => ids.add(java.lang.Long.valueOf(id)))
    PrismSQL.influenceJava(df, ids, agg, 0, depth).asScala.values
      .map(_.doubleValue).toSeq.sorted
  }

  /** Plain Spark, capture off, evaluates `query` once per left-out input. */
  private def leaveOneOut(s: SparkSession, rows: Seq[Int], query: Seq[Int] => String)
      : Seq[Double] = {
    s.conf.set("spark.prism.sql.capture", "false")
    try {
      def eval(xs: Seq[Int]): Double = s.sql(query(xs)).head().getDouble(0)
      val full = eval(rows)
      rows.indices.map(i => full - eval(rows.patch(i, Nil, 1))).sorted
    } finally s.conf.set("spark.prism.sql.capture", "true")
  }

  private def values(xs: Seq[Int]): String =
    if (xs.isEmpty) "SELECT CAST(NULL AS INT) AS v WHERE false"
    else xs.map(x => s"SELECT $x AS v").mkString(" UNION ALL ")

  test("the trace walks both aggregate levels back to the store's sales") {
    val s = newSession()
    val df = nested(s)
    val output = PrismSQL.collectWithLineage(df)
    val target = output.filter(_._1.getInt(0) == 1).map(_._2).toSeq
    var cursor = PrismSQL.trace(df, target)
    cursor = cursor.goBack()
    cursor.atScan should be (false)
    cursor.ids should have length 3 // the store's three day groups
    while (!cursor.atScan) cursor = cursor.goBack()
    cursor.show().map(r => (r.getInt(0), r.getInt(1), r.getInt(2))).sorted should equal (
      sales.filter(_._1 == 1).sorted)
  }

  test("outer influence ranks days by their effect on the store average") {
    val s = newSession()
    val daily = sales.filter(_._1 == 1).groupBy(_._2).values.map(_.map(_._3).sum).toSeq
    val captured = influenceAt(s, nested(s), "avg", depth = 0)
    val oracle = leaveOneOut(s, daily,
      xs => s"SELECT CAST(AVG(v) AS DOUBLE) FROM (${values(xs)})")
    captured.zip(oracle).foreach { case (c, o) => c should be (o +- 1e-9) }
    captured should have length 3
    // Day 3 dominates, since removing it lowers the average far more than any other day.
    captured.last should be > 200.0
  }

  test("inner influence ranks sales by their effect on their day's total") {
    val s = newSession()
    val amounts = sales.filter(_._1 == 1).map(_._3)
    val captured = influenceAt(s, nested(s), "sum", depth = 1)
    captured should equal (amounts.map(_.toDouble).sorted)
    captured.last should be (900.0)
  }
}
