/*
 * Tests tracing through holistic aggregates (median, percentile, collect_list, count
 * distinct). They have no constant-size buffer, so Spark runs them through
 * ObjectHashAggregateExec or SortAggregateExec. Each test traces one group back to the
 * scan and compares the witnesses against the group's rows read directly.
 */
package org.apache.spark.sql.lineage

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.aggregate.{ObjectHashAggregateExec, SortAggregateExec}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SQLLineageHolisticSuite extends AnyFunSuite with BeforeAndAfterEach with Matchers {

  @transient private var spark: SparkSession = _

  private val salesDir = "src/test/resources/sales_parts"

  override def afterEach(): Unit = {
    if (spark != null) { spark.stop(); spark = null }
    System.clearProperty("spark.driver.port")
  }

  private def newSession(objectHash: Boolean = true): SparkSession = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("prism-sql-holistic-test")
      .config("spark.sql.extensions", classOf[PrismSQLExtension].getName)
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.sql.execution.useObjectHashAggregateExec", objectHash.toString)
      .config("spark.prism.sql.capture", "true")
      .config("spark.prism.sql.influence", "true")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.read.schema("category STRING, amount INT").csv(salesDir)
      .createOrReplaceTempView("sales")
    spark
  }

  /** Traces the output row of `category` to the scan and checks the witnesses are
   *  exactly that category's source rows. */
  private def checkTrace(s: SparkSession, df: DataFrame, category: String): Unit = {
    val output = PrismSQL.collectWithLineage(df)
    val target = output.filter(_._1.getString(0) == category).map(_._2).toSeq
    target should have length 1
    var cursor = PrismSQL.trace(df, target)
    while (!cursor.atScan) cursor = cursor.goBack()
    val oracle = s.sql(s"SELECT category, amount FROM sales WHERE category = '$category'")
      .collect().map(r => (r.getString(0), r.getInt(1))).sorted
    cursor.show().map(r => (r.getString(0), r.getInt(1))).sorted should equal (oracle)
  }

  private def planHas(df: DataFrame, cls: Class[_]): Boolean =
    df.queryExecution.executedPlan.exists(p => cls.isInstance(p))

  test("median through ObjectHashAggregateExec") {
    val s = newSession()
    val df = s.sql(
      "SELECT category, percentile(amount, 0.5) AS med FROM sales GROUP BY category")
    planHas(df, classOf[ObjectHashAggregateExec]) should be (true)
    checkTrace(s, df, "electronics")
    checkTrace(s, df, "toys")
  }

  test("approximate percentile and collect_list") {
    val s = newSession()
    val df = s.sql(
      """SELECT category, percentile_approx(amount, 0.9) AS p90,
        |       collect_list(amount) AS amounts
        |FROM sales GROUP BY category""".stripMargin)
    checkTrace(s, df, "furniture")
  }

  test("count distinct, planned as two aggregate levels") {
    val s = newSession()
    val df = s.sql(
      "SELECT category, COUNT(DISTINCT amount) AS n FROM sales GROUP BY category")
    checkTrace(s, df, "groceries")
  }

  test("median through SortAggregateExec when object-hash aggregation is off") {
    val s = newSession(objectHash = false)
    val df = s.sql(
      "SELECT category, percentile(amount, 0.5) AS med FROM sales GROUP BY category")
    planHas(df, classOf[SortAggregateExec]) should be (true)
    checkTrace(s, df, "electronics")
  }

  test("median and percentile influence equal leave-one-out recomputed by Spark") {
    val s = newSession()
    for ((fn, name) <- Seq(("percentile(amount, 0.5)", "median"),
                           ("percentile(amount, 0.25)", "percentile:0.25"))) {
      val df = s.sql(s"SELECT category, $fn AS q FROM sales GROUP BY category")
      val output = PrismSQL.collectWithLineage(df)
      val target = output.filter(_._1.getString(0) == "electronics").map(_._2).toSeq
      val outIds = new java.util.ArrayList[java.lang.Number]()
      target.foreach(id => outIds.add(java.lang.Long.valueOf(id)))
      val influence = PrismSQL.influenceJava(df, outIds, name, 0, 0)
      // Map each traced source id to its amount, then recompute the aggregate without it.
      var cursor = PrismSQL.trace(df, target)
      while (!cursor.atScan) cursor = cursor.goBack()
      val amountOf = cursor.ids.zip(cursor.show().map(_.getInt(1))).toMap
      val amounts = amountOf.values.toSeq
      // The oracle runs without capture, as plain Spark recomputing the aggregate.
      def q(xs: Seq[Int]): Double = {
        s.conf.set("spark.prism.sql.capture", "false")
        try s.createDataFrame(s.sparkContext.parallelize(xs.map(Tuple1(_))))
          .selectExpr(fn.replace("amount", "_1")).head().getDouble(0)
        finally s.conf.set("spark.prism.sql.capture", "true")
      }
      val full = q(amounts)
      influence.size should equal (amounts.length)
      // The aggregate level and the scan level number the same records.
      import scala.jdk.CollectionConverters._
      influence.asScala.values.map(_.doubleValue).toSeq.sorted should equal (
        amounts.indices.map(i => full - q(amounts.patch(i, Nil, 1))).sorted)
    }
  }

  test("influence capture on a query mixing distinct and non-distinct aggregates") {
    val s = newSession()
    val df = s.sql(
      """SELECT category, percentile(amount, 0.5) AS med, COUNT(DISTINCT amount) AS n
        |FROM sales GROUP BY category""".stripMargin)
    checkTrace(s, df, "electronics")
  }
}
