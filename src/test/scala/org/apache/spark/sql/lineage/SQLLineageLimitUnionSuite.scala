/*
 * SQL lineage tests for ORDER BY ... LIMIT (TakeOrderedAndProject) and UNION ALL.
 *
 * TakeOrderedAndProject is a keyed boundary with taps on the sort expressions, so the
 * trace has key-hash granularity and is exact when sort keys are unique. UNION ALL needs
 * no tap, since each task runs one child's partition. The trace graph routes
 * goBack(branch) by stage-partition range.
 */
package org.apache.spark.sql.lineage

import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SQLLineageLimitUnionSuite extends AnyFunSuite with BeforeAndAfterEach
  with Matchers {

  @transient private var spark: SparkSession = _

  private val salesDir = "src/test/resources/sales_parts"

  override def afterEach(): Unit = {
    if (spark != null) { spark.stop(); spark = null }
    System.clearProperty("spark.driver.port")
  }

  private def newSession(adaptive: Boolean = true): SparkSession = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("prism-sql-limit-union-test")
      .config("spark.sql.extensions", classOf[PrismSQLExtension].getName)
      .config("spark.sql.adaptive.enabled", adaptive.toString)
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.prism.sql.capture", "true")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.read.schema("category STRING, amount INT").csv(salesDir)
      .createOrReplaceTempView("sales")
    spark
  }

  // Sales totals are electronics 101109, furniture 865, groceries 355, and toys 280.

  test("ORDER BY ... LIMIT over an aggregate captured and traced to sources") {
    val s = newSession()
    val df = s.sql(
      """SELECT category, SUM(amount) AS total FROM sales
        |GROUP BY category ORDER BY total DESC LIMIT 2""".stripMargin)
    val output = PrismSQL.collectWithLineage(df)
    assert(df.queryExecution.executedPlan.toString.contains("TakeOrderedAndProject"),
      s"expected TakeOrderedAndProject:\n${df.queryExecution.executedPlan}")
    output.map(_._1).toSeq should equal (
      Seq(Row("electronics", 101109), Row("furniture", 865)))

    // Backward through the order-by-limit boundary, then the aggregate's shuffle.
    var cursor = PrismSQL.trace(df, Seq(output.head._2))
    cursor = cursor.goBack() // limit level to aggregate level
    cursor = cursor.goBack() // aggregate level to scan
    cursor.atScan should be (true)
    val witnesses = cursor.show()
    witnesses should have length 4
    witnesses.map(_.getInt(1)).sum should equal (101109)

    // The forward retrace ends at exactly the one surviving result row.
    cursor.goNext().goNext().ids should have length 1
  }

  test("ORDER BY on a column pruned from the output carried through and stripped") {
    val s = newSession()
    // amount is the sort key but not selected. The tap pair carries it through the
    // limit's projection and a Project above strips it, so the schema is unchanged.
    val df = s.sql("SELECT category FROM sales ORDER BY amount LIMIT 3")
    val output = PrismSQL.collectWithLineage(df)
    output.map(_._1) should equal (Array(Row("toys"), Row("toys"), Row("groceries")))

    // The smallest amounts are 55, 60, and 70. Sort keys are unique, so the trace is exact.
    val cursor = PrismSQL.trace(df, Seq(output.head._2)).goBack()
    cursor.atScan should be (true)
    cursor.show().toSeq should equal (Seq(Row("toys", 55)))
  }

  test("UNION ALL at the result branches route to the correct scans") {
    val s = newSession()
    val df = s.sql(
      """SELECT category, amount FROM sales WHERE amount > 300
        |UNION ALL
        |SELECT category, amount FROM sales WHERE amount < 70""".stripMargin)
    val output = PrismSQL.collectWithLineage(df)
    output should have length 6 // 420, 310, 99999, 380 and 60, 55

    val big = output.find(_._1.getInt(1) == 99999).get
    val viaFirst = PrismSQL.trace(df, Seq(big._2)).goBack(0)
    viaFirst.show().toSeq should equal (Seq(Row("electronics", 99999)))
    // The row belongs to branch 0, so branch 1 holds none of its ids.
    PrismSQL.trace(df, Seq(big._2)).goBack(1).ids shouldBe empty

    val small = output.find(_._1.getInt(1) == 55).get
    val viaSecond = PrismSQL.trace(df, Seq(small._2)).goBack(1)
    viaSecond.show().toSeq should equal (Seq(Row("toys", 55)))
    PrismSQL.trace(df, Seq(small._2)).goBack(0).ids shouldBe empty
  }

  test("UNION ALL under an aggregate partition offsets translate on re-scan") {
    val s = newSession()
    val df = s.sql(
      """SELECT category, SUM(amount) AS total FROM (
        |  SELECT category, amount FROM sales
        |  UNION ALL
        |  SELECT category, amount FROM sales
        |) GROUP BY category""".stripMargin)
    val output = PrismSQL.collectWithLineage(df)
    val electronics = output.find(_._1.getString(0) == "electronics").get
    electronics._1.getLong(1) should equal (2L * 101109)

    // From the aggregate level to the union level, both branches resolve the same 4
    // source rows. Branch 1 needs the partition-offset translation, because its stage
    // partitions are shifted past branch 0's.
    val union = PrismSQL.trace(df, Seq(electronics._2)).goBack()
    val first = union.goBack(0)
    val second = union.goBack(1)
    first.show() should have length 4
    second.show() should have length 4
    first.show().map(_.getInt(1)).sorted should equal (
      second.show().map(_.getInt(1)).sorted)
    first.show().map(_.getInt(1)).sum should equal (101109)

    // Forward from a union branch retraces to the single aggregate row.
    first.goNext().goNext().ids should have length 1
  }

  test("GROUP BY over a union of identically-grouped aggregates fused pair tapped") {
    val s = newSession()
    // The inner aggregates already partition by category, so Spark fuses the outer
    // partial and final aggregates with no shuffle between them.
    val df = s.sql(
      """SELECT category, SUM(t) AS total FROM (
        |  SELECT category, SUM(amount) AS t FROM sales GROUP BY category
        |  UNION ALL
        |  SELECT category, SUM(amount) AS t FROM sales GROUP BY category
        |) GROUP BY category ORDER BY total DESC LIMIT 2""".stripMargin)
    val output = PrismSQL.collectWithLineage(df)
    output.map(_._1).toSeq should equal (
      Seq(Row("electronics", 2L * 101109), Row("furniture", 2L * 865)))

    // The trace goes from the limit to the outer aggregate. The fused aggregate's
    // branches are its pre taps per union child, so branch i walks into union child i
    // down to the exact electronics source rows.
    val agg = PrismSQL.trace(df, Seq(output.head._2)).goBack()
    Seq(0, 1).foreach { branch =>
      var cursor = agg.goBack(branch)
      while (!cursor.atScan) { cursor = cursor.goBack() }
      val witnesses = cursor.show()
      witnesses should have length 4
      witnesses.map(_.getInt(1)).sum should equal (101109)
      witnesses.foreach(_.getString(0) should equal ("electronics"))
    }
  }

  test("UNION ALL + aggregate + ORDER BY LIMIT, the TPC-DS result shape end-to-end") {
    val s = newSession()
    val df = s.sql(
      """SELECT category, SUM(amount) AS total FROM (
        |  SELECT category, amount FROM sales
        |  UNION ALL
        |  SELECT category, amount FROM sales
        |) GROUP BY category ORDER BY total DESC LIMIT 2""".stripMargin)
    val output = PrismSQL.collectWithLineage(df)
    output.map(_._1).toSeq should equal (
      Seq(Row("electronics", 2L * 101109), Row("furniture", 2L * 865)))

    var cursor = PrismSQL.trace(df, Seq(output.head._2))
    cursor = cursor.goBack() // limit to aggregate
    cursor = cursor.goBack() // aggregate to union
    val witnesses = cursor.goBack(1).show() // second union branch
    witnesses should have length 4
    witnesses.map(_.getInt(1)).sum should equal (101109)

    // A full forward retrace goes from the scan through the union and aggregate to the
    // limit level.
    cursor.goBack(1).goNext().goNext().goNext().ids should have length 1
  }
}
