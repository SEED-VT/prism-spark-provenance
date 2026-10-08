/*
 * SQL lineage tests that trace aggregates and joins across exchanges and multi-hop
 * queries back to source rows, with AQE and codegen on and off.
 *
 * sales_parts/ holds two CSV files, giving two scan partitions, with rows
 * "category,amount" and an electronics 99999 outlier.
 */
package org.apache.spark.sql.lineage

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SQLLineageOpsSuite extends AnyFunSuite with BeforeAndAfterEach with Matchers {

  @transient private var spark: SparkSession = _

  private val salesDir = "src/test/resources/sales_parts"
  private val ordersDir = "src/test/resources/orders_csv"
  private val customersDir = "src/test/resources/customers_csv"

  private val electronicsRows =
    Set(Row("electronics", 420), Row("electronics", 310),
        Row("electronics", 99999), Row("electronics", 380))

  override def afterEach(): Unit = {
    if (spark != null) { spark.stop(); spark = null }
    System.clearProperty("spark.driver.port")
  }

  private def newSession(adaptive: Boolean, codegen: Boolean = true): SparkSession = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("prism-sql-ops-test")
      .config("spark.sql.extensions", classOf[PrismSQLExtension].getName)
      .config("spark.sql.adaptive.enabled", adaptive.toString)
      .config("spark.sql.adaptive.optimizeSkewedJoin.enabled", "false")
      .config("spark.sql.adaptive.skewJoin.enabled", "false")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.sql.codegen.wholeStage", codegen.toString)
      .config("spark.prism.sql.capture", "true")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }

  private def sales(s: SparkSession): DataFrame =
    s.read.schema("category STRING, amount INT").csv(salesDir)

  private def orders(s: SparkSession): DataFrame =
    s.read.schema("oid STRING, cid STRING, amount INT").csv(ordersDir)

  private def customers(s: SparkSession): DataFrame =
    s.read.schema("cid STRING, name STRING").csv(customersDir)

  private def aggregateRoundTrip(s: SparkSession): Unit = {
    sales(s).createOrReplaceTempView("sales")
    val df = s.sql("SELECT category, SUM(amount) AS total FROM sales GROUP BY category")
    val output = PrismSQL.collectWithLineage(df)
    output.map(_._1).toSet should equal (Set(
      Row("electronics", 101109L), Row("furniture", 865L),
      Row("groceries", 355L), Row("toys", 280L)))

    val electronicsId = output.find(_._1.getString(0) == "electronics").get._2
    var cursor = PrismSQL.trace(df, Seq(electronicsId))
    val topIds = cursor.ids
    cursor = cursor.goBack()
    cursor.atScan should be (true)
    // Electronics rows live in both input files, split across scan partitions.
    cursor.show().toSet should equal (electronicsRows)
    cursor.ids should have length 4

    // Forward retraces to exactly the electronics aggregate.
    val back = cursor.goNext()
    back.ids.toSet should equal (topIds.toSet)
  }

  test("groupBy aggregate over two scan partitions backward and forward (AQE on)") {
    aggregateRoundTrip(newSession(adaptive = true))
  }

  test("groupBy aggregate over two scan partitions backward and forward (AQE off)") {
    aggregateRoundTrip(newSession(adaptive = false))
  }

  test("groupBy aggregate interpreted path (codegen off, AQE on)") {
    aggregateRoundTrip(newSession(adaptive = true, codegen = false))
  }

  test("multi-aggregate with HAVING") {
    val s = newSession(adaptive = true)
    sales(s).createOrReplaceTempView("sales")
    val df = s.sql(
      """SELECT category, SUM(amount) AS total, COUNT(*) AS cnt, MAX(amount) AS mx
        |FROM sales GROUP BY category HAVING SUM(amount) > 300""".stripMargin)
    val output = PrismSQL.collectWithLineage(df)
    // toys (280) filtered out by HAVING
    output.map(_._1).toSet should equal (Set(
      Row("electronics", 101109L, 4L, 99999),
      Row("furniture", 865L, 4L, 250),
      Row("groceries", 355L, 4L, 110)))

    val groceriesId = output.find(_._1.getString(0) == "groceries").get._2
    val cursor = PrismSQL.trace(df, Seq(groceriesId)).goBack()
    cursor.show().toSet should equal (Set(
      Row("groceries", 80), Row("groceries", 95), Row("groceries", 110),
      Row("groceries", 70)))
  }

  test("global aggregate traces to every input row") {
    val s = newSession(adaptive = true)
    sales(s).createOrReplaceTempView("sales")
    val df = s.sql("SELECT SUM(amount) AS total, COUNT(*) AS cnt FROM sales")
    val output = PrismSQL.collectWithLineage(df)
    output.map(_._1) should equal (Array(Row(102609L, 16L)))

    val cursor = PrismSQL.trace(df, Seq(output(0)._2)).goBack()
    cursor.ids should have length 16
    cursor.show() should have length 16
  }

  test("DISTINCT control-flow witnesses across the exchange") {
    val s = newSession(adaptive = true)
    sales(s).createOrReplaceTempView("sales")
    val df = s.sql("SELECT DISTINCT category FROM sales")
    val output = PrismSQL.collectWithLineage(df)
    output.map(_._1.getString(0)).toSet should equal (
      Set("electronics", "furniture", "groceries", "toys"))

    // A distinct output depends on all duplicate witnesses. The scan is column-pruned
    // to `category`, so the four witnesses display identically but are distinct records.
    val electronicsId = output.find(_._1.getString(0) == "electronics").get._2
    val cursor = PrismSQL.trace(df, Seq(electronicsId)).goBack()
    cursor.ids should have length 4
    val witnesses = cursor.show()
    witnesses should have length 4
    witnesses.toSet should equal (Set(Row("electronics")))
  }

  test("filter + projection + expression pipeline under an aggregate") {
    val s = newSession(adaptive = true)
    sales(s).createOrReplaceTempView("sales")
    val df = s.sql(
      """SELECT UPPER(category) AS cat, SUM(amount + 1) AS total
        |FROM sales WHERE amount > 100 GROUP BY UPPER(category)""".stripMargin)
    val output = PrismSQL.collectWithLineage(df)
    // Rows with amount > 100 are electronics 420/310/99999/380, furniture
    // 250/190/205/220, and groceries 110. SUM(amount + 1) adds 1 per row.
    output.map(_._1).toSet should equal (Set(
      Row("ELECTRONICS", 101113L), Row("FURNITURE", 869L), Row("GROCERIES", 111L)))

    val furnitureId = output.find(_._1.getString(0) == "FURNITURE").get._2
    val cursor = PrismSQL.trace(df, Seq(furnitureId)).goBack()
    cursor.show().toSet should equal (Set(
      Row("furniture", 250), Row("furniture", 190), Row("furniture", 205),
      Row("furniture", 220)))
  }

  private def joinBothBranches(s: SparkSession): Unit = {
    orders(s).createOrReplaceTempView("orders")
    customers(s).createOrReplaceTempView("customers")
    val df = s.sql(
      """SELECT o.oid, o.amount, c.name
        |FROM orders o JOIN customers c ON o.cid = c.cid""".stripMargin)
    val output = PrismSQL.collectWithLineage(df)
    output should have length 12
    val suspect = output.maxBy(_._1.getInt(1))
    suspect._1 should equal (Row("o8", 99999, "Bob"))

    // Branch 0 is the orders side and yields all c2 order rows, at key-hash granularity.
    val ordersSide = PrismSQL.trace(df, Seq(suspect._2)).goBack(0)
    ordersSide.atScan should be (true)
    ordersSide.show().toSet should equal (Set(
      Row("o2", "c2", 250), Row("o5", "c2", 190),
      Row("o8", "c2", 99999), Row("o11", "c2", 205)))

    // Branch 1 is the customers side.
    val customersSide = PrismSQL.trace(df, Seq(suspect._2)).goBack(1)
    customersSide.show().toSet should equal (Set(Row("c2", "Bob")))
  }

  test("inner join (SMJ) backward into both sources (AQE on)") {
    joinBothBranches(newSession(adaptive = true))
  }

  test("inner join (SMJ) backward into both sources (AQE off)") {
    joinBothBranches(newSession(adaptive = false))
  }

  test("join then aggregate two-hop trace into both sources") {
    val s = newSession(adaptive = true)
    orders(s).createOrReplaceTempView("orders")
    customers(s).createOrReplaceTempView("customers")
    val df = s.sql(
      """SELECT c.name, SUM(o.amount) AS total
        |FROM orders o JOIN customers c ON o.cid = c.cid
        |GROUP BY c.name""".stripMargin)
    val output = PrismSQL.collectWithLineage(df)
    output.map(_._1).toSet should equal (Set(
      Row("Alice", 900L), Row("Bob", 100644L), Row("Carol", 630L)))

    val bobId = output.find(_._1.getString(0) == "Bob").get._2

    // The first hop goes from the aggregate level to the joined-row level, and the
    // second from the join to the orders source.
    var cursor = PrismSQL.trace(df, Seq(bobId)).goBack() // through the agg exchange
    cursor.atScan should be (false) // at the join's keyed level
    val ordersSide = cursor.goBack(0)
    // The orders scan is column-pruned to (cid, amount), since this query never reads oid.
    ordersSide.show().toSet should equal (Set(
      Row("c2", 250), Row("c2", 190), Row("c2", 99999), Row("c2", 205)))

    // The other branch of the second hop goes from the join to the customers source.
    val customersSide = cursor.goBack(1)
    customersSide.show().toSet should equal (Set(Row("c2", "Bob")))

    // Forward from the orders rows through the join level to the aggregate level.
    val up = ordersSide.goNext().goNext()
    up.ids.toSet should equal (PrismSQL.trace(df, Seq(bobId)).ids.toSet)
  }

  test("non-equi joins (BroadcastNestedLoopJoin) fail loudly") {
    val s = newSession(adaptive = true)
    sales(s).createOrReplaceTempView("sales")
    val df = s.sql(
      "SELECT * FROM sales a JOIN sales b ON a.amount > b.amount")
    val e = intercept[Exception] { df.collect() }
    def causes(t: Throwable): Seq[Throwable] =
      if (t == null) Nil else t +: causes(t.getCause)
    assert(causes(e).exists(_.isInstanceOf[PrismUnsupportedOperatorException]),
      s"expected PrismUnsupportedOperatorException, got: $e")
  }
}
