/*
 * Basic SQL lineage tests for extension injection, single-stage capture fused into
 * codegen, backward trace, and show() through a deterministic re-scan.
 */
package org.apache.spark.sql.lineage

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions.col
import org.apache.spark.util.PackIntIntoLong
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SQLLineageSuite extends AnyFunSuite with BeforeAndAfterEach with Matchers {

  @transient private var spark: SparkSession = _

  private val fruitsFile = "src/test/resources/fruits.txt"

  override def beforeEach(): Unit = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("prism-sql-test")
      .config("spark.sql.extensions", classOf[PrismSQLExtension].getName)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterEach(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
    System.clearProperty("spark.driver.port")
  }

  test("capture off makes the extension a strict no-op") {
    spark.conf.set("spark.prism.sql.capture", "false")
    val df = spark.read.text(fruitsFile).filter(col("value").contains("a"))
    df.collect().length should equal (6)
    assert(!df.queryExecution.executedPlan.exists(_.isInstanceOf[PrismTapExec]),
      "no tap operators may appear when capture is off")
  }

  test("unsupported operators fail loudly") {
    spark.conf.set("spark.prism.sql.capture", "true")
    // A global orderBy plans a RangePartitioning exchange, which is not supported.
    val df = spark.read.text(fruitsFile).orderBy(col("value"))
    val e = intercept[Exception] { df.collect() }
    def causes(t: Throwable): Seq[Throwable] =
      if (t == null) Nil else t +: causes(t.getCause)
    assert(causes(e).exists(_.isInstanceOf[PrismUnsupportedOperatorException]),
      s"expected PrismUnsupportedOperatorException, got: $e")
  }

  test("single-stage capture: taps are fused into whole-stage codegen") {
    spark.conf.set("spark.prism.sql.capture", "true")
    val df = spark.read.text(fruitsFile).filter(col("value").contains("a"))
    df.collect()

    val plan = df.queryExecution.executedPlan
    val fusedStage = plan.collectFirst {
      case w: WholeStageCodegenExec
        if w.exists(_.isInstanceOf[TapScanExec]) && w.exists(_.isInstanceOf[TapResultExec]) => w
    }
    assert(fusedStage.isDefined,
      s"taps must be inside WholeStageCodegen (no fallback); plan:\n$plan")
  }

  test("single-stage backward trace and show") {
    spark.conf.set("spark.prism.sql.capture", "true")
    // fruits.txt holds apple, banana, apple, cherry, banana, apple, cherry, date. The
    // filter drops the two cherry rows, so output indices shift against scan indices.
    val df = spark.read.text(fruitsFile).filter(col("value").contains("a"))
    val output = PrismSQL.collectWithLineage(df)

    output.map(_._1.getString(0)).sorted should equal (
      Array("apple", "apple", "apple", "banana", "banana", "date"))

    // The filter drops cherry rows before "date", so its scan row index is strictly
    // greater than its post-filter output index. This shows ids thread through the filter.
    val dateId = output.find(_._1.getString(0) == "date").get._2
    val dateInputs = PrismSQL.backward(df, Seq(dateId))
    dateInputs should have length 1
    assert(PackIntIntoLong.getRight(dateInputs(0)) > PackIntIntoLong.getRight(dateId),
      "scan row index must exceed post-filter output index for 'date'")

    PrismSQL.showInputs(df, dateInputs).map(_.getString(0)) should equal (Array("date"))

    // Tracing every output back and re-showing must reproduce exactly the filtered rows.
    val allInputs = PrismSQL.backward(df, output.map(_._2))
    allInputs should have length 6
    PrismSQL.showInputs(df, allInputs).map(_.getString(0)).sorted should equal (
      Array("apple", "apple", "apple", "banana", "banana", "date"))
  }

  test("interpreted path: capture works with whole-stage codegen disabled") {
    spark.conf.set("spark.prism.sql.capture", "true")
    spark.conf.set("spark.sql.codegen.wholeStage", "false")
    val df = spark.read.text(fruitsFile).filter(col("value").contains("an"))
    val output = PrismSQL.collectWithLineage(df)
    output.map(_._1.getString(0)).sorted should equal (Array("banana", "banana"))

    val inputs = PrismSQL.backward(df, output.map(_._2))
    PrismSQL.showInputs(df, inputs).map(_.getString(0)).sorted should equal (
      Array("banana", "banana"))
  }
}
