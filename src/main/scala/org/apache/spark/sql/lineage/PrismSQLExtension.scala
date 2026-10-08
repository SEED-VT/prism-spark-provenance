/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.lineage

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Attribute, Coalesce, Expression}
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, SinglePartition}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, ExpandExec, FileSourceScanExec, FilterExec, GenerateExec, ProjectExec, SortExec, SparkPlan, TakeOrderedAndProjectExec, PrismJoinKeys, UnionExec}
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, BroadcastQueryStageExec, ShuffleQueryStageExec, TableCacheQueryStageExec}
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec,
  ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, Exchange, ShuffleExchangeExec}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashedRelationBroadcastMode, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.catalyst.expressions.{GetStructField, PythonUDF}
import org.apache.spark.sql.execution.python.{ArrowEvalPythonExec, BatchEvalPythonExec, EvalPythonExec}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.execution.window.WindowExec

/**
 * Registers Prism SQL capture. Enable it with
 * `spark.sql.extensions=org.apache.spark.sql.lineage.PrismSQLExtension` and toggle
 * capture per query with `spark.prism.sql.capture=true`.
 *
 * Taps are inserted by a ColumnarRule's pre-transitions hook, which runs before
 * CollapseCodegenStages so the taps are fused into whole-stage codegen. Without AQE the
 * rule runs once over the whole plan. With AQE it runs per query stage, where map stages
 * are rooted at their ShuffleExchangeExec and the final stage at the result operator.
 */
class PrismSQLExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectColumnar(session => PrismColumnarRule(session))
  }
}

case class PrismColumnarRule(session: SparkSession) extends ColumnarRule {
  override def preColumnarTransitions: Rule[SparkPlan] = InsertPrismTaps(session)
}

class PrismUnsupportedOperatorException(plan: SparkPlan)
  extends UnsupportedOperationException(
    s"Lineage capture does not support the operator ${plan.getClass.getSimpleName}. " +
      s"Set spark.prism.sql.capture=false to run this query without provenance.\n" +
      s"Plan node: ${plan.simpleStringWithNodeId()}")

/**
 * Inserts the tap operators into a physical plan.
 *  - `TapScanExec` goes above every file scan.
 *  - `TapPreExchangeExec` goes on the map side of every shuffle. It sits below the partial
 *    aggregate when one exists, keyed by its grouping expressions, and otherwise directly
 *    below the exchange, keyed by its hash-partitioning expressions.
 *  - `TapPostKeyedExec` goes above every final aggregate and shuffle join, keyed by
 *    grouping or join keys evaluated on the operator output.
 *  - `TapResultExec` goes at the root of the final (non-exchange-rooted) stage.
 *
 * Any operator outside the supported set raises [[PrismUnsupportedOperatorException]].
 */
case class InsertPrismTaps(session: SparkSession) extends Rule[SparkPlan] with Logging {

  private def captureEnabled: Boolean =
    session.conf.get("spark.prism.sql.capture", "false").toBoolean

  // When set, the map-side tap below a partial aggregate also records each row's
  // contribution to the aggregates (influence).
  private def influenceEnabled: Boolean =
    session.conf.get("spark.prism.sql.influence", "false").toBoolean

  // `spark.prism.sql.focusExpr` is a SQL predicate naming the suspicious output,
  // usually on the group key. The group key is present on every pre-combine row, so the
  // map-side tap records lineage only for rows where the predicate holds. A predicate
  // that does not resolve against the pre-aggregate schema (for example, one on an
  // aggregate result) returns None, and capture covers all rows.
  private def focusPredicate(childOutput: Seq[Attribute]): Option[Expression] = {
    val raw = session.conf.get("spark.prism.sql.focusExpr", "")
    if (raw.isEmpty) return None
    try {
      val parsed = session.sessionState.sqlParser.parseExpression(raw)
      val resolved = parsed.transformUp {
        case u: UnresolvedAttribute =>
          childOutput.find(_.name.equalsIgnoreCase(u.nameParts.last))
            .getOrElse(throw new RuntimeException(
              s"focus predicate column ${u.name} is not available pre-aggregate"))
      }
      Some(resolved)
    } catch { case _: Throwable => None } // not resolvable on the map side
  }

  /**
   * The provcode expression of each prism UDF in a Python eval, which is the last
   * subfield of the UDF's struct output. A prism UDF has a `prism$` name prefix and
   * returns struct&lt;value, provcode&gt;.
   */
  private def prismProvExprs(py: SparkPlan): Seq[Expression] = {
    val ep = py.asInstanceOf[EvalPythonExec]
    ep.udfs.zip(ep.resultAttrs).collect {
      case (u: PythonUDF, attr) if u.name.startsWith("prism$") &&
        attr.dataType.isInstanceOf[StructType] =>
        val st = attr.dataType.asInstanceOf[StructType]
        GetStructField(attr, st.length - 1): Expression
    }
  }

  /**
   * The input expression of each aggregate, one slot per aggregate in the aggregate's
   * own order. When a query mixes distinct and non-distinct aggregates, Spark's second
   * stage merges the non-distinct buffers instead of reading raw rows. Those slots hold
   * null so the positions stay aligned.
   */
  private def aggInputExprs(agg: BaseAggregateExec): Seq[Expression] = {
    import org.apache.spark.sql.catalyst.expressions.Literal
    import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
    import org.apache.spark.sql.types.DoubleType
    agg.aggregateExpressions.collect {
      case ae: AggregateExpression if ae.aggregateFunction.children.nonEmpty =>
        val input = ae.aggregateFunction.children.head
        if (input.references.subsetOf(agg.child.outputSet)) input
        else Literal(null, DoubleType)
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!captureEnabled) return plan
    // DDL and commands (CREATE VIEW, SHOW, writes) are not data queries.
    if (isCommand(plan)) return plan
    // Display queries (df.show(), bare LIMIT n) select rows by position, which Prism
    // does not capture, so they skip capture instead of failing. ORDER BY ... LIMIT
    // selects rows by sort key and is captured below.
    if (isDisplayOnly(plan)) return plan
    // With AQE the outer preparation also passes the AdaptiveSparkPlanExec wrapper
    // through this rule. Taps are inserted on the per-stage invocations inside AQE.
    if (plan.exists(_.isInstanceOf[
      org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec])) return plan
    // AQE may re-prepare a subtree that already carries taps.
    if (plan.exists(_.isInstanceOf[PrismTapExec])) return plan

    validateSupported(plan)

    var tapped = plan.transformUp {
      // Input ids at the sources. For columnar scans (Parquet/ORC) the transition
      // inserter runs after this rule and places ColumnarToRowExec between the scan and
      // this row-based tap, so ids are assigned at the row boundary.
      case scan: FileSourceScanExec =>
        TapScanExec(scan)

      // A cached DataFrame is a source boundary. Trace stops at the cached rows and does
      // not cross into the plan that built the cache. Under AQE the cache arrives
      // wrapped in a TableCacheQueryStage leaf.
      case imts: InMemoryTableScanExec =>
        TapScanExec(imts)
      case tc: TableCacheQueryStageExec =>
        TapScanExec(tc)

      // Python UDF eval is one-to-one and order-preserving but batched, which breaks
      // currentInputId threading. The id sequence is recorded below and replayed above.
      // Above the reseq tap, each prism UDF's provcode (its struct's last field) is also
      // captured, keyed by row id.
      case py @ (_: BatchEvalPythonExec | _: ArrowEvalPythonExec)
        if !py.children.head.isInstanceOf[TapSeqExec] =>
        val pairId = session.sparkContext.newRddId()
        val reseq = TapReseqExec(pairId,
          py.withNewChildren(Seq(TapSeqExec(pairId, py.children.head))))
        val provExprs = prismProvExprs(py)
        if (provExprs.isEmpty) reseq
        else TapUdfProvExec(session.sparkContext.newRddId(), provExprs, reseq)

      // Map-side capture, below the partial aggregate when the exchange feeds one.
      case ex: ShuffleExchangeExec => ex.child match {
        case agg: BaseAggregateExec if !hasPreTap(agg) =>
          // A sort-based partial aggregate reads its input through a sort, which
          // buffers the partition. The tap goes under that sort, where rows still
          // arrive one at a time with their own ids.
          val (input, wrap) = agg.child match {
            case s: SortExec => (s.child, (p: SparkPlan) => s.withNewChildren(Seq(p)))
            case c => (c, (p: SparkPlan) => p)
          }
          if (!idSafeFromShuffle(input)) {
            throw new PrismUnsupportedOperatorException(ex)
          }
          val pre = TapPreExchangeExec(
            session.sparkContext.newRddId(), visibleGroupKeys(agg.groupingExpressions),
            input, if (influenceEnabled) aggInputExprs(agg) else Nil,
            focusPredicate(input.output))
          ex.withNewChildren(Seq(agg.withNewChildren(Seq(wrap(pre)))))
        case _: BaseAggregateExec => ex
        case _: TapPreExchangeExec => ex
        case other =>
          val keys = ex.outputPartitioning match {
            case h: HashPartitioning => h.expressions
            case SinglePartition => Seq.empty
            case p => throw new PrismUnsupportedOperatorException(ex)
          }
          if (!idSafeFromShuffle(other)) {
            throw new PrismUnsupportedOperatorException(ex)
          }
          ex.withNewChildren(
            Seq(TapPreExchangeExec(session.sparkContext.newRddId(), keys, other)))
      }

      // Build side of a broadcast join, keyed by the rewritten key hash.
      case bex: BroadcastExchangeExec
        if !bex.child.isInstanceOf[TapPreExchangeExec] => bex.mode match {
        case HashedRelationBroadcastMode(keys, _) =>
          if (!idSafeFromShuffle(bex.child)) {
            throw new PrismUnsupportedOperatorException(bex)
          }
          bex.withNewChildren(
            Seq(TapPreExchangeExec(session.sparkContext.newRddId(), keys, bex.child)))
        case _ => throw new PrismUnsupportedOperatorException(bex)
      }
    }

    // Reduce-side capture above final aggregates and shuffle joins.
    tapped = tapped.transformUp {
      // Output-side aggregates need a post tap. On the reduce side of a shuffle, the
      // matching pre tap is already on the map side. A fused pair (final directly over
      // partial with no shuffle, because the input is already partitioned on the
      // grouping keys) still breaks id threading, so it gets its own pre tap below.
      case agg: BaseAggregateExec
        if (isReduceSide(agg.child) || fusedPartialOf(agg).isDefined) &&
          !agg.isInstanceOf[TapPostKeyedExec] =>
        // Grouping values may appear in the output under an Alias, so each grouping
        // attribute resolves to the output attribute that carries its value. Grouping
        // attributes pruned from the output are appended to the result expressions for
        // the tap and projected away above, leaving the visible schema unchanged.
        val groupAttrs = visibleGroupKeys(agg.groupingExpressions).map(_.toAttribute)
        val resolved = groupAttrs.map { g =>
          agg.resultExpressions.collectFirst {
            case ar: org.apache.spark.sql.catalyst.expressions.AttributeReference
              if ar.exprId == g.exprId => ar: Expression
            case al @ org.apache.spark.sql.catalyst.expressions.Alias(
              ar: org.apache.spark.sql.catalyst.expressions.AttributeReference, _)
              if ar.exprId == g.exprId => al.toAttribute: Expression
          }
        }
        val keyAttrs = groupAttrs.zip(resolved).map { case (g, r) => r.getOrElse(g) }
        val missing = groupAttrs.zip(resolved).collect { case (g, None) => g }
        val body = fusedPartialOf(agg) match {
          case Some(partial) if !isReduceSide(agg.child) =>
            val keys = visibleGroupKeys(partial.groupingExpressions)
            partial.child match {
              case u: UnionExec =>
                // Fusion implies the union is partitioner-aligned at runtime (child
                // partitions zipped, not concatenated), so positional ids are ambiguous
                // across children. One pre tap per child, keyed on the grouping keys
                // rebased onto that child's output, keeps the branches apart.
                val tappedChildren = u.children.map { c =>
                  if (!idSafeFromShuffle(c)) {
                    throw new PrismUnsupportedOperatorException(agg)
                  }
                  val rebased = keys.map(_.transform {
                    case ar: org.apache.spark.sql.catalyst.expressions.AttributeReference =>
                      val j = u.output.indexWhere(_.exprId == ar.exprId)
                      if (j < 0) throw new PrismUnsupportedOperatorException(agg)
                      c.output(j)
                  })
                  TapPreExchangeExec(session.sparkContext.newRddId(), rebased, c)
                }
                agg.withNewChildren(Seq(partial.withNewChildren(
                  Seq(u.withNewChildren(tappedChildren)))))
              case other =>
                if (!idSafeFromShuffle(other)) {
                  throw new PrismUnsupportedOperatorException(agg)
                }
                val pre = TapPreExchangeExec(
                  session.sparkContext.newRddId(), keys, other)
                agg.withNewChildren(Seq(partial.withNewChildren(Seq(pre))))
            }
          case _ => agg
        }
        if (missing.isEmpty) {
          TapPostKeyedExec(session.sparkContext.newRddId(), keyAttrs, body)
        } else {
          val augmented = withResults(body, agg.resultExpressions ++ missing)
          ProjectExec(agg.output,
            TapPostKeyedExec(session.sparkContext.newRddId(), keyAttrs, augmented))
        }

      case j: SortMergeJoinExec =>
        TapPostKeyedExec(session.sparkContext.newRddId(),
          shuffleJoinKeys(j.joinType, j.leftKeys, j.rightKeys), j)

      case j: ShuffledHashJoinExec =>
        TapPostKeyedExec(session.sparkContext.newRddId(),
          shuffleJoinKeys(j.joinType, j.leftKeys, j.rightKeys), j)

      case j: BroadcastHashJoinExec =>
        // Streamed keys are rewritten like the broadcast mode's build keys, so the
        // output hash matches the build-side tap.
        val buildRight =
          j.buildSide == org.apache.spark.sql.catalyst.optimizer.BuildRight
        val streamedKeys = if (buildRight) j.leftKeys else j.rightKeys
        // Probe ids are recorded as exact row provenance, so they must not arrive from
        // an untapped shuffle. AQE's conversion of a sort-merge join to a broadcast join
        // can produce this shape.
        val streamedPlan = if (buildRight) j.left else j.right
        if (!idSafeFromShuffle(streamedPlan)) {
          throw new PrismUnsupportedOperatorException(j)
        }
        // The mark tap saves the probe id as the row enters the join, so every fan-out
        // match records the right probe row.
        val tapId = session.sparkContext.newRddId()
        val marked = TapProbeMarkExec(tapId, streamedPlan)
        val joined =
          if (buildRight) j.withNewChildren(Seq(marked, j.right))
          else j.withNewChildren(Seq(j.left, marked))
        TapPostBroadcastJoinExec(tapId, tapId,
          PrismJoinKeys.rewrite(streamedKeys), joined)

      case w: WindowExec if isReduceSide(w.child) =>
        // Keys outputs by the partition spec (peer-set granularity).
        TapPostKeyedExec(session.sparkContext.newRddId(), w.partitionSpec, w)

      case w: WindowExec =>
        // A window over already-partitioned input buffers per partition and breaks id
        // threading, so it gets its own pre tap.
        if (!idSafeFromShuffle(w.child)) {
          throw new PrismUnsupportedOperatorException(w)
        }
        val pre = TapPreExchangeExec(
          session.sparkContext.newRddId(), w.partitionSpec, w.child)
        TapPostKeyedExec(session.sparkContext.newRddId(), w.partitionSpec,
          w.withNewChildren(Seq(pre)))

      // ORDER BY ... LIMIT selects rows by sort key, so it is treated like a shuffle
      // boundary with a pre tap below and a post tap above, both keyed by the sort
      // columns. Sort keys pruned by the projection are carried through for the tap and
      // stripped above.
      case top: TakeOrderedAndProjectExec
        if !top.child.isInstanceOf[TapPreExchangeExec] =>
        val sortAttrs = topSortAttrs(top)
        if (!idSafeFromShuffle(top.child)) {
          throw new PrismUnsupportedOperatorException(top)
        }
        val resolved = sortAttrs.map { sa =>
          top.projectList.collectFirst {
            case a: org.apache.spark.sql.catalyst.expressions.AttributeReference
              if a.exprId == sa.exprId => a: Expression
            case al @ org.apache.spark.sql.catalyst.expressions.Alias(
              a: org.apache.spark.sql.catalyst.expressions.AttributeReference, _)
              if a.exprId == sa.exprId => al.toAttribute: Expression
          }
        }
        val outKeys = sortAttrs.zip(resolved).map { case (sa, r) => r.getOrElse(sa) }
        val missing = sortAttrs.zip(resolved).collect { case (sa, None) => sa }
        val pre = TapPreExchangeExec(
          session.sparkContext.newRddId(), sortAttrs, top.child)
        val augmented = top.copy(projectList = top.projectList ++ missing)
          .withNewChildren(Seq(pre))
        val tapped =
          TapPostKeyedExec(session.sparkContext.newRddId(), outKeys, augmented)
        if (missing.isEmpty) tapped else ProjectExec(top.output, tapped)
    }

    // The result tap goes only on the final (non-exchange-rooted) stage, exactly once.
    tapped match {
      case _: Exchange => tapped
      case _ if tapped.isInstanceOf[TapResultExec] => tapped
      case _ if !tapped.exists(_.isInstanceOf[TapScanExec]) &&
                !tapped.exists(_.isInstanceOf[TapPostKeyedExec]) => tapped
      case root =>
        if (!idSafeFromShuffle(root)) {
          throw new PrismUnsupportedOperatorException(root)
        }
        val tapId = session.sparkContext.newRddId()
        logInfo(s"Prism SQL capture: result tap $tapId inserted")
        TapResultExec(tapId, root)
    }
  }

  private def isDisplayOnly(p: SparkPlan): Boolean = p.exists { node =>
    node.isInstanceOf[org.apache.spark.sql.execution.CollectLimitExec] ||
      node.getClass.getSimpleName.startsWith("GlobalLimit") ||
      node.getClass.getSimpleName.startsWith("LocalLimit")
  }

  /**
   * The sort keys of an ORDER BY ... LIMIT as attributes of its child. Inline
   * computed sort expressions (Catalyst normally pre-computes them below) are
   * unsupported.
   */
  private def topSortAttrs(top: TakeOrderedAndProjectExec): Seq[
      org.apache.spark.sql.catalyst.expressions.AttributeReference] =
    top.sortOrder.map(_.child).map {
      case ar: org.apache.spark.sql.catalyst.expressions.AttributeReference => ar
      case _ => throw new PrismUnsupportedOperatorException(top)
    }

  /**
   * True when rows reaching `p` carry valid ids. A tap must not consume rows that
   * crossed a shuffle without a keyed operator in between, whose post tap re-bases ids.
   * Otherwise the ids are stale values from unrelated rows.
   */
  private def idSafeFromShuffle(p: SparkPlan): Boolean = p match {
    case _: ShuffleQueryStageExec | _: ShuffleExchangeExec | _: AQEShuffleReadExec =>
      false
    // A partitioner-aligned union zips child partitions into shared tasks, making
    // positional ids ambiguous across children. Only the fused-aggregate path, which
    // taps each child, may record above it.
    case u: UnionExec if isAlignedUnion(u) => false
    // Keyed operators get a post tap that re-bases ids on their outputs.
    case _: BaseAggregateExec | _: SortMergeJoinExec | _: ShuffledHashJoinExec |
         _: BroadcastHashJoinExec | _: WindowExec |
         _: TakeOrderedAndProjectExec => true
    case _: TapPostKeyedExec | _: TapPostBroadcastJoinExec => true
    case leaf if leaf.children.isEmpty => true
    case other => other.children.forall(idSafeFromShuffle)
  }

  /**
   * Approximates when `SparkContext.union` zips partitions instead of concatenating
   * them, which happens when every child is hash-partitioned.
   */
  private def isAlignedUnion(u: UnionExec): Boolean =
    u.children.forall { c =>
      c.outputPartitioning match {
        case _: HashPartitioning => true
        case p => p.getClass.getSimpleName.contains("HashPartitioning")
      }
    }

  private def isCommand(p: SparkPlan): Boolean = p.exists { node =>
    val name = node.getClass.getSimpleName
    name.contains("Command") || name.contains("LocalTableScan")
  }

  /**
   * Grouping keys minus the synthetic grouping id that Expand adds for ROLLUP, CUBE, and
   * GROUPING SETS. The id rarely survives into the aggregate output, so both taps key on
   * the visible subset. This may merge grouping sets whose visible keys coincide.
   */
  private def visibleGroupKeys(groupingExpressions: Seq[
      org.apache.spark.sql.catalyst.expressions.NamedExpression]): Seq[
      org.apache.spark.sql.catalyst.expressions.NamedExpression] =
    groupingExpressions.filterNot(_.name == "spark_grouping_id")

  private val supportedJoinTypes: Set[JoinType] =
    Set(Inner, LeftOuter, RightOuter, FullOuter, LeftSemi, LeftAnti)

  /**
   * Key expressions to hash on a shuffle join's output. Matched rows can use either
   * side. Full outer rows use Coalesce so the present side wins.
   */
  private def shuffleJoinKeys(
      joinType: JoinType,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression]): Seq[Expression] = joinType match {
    case Inner | LeftOuter | LeftSemi | LeftAnti => leftKeys
    case RightOuter => rightKeys
    case FullOuter =>
      leftKeys.zip(rightKeys).map { case (l, r) => Coalesce(Seq(l, r)) }
    case _ => leftKeys
  }

  /** True when the map-side tap is already in place under `agg` (or under its sort). */
  private def hasPreTap(agg: BaseAggregateExec): Boolean = agg.child match {
    case _: TapPreExchangeExec => true
    case s: SortExec => s.child.isInstanceOf[TapPreExchangeExec]
    case _ => false
  }

  /** `agg` with its result expressions replaced, for each aggregate operator. */
  private def withResults(
      agg: SparkPlan,
      results: Seq[org.apache.spark.sql.catalyst.expressions.NamedExpression]): SparkPlan =
    agg match {
      case a: HashAggregateExec => a.copy(resultExpressions = results)
      case a: ObjectHashAggregateExec => a.copy(resultExpressions = results)
      case a: SortAggregateExec => a.copy(resultExpressions = results)
    }

  /**
   * The partial aggregate fused directly under a final one, with no shuffle between
   * because the input is already partitioned on the grouping keys.
   */
  private def fusedPartialOf(agg: BaseAggregateExec): Option[BaseAggregateExec] =
    agg.child match {
      case p: BaseAggregateExec
        if p.groupingExpressions.map(_.toAttribute.exprId) ==
           agg.groupingExpressions.map(_.toAttribute.exprId) => Some(p)
      case _ => None
    }

  /** True when `p` reads from an exchange, as final aggregate and join inputs do. */
  private def isReduceSide(p: SparkPlan): Boolean = p match {
    case _: ShuffleQueryStageExec | _: ShuffleExchangeExec | _: AQEShuffleReadExec => true
    case _: SortExec | _: ProjectExec | _: FilterExec | _: TapPostKeyedExec =>
      p.children.exists(isReduceSide)
    case _ => false
  }

  /**
   * Checks every node against the supported operator set. QueryStageExec nodes are
   * leaves whose inner plans were validated when their stage was created.
   */
  private def validateSupported(plan: SparkPlan): Unit = {
    // The sort under a map-side sort aggregate is allowed because the tap goes below it.
    val mapSideSorts = plan.collect {
      case a: SortAggregateExec if !isReduceSide(a.child) => a.child
    }.collect { case s: SortExec => s }
    plan.foreach {
      case _: PrismTapExec => // ours
      case _: FileSourceScanExec => // row-based or columnar
      case _: InMemoryTableScanExec | _: TableCacheQueryStageExec => // cache as a source
      case _: BatchEvalPythonExec | _: ArrowEvalPythonExec => // Python UDF eval
      case _: FilterExec | _: ProjectExec => // pass-through
      case _: ExpandExec | _: GenerateExec => // One input yields many rows, consumed
        // depth-first per input row, so currentInputId threading stays exact.
      case _: HashAggregateExec | _: ObjectHashAggregateExec | _: SortAggregateExec =>
        // Partial or final. Object-hash and sort aggregates carry holistic functions
        // such as percentile and collect_list.
      case _: WindowExec => // peer-set granularity via partition-spec keying
      case _: UnionExec => // Each task runs one child's partition, so id threading holds.
      case top: TakeOrderedAndProjectExec =>
        topSortAttrs(top) // throws if a sort key is computed inline
      case _: org.apache.spark.sql.execution.EmptyRelationExec => // AQE's zero-row leaf
      case ex: ShuffleExchangeExec => ex.outputPartitioning match {
        case _: HashPartitioning | SinglePartition => // supported
        case _ => throw new PrismUnsupportedOperatorException(ex)
      }
      case bex: BroadcastExchangeExec => bex.mode match {
        case _: HashedRelationBroadcastMode => // supported
        case _ => throw new PrismUnsupportedOperatorException(bex)
      }
      case _: ShuffleQueryStageExec | _: AQEShuffleReadExec |
           _: BroadcastQueryStageExec => // stage reads, validated at their creation
      case s: SortExec =>
        // Allowed only as a shuffle-join or window input. A sort between a tap and the
        // result would break currentInputId threading.
        if (!isReduceSide(s.child) && !mapSideSorts.exists(_ eq s)) {
          throw new PrismUnsupportedOperatorException(s)
        }
      case j: SortMergeJoinExec =>
        if (!supportedJoinTypes.contains(j.joinType)) {
          throw new PrismUnsupportedOperatorException(j)
        }
      case j: ShuffledHashJoinExec =>
        if (!supportedJoinTypes.contains(j.joinType)) {
          throw new PrismUnsupportedOperatorException(j)
        }
      case j: BroadcastHashJoinExec =>
        if (!supportedJoinTypes.contains(j.joinType) || j.joinType == FullOuter) {
          throw new PrismUnsupportedOperatorException(j)
        }
      case other => throw new PrismUnsupportedOperatorException(other)
    }
  }
}
