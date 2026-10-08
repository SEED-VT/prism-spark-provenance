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

import scala.reflect.ClassTag

import org.apache.spark.{Partition, SparkContext, SparkEnv, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.storage.{BlockId, RDDBlockId}
import org.apache.spark.util.PackIntIntoLong

/**
 * Driver-side trace model for SQL lineage.
 *
 * The executed plan is parsed into a tree of capture levels. The result tap sits above
 * zero or more keyed levels (post taps above aggregates and joins). Each keyed level has
 * branches that lead through a pre-exchange tap to an upstream level. The leaves are
 * scan levels, resolved by deterministic re-scan.
 *
 * Traversal mirrors the RDD LineageRDD API. `goBack(path)` follows join input `path`, and
 * `show()` resolves source rows at a scan level.
 *
 * @groupname capture Capture
 * @groupdesc capture Toggle lineage capture for a session.
 * @groupprio capture 0
 * @groupname api Trace API
 * @groupdesc api Collect results with lineage ids and walk them back to source rows.
 * @groupprio api 1
 * @groupname lifecycle Footprint & lifecycle
 * @groupdesc lifecycle Inspect and release a query's materialized lineage blocks.
 * @groupprio lifecycle 2
 */
object PrismSQL {

  /**
   * Turn lineage capture on for `spark`, the same as setting
   * `spark.prism.sql.capture=true`.
   * @group capture
   */
  def enableCapture(spark: SparkSession): Unit =
    spark.conf.set("spark.prism.sql.capture", "true")
  /**
   * Turn lineage capture off for `spark`.
   * @group capture
   */
  def disableCapture(spark: SparkSession): Unit =
    spark.conf.set("spark.prism.sql.capture", "false")

  // Plan parsing.

  sealed trait SourceInfo
  /**
   * Terminal level, a file scan or a cached relation (InMemoryTableScan).
   * Inside a union's stage, recorded partition ids are stage-wide (scan partition plus
   * `partOffset`), and the re-scan translates them back.
   */
  case class ScanSource(scan: SparkPlan, partOffset: Int = 0) extends SourceInfo
  /**
   * A keyed capture level, the post tap above an aggregate, join, window, or
   * order-by-limit. When `hasProbe` is set (broadcast joins), each output row stores its
   * key hash and exact probe id, and the probe branch is a [[DirectBranch]].
   */
  case class KeyedSource(tapId: Int, hasProbe: Boolean, branches: Seq[BranchLink])
    extends SourceInfo
  /**
   * UNION ALL level, which has no tap of its own. Each output row keeps the id its child
   * pipeline assigned. Child i owns stage partitions [starts(i), starts(i) + counts(i)),
   * and goBack(i) filters ids to that range.
   */
  case class UnionSource(children: Seq[SourceInfo], starts: Seq[Int], counts: Seq[Int])
    extends SourceInfo

  sealed trait BranchLink { def upstream: SourceInfo }
  /** Branch across a shuffle or broadcast, resolved via its pre-exchange tap. */
  case class HashBranch(preTapId: Int, upstream: SourceInfo) extends BranchLink
  /** Same-pipeline probe side of a broadcast join, where ids are exact. */
  case class DirectBranch(upstream: SourceInfo) extends BranchLink

  case class CaptureGraph(spark: SparkSession, resultTapId: Int, source: SourceInfo)

  private def finalPlan(df: DataFrame): SparkPlan = unwrap(df.queryExecution.executedPlan)

  /** Strips the AQE wrappers AdaptiveSparkPlanExec and ResultQueryStageExec. */
  private def unwrap(p: SparkPlan): SparkPlan = p match {
    case a: AdaptiveSparkPlanExec => unwrap(a.executedPlan)
    case other =>
      other.getClass.getSimpleName match {
        case "ResultQueryStageExec" =>
          // Spark wraps the final stage in this class. Its inner plan is read
          // reflectively so the code tolerates the field moving.
          val m = other.getClass.getMethod("plan")
          unwrap(m.invoke(other).asInstanceOf[SparkPlan])
        case _ => other
      }
  }

  def captureGraph(df: DataFrame): CaptureGraph = {
    val plan = finalPlan(df)
    val result = plan.collectFirst { case t: TapResultExec => t }.getOrElse {
      throw new IllegalStateException(
        "The executed plan has no result tap. Run the query with " +
          "spark.prism.sql.capture=true and the PrismSQLExtension registered.")
    }
    CaptureGraph(df.sparkSession, result.tapId, parseSource(result.child))
  }

  private def parseSource(plan: SparkPlan, partOffset: Int = 0): SourceInfo = plan match {
    case t: TapScanExec =>
      // CollapseCodegenStages may wrap the leaf in an InputAdapter or ColumnarToRow.
      ScanSource(t.child.collectFirst {
        case s: FileSourceScanExec => s: SparkPlan
        case m: org.apache.spark.sql.execution.columnar.InMemoryTableScanExec => m: SparkPlan
        case c: org.apache.spark.sql.execution.adaptive.TableCacheQueryStageExec => c: SparkPlan
      }.getOrElse(
        throw new IllegalStateException(s"No source leaf under scan tap:\n$t")), partOffset)
    case t: TapPostKeyedExec => topBelow(t.child) match {
      // ORDER BY ... LIMIT has a single branch through the tap pair around it. Its
      // internal shuffle is not in the tree, so directShuffles cannot find it.
      case Some(top) =>
        val pre = firstPreTap(top.child).getOrElse(
          throw new IllegalStateException(s"No pre tap under order-by-limit:\n$top"))
        KeyedSource(t.tapId, hasProbe = false,
          Seq(HashBranch(pre.tapId, parseSource(pre.child))))
      case None =>
        // Same-pipeline pre taps (fused aggregate or window) are keyed on exactly this
        // level's keys, so they take precedence over deeper shuffles, whose partitioning
        // keys may be a subset and hash differently. A fused aggregate over an aligned
        // union has one pre tap per union child, each a branch in child order.
        val pres = directPreTaps(t.child)
        val branches =
          if (pres.nonEmpty) {
            pres.map(pre => HashBranch(pre.tapId, parseSource(pre.child, partOffset)))
          } else {
            directShuffles(t.child).map(parseBranch)
          }
        KeyedSource(t.tapId, hasProbe = false, branches)
    }
    case t: TapPostBroadcastJoinExec =>
      val bhj = t.child.asInstanceOf[
        org.apache.spark.sql.execution.joins.BroadcastHashJoinExec]
      val buildOnLeft =
        bhj.buildSide == org.apache.spark.sql.catalyst.optimizer.BuildLeft
      val (buildPlan, probePlan) =
        if (buildOnLeft) (bhj.left, bhj.right) else (bhj.right, bhj.left)
      val buildBranch = parseBroadcastBranch(buildPlan)
      // The probe side shares this pipeline's tasks, so any union offset carries over.
      val probeBranch = DirectBranch(parseSource(probePlan, partOffset))
      // Branch indices follow logical join inputs, 0 for left and 1 for right.
      val branches =
        if (buildOnLeft) Seq(buildBranch, probeBranch) else Seq(probeBranch, buildBranch)
      KeyedSource(t.tapId, hasProbe = true, branches)
    case e: org.apache.spark.sql.execution.EmptyRelationExec =>
      // AQE replaced an empty subtree with a zero-row source. No ids reach it and a
      // re-scan yields nothing.
      ScanSource(e, partOffset)
    case u: org.apache.spark.sql.execution.UnionExec =>
      // Rows keep their child-pipeline ids. Child i's rows live in stage partitions
      // offset by the partition counts of the children before it.
      val counts = u.children.map(_.execute().getNumPartitions)
      val starts = counts.scanLeft(partOffset)(_ + _).init
      UnionSource(
        u.children.zip(starts).map { case (c, o) => parseSource(c, o) }, starts, counts)
    case u: UnaryExecNode => parseSource(u.child, partOffset)
    case other =>
      throw new IllegalStateException(
        s"Cannot parse capture graph at ${other.getClass.getSimpleName}:\n$other")
  }

  private def parseBroadcastBranch(p: SparkPlan): HashBranch = {
    val exchangeChild = p.collectFirst {
      case b: org.apache.spark.sql.execution.exchange.BroadcastExchangeExec => b.child
    }.orElse {
      // AQE wraps the broadcast in a leaf BroadcastQueryStageExec.
      p.collectFirst {
        case s: org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec =>
          s.plan match {
            case b: org.apache.spark.sql.execution.exchange.BroadcastExchangeExec => b.child
            case inner => inner
          }
      }
    }.getOrElse(throw new IllegalStateException(s"No broadcast exchange under:\n$p"))
    val pre = firstPreTap(exchangeChild).getOrElse {
      throw new IllegalStateException(s"No pre tap under broadcast:\n$exchangeChild")
    }
    HashBranch(pre.tapId, parseSource(pre.child))
  }

  /** Shuffle boundaries directly feeding this operator (not nested deeper ones). */
  private def directShuffles(p: SparkPlan): Seq[SparkPlan] = p match {
    case s: ShuffleQueryStageExec => Seq(s)
    case e: ShuffleExchangeExec => Seq(e)
    case other => other.children.flatMap(directShuffles)
  }

  private def parseBranch(shuffle: SparkPlan): HashBranch = {
    val mapSide = shuffle match {
      case s: ShuffleQueryStageExec => s.plan match {
        case e: ShuffleExchangeExec => e.child
        case p => p
      }
      case e: ShuffleExchangeExec => e.child
      case p => p
    }
    val pre = firstPreTap(mapSide).getOrElse {
      throw new IllegalStateException(s"No pre-exchange tap under shuffle:\n$mapSide")
    }
    HashBranch(pre.tapId, parseSource(pre.child))
  }

  /**
   * The TakeOrderedAndProject a post tap was inserted directly above, looking through
   * the InputAdapter that CollapseCodegenStages places at the codegen boundary.
   */
  private def topBelow(p: SparkPlan): Option[
      org.apache.spark.sql.execution.TakeOrderedAndProjectExec] = p match {
    case top: org.apache.spark.sql.execution.TakeOrderedAndProjectExec => Some(top)
    case ia: org.apache.spark.sql.execution.InputAdapter => topBelow(ia.child)
    case _ => None
  }

  /**
   * Same-pipeline pre taps directly below a keyed level, stopping at shuffle boundaries
   * and at keyed taps of deeper levels. They are returned in child order, so a fused
   * aggregate's per-union-child taps index its branches.
   */
  private def directPreTaps(p: SparkPlan): Seq[TapPreExchangeExec] = p match {
    case t: TapPreExchangeExec => Seq(t)
    case _: ShuffleQueryStageExec | _: ShuffleExchangeExec => Nil
    case _: TapPostKeyedExec | _: TapPostBroadcastJoinExec => Nil
    case other => other.children.flatMap(directPreTaps)
  }

  private def firstPreTap(p: SparkPlan): Option[TapPreExchangeExec] = p match {
    case t: TapPreExchangeExec => Some(t)
    case _: ShuffleQueryStageExec | _: ShuffleExchangeExec => None
    case other => other.children.flatMap(firstPreTap).headOption
  }

  // Block access.

  /**
   * Lineage blocks of a tap as an RDD, one [[TapBlock]] per partition, read where the
   * block lives. Trace steps run filtering closures over it so only matching ids reach
   * the driver. The driverTrace ablation drops the locality hints.
   */
  private[lineage] def tapBlocks[T <: TapBlock: ClassTag](
      spark: SparkSession, tapId: Int): RDD[T] = {
    val sc = spark.sparkContext
    val master = sc.env.blockManager.master
    val parts = master.getMatchingBlockIds({
      case RDDBlockId(id, _) => id == tapId
      case _ => false
    }, askStorageEndpoints = true).collect {
      case RDDBlockId(_, split) => split
    }.distinct.sorted
    val locations =
      if (org.apache.spark.lineage.PrismAblation.driverTrace) Nil
      else master
        .getLocations(parts.map(p => RDDBlockId(tapId, p): BlockId).toArray)
        .map(_.map(bm => ExecutorCacheTaskLocation(bm.host, bm.executorId).toString))
    new TapBlockRDD[T](sc, tapId, parts, locations)
  }

  /**
   * Runs a filtering function over a tap's blocks on the executors and returns only the
   * matches. The driverTrace ablation collects whole blocks and filters on the driver.
   */
  private[lineage] def mapBlocks[R: ClassTag](spark: SparkSession, tapId: Int)(
      f: TapBlock => Iterator[R]): Array[R] = {
    val rdd = tapBlocks[TapBlock](spark, tapId)
    if (org.apache.spark.lineage.PrismAblation.driverTrace) {
      rdd.collect().iterator.flatMap(f).toArray
    } else {
      rdd.flatMap(f).collect()
    }
  }

  /**
   * Bounded variant of `mapBlocks` that returns at most `limit` matches. Partitions are
   * evaluated incrementally, so an oversized frontier stops early. When the total is at
   * most `limit`, every partition was scanned and the result is complete.
   */
  private[lineage] def mapBlocksTake[R: ClassTag](spark: SparkSession, tapId: Int, limit: Int)(
      f: TapBlock => Iterator[R]): Array[R] =
    tapBlocks[TapBlock](spark, tapId).flatMap(f).take(limit)

  /** All (packedOutputId, value) pairs of a result tap, sorted by output id. */
  private def resultAssociations(spark: SparkSession, tapId: Int): Array[(Long, Int)] =
    mapBlocks(spark, tapId) { b =>
      val (split, values) = TapBlock.keyedValues(b)
      values.indices.iterator.map(i => (PackIntIntoLong(split, i), values(i)))
    }.sortBy(_._1)

  // Public API.

  /**
   * Collect result rows paired with their packed output lineage ids.
   * @group api
   */
  def collectWithLineage(df: DataFrame): Array[(Row, Long)] = {
    val rows = df.collect() // triggers execution and capture
    // A query that AQE collapsed to an empty relation has no taps and no rows.
    if (rows.isEmpty && !finalPlan(df).exists(_.isInstanceOf[TapResultExec])) {
      return Array.empty
    }
    val graph = captureGraph(df)
    val associations = resultAssociations(graph.spark, graph.resultTapId)
    require(rows.length == associations.length,
      s"capture mismatch: ${rows.length} rows vs ${associations.length} lineage records")
    rows.zip(associations.map(_._1))
  }

  /**
   * Start a trace from packed result-row ids returned by [[collectWithLineage]].
   * @group api
   */
  def trace(df: DataFrame, outputIds: Seq[Long]): TraceCursor = {
    val graph = captureGraph(df)
    val wanted = outputIds.toSet
    val locals = mapBlocks(graph.spark, graph.resultTapId) { b =>
      val (split, values) = TapBlock.keyedValues(b)
      values.indices.iterator.collect {
        case i if wanted.contains(PackIntIntoLong(split, i)) =>
          PackIntIntoLong(split, values(i))
      }
    }
    new TraceCursor(graph.spark, graph.source, locals, Nil)
  }

  /**
   * Walk backward along join input 0 down to the scan and return its input ids.
   * @group api
   */
  def backward(df: DataFrame, outputIds: Seq[Long]): Array[Long] = {
    var cursor = trace(df, outputIds)
    while (!cursor.atScan) { cursor = cursor.goBack(0) }
    cursor.ids
  }

  /**
   * Resolve input ids to source rows for a plan with a single scan.
   * @group api
   */
  def showInputs(df: DataFrame, inputIds: Seq[Long]): Array[Row] = {
    val graph = captureGraph(df)
    def findScan(s: SourceInfo): ScanSource = s match {
      case sc: ScanSource => sc
      case KeyedSource(_, _, branches) => findScan(branches.head.upstream)
      case UnionSource(children, _, _) => findScan(children.head)
    }
    val scan = findScan(graph.source)
    showScanRows(graph.spark, scan.scan, inputIds, full = false, scan.partOffset)
  }

  private[lineage] def showScanRows(
      spark: SparkSession,
      scan: SparkPlan,
      inputIds: Seq[Long],
      full: Boolean = false,
      partOffset: Int = 0): Array[Row] =
    showScanRowsWithSchema(spark, scan, inputIds, full, partOffset)._1

  /**
   * Source rows of `scan` keyed by their (split, row offset) id, kept when `select`
   * accepts the id. When `splits` is given, only those scan partitions are read. Pruning
   * happens after indexing, so each partition keeps its original index and capture ids.
   */
  private def scanById(
      scan: SparkPlan, partOffset: Int, splits: Option[Set[Int]],
      select: Long => Boolean): RDD[(Long, org.apache.spark.sql.catalyst.InternalRow)] = {
    import scala.jdk.CollectionConverters._
    val offset = partOffset
    val indexed: RDD[(Long, org.apache.spark.sql.catalyst.InternalRow)] =
      if (scan.supportsColumnar) {
        scan.executeColumnar().mapPartitionsWithIndexInternal { (p, batches) =>
          val split = p + offset
          var idx = -1
          batches.flatMap { batch =>
            batch.rowIterator().asScala.flatMap { row =>
              idx += 1
              val id = PackIntIntoLong(split, idx)
              if (select(id)) Iterator.single((id, row.copy())) else Iterator.empty
            }
          }
        }
      } else {
        scan.execute().mapPartitionsWithIndexInternal { (p, iter) =>
          val split = p + offset
          var idx = -1
          iter.flatMap { row =>
            idx += 1
            val id = PackIntIntoLong(split, idx)
            if (select(id)) Iterator.single((id, row.copy())) else Iterator.empty
          }
        }
      }
    splits match {
      case Some(keep) => org.apache.spark.rdd.PartitionPruningRDD.create(indexed, keep.contains)
      case None => indexed
    }
  }

  /**
   * Scan-local partitions that hold `inputIds`, or None when pruning is switched off
   * with `spark.prism.trace.prunePartitions=false`.
   */
  private def splitsOf(spark: SparkSession, inputIds: Seq[Long], partOffset: Int)
      : Option[Set[Int]] =
    if (!spark.conf.get("spark.prism.trace.prunePartitions", "true").toBoolean) None
    else Some(inputIds.iterator.map(id => PackIntIntoLong.getLeft(id) - partOffset).toSet)

  private[lineage] def showScanRowsWithSchema(
      spark: SparkSession,
      scan: SparkPlan,
      inputIds: Seq[Long],
      full: Boolean,
      partOffset: Int = 0): (Array[Row], org.apache.spark.sql.types.StructType) = {
    // In full mode, re-execute the same scan (same FilePartitions and row order) with
    // the complete data schema instead of the pruned one. This is safe only when no
    // pushed-down filter or partition schema could change the emitted rows, so other
    // scans keep the pruned view.
    val effective = scan match {
      case fs: FileSourceScanExec
        if full && fs.dataFilters.isEmpty &&
           fs.relation.partitionSchema.isEmpty &&
           fs.requiredSchema != fs.relation.dataSchema =>
        val fullAttrs = fs.relation.dataSchema.fields.toSeq.map { f =>
          org.apache.spark.sql.catalyst.expressions.AttributeReference(
            f.name, f.dataType, f.nullable)()
        }
        fs.copy(output = fullAttrs, requiredSchema = fs.relation.dataSchema)
      case other => other
    }
    val wanted = inputIds.toSet
    val matched = scanById(effective, partOffset, splitsOf(spark, inputIds, partOffset),
      wanted.contains).map(_._2).collect()
    val converter =
      org.apache.spark.sql.catalyst.CatalystTypeConverters
        .createToScalaConverter(effective.schema)
    (matched.map(r => converter(r).asInstanceOf[Row]), effective.schema)
  }

  /**
   * Writes the matched witness rows of `scan` (pruned view) to `path` as text, without
   * collecting them to the driver. It uses the same id filter as `showScanRowsWithSchema`.
   */
  private[lineage] def writeScanRows(
      spark: SparkSession, scan: SparkPlan, inputIds: Seq[Long],
      partOffset: Int, path: String): Unit = {
    val wanted = inputIds.toSet
    val matched = scanById(scan, partOffset, splitsOf(spark, inputIds, partOffset),
      wanted.contains).map(_._2)
    val schema = scan.schema
    matched.mapPartitions { it =>
      val conv = org.apache.spark.sql.catalyst.CatalystTypeConverters
        .createToScalaConverter(schema)
      it.map(r => conv(r).asInstanceOf[Row].mkString("|"))
    }.saveAsTextFile(path)
  }

  // Distributed backward trace. The driver-side cursor holds the witness frontier on the
  // driver at every hop, which limits its size. These helpers expose each tap's
  // associations as a keyed RDD, so a hop becomes a shuffle join on the cluster.

  /** Pairs of packed output id and key hash for every row of a keyed level tap. */
  private[lineage] def levelKeyHashRDD(
      spark: SparkSession, tapId: Int, hasProbe: Boolean): RDD[(Long, Int)] =
    if (hasProbe)
      tapBlocks[TapBlock](spark, tapId).flatMap { b =>
        val (split, pairs) = TapBlock.joinPairs(b)
        pairs.indices.iterator.map(i =>
          (PackIntIntoLong(split, i), PackIntIntoLong.getLeft(pairs(i))))
      }
    else
      tapBlocks[TapBlock](spark, tapId).flatMap { b =>
        val (split, values) = TapBlock.keyedValues(b)
        values.indices.iterator.map(i => (PackIntIntoLong(split, i), values(i)))
      }

  /** Pairs of key hash and packed upstream input id for every pre-exchange association. */
  private[lineage] def preHashIdRDD(spark: SparkSession, preTapId: Int): RDD[(Int, Long)] =
    tapBlocks[TapBlock](spark, preTapId).flatMap { b =>
      val (split, blockHashes, idLists) = TapBlock.preEntries(b)
      blockHashes.indices.iterator.flatMap { j =>
        idLists(j).iterator.map(id => (blockHashes(j), PackIntIntoLong(split, id)))
      }
    }

  /** Pairs of packed output id and packed probe input id for a direct branch. */
  private[lineage] def directPairsRDD(spark: SparkSession, tapId: Int): RDD[(Long, Long)] =
    tapBlocks[TapBlock](spark, tapId).flatMap { b =>
      val (split, pairs) = TapBlock.joinPairs(b)
      pairs.indices.iterator.map(i =>
        (PackIntIntoLong(split, i),
         PackIntIntoLong(split, PackIntIntoLong.getRight(pairs(i)))))
    }

  /**
   * Resolves source witnesses by joining the distributed frontier against the
   * re-executed scan and writes the matched rows to `path`. Returns the witness count.
   */
  private[lineage] def writeScanRowsJoin(
      spark: SparkSession, scan: SparkPlan, frontier: RDD[Long],
      partOffset: Int, path: String, numParts: Int): Long = {
    // A distributed trace usually touches nearly every split, so the scan is not pruned.
    val idRows = scanById(scan, partOffset, None, _ => true)
    val schema = scan.schema
    val count = spark.sparkContext.longAccumulator("prismWitnessCount")
    idRows.join(frontier.map(id => (id, ())), numParts)
      .mapPartitions { it =>
        val conv = org.apache.spark.sql.catalyst.CatalystTypeConverters
          .createToScalaConverter(schema)
        it.map { case (_, (row, _)) =>
          count.add(1L); conv(row).asInstanceOf[Row].mkString("|")
        }
      }.saveAsTextFile(path)
    count.value
  }

  // Lifecycle and py4j API.

  private def allTapIds(graph: CaptureGraph): Seq[Int] = {
    def tapIds(s: SourceInfo): Seq[Int] = s match {
      case _: ScanSource => Nil
      case UnionSource(children, _, _) => children.flatMap(tapIds)
      case KeyedSource(id, _, branches) => id +: branches.flatMap {
        case HashBranch(pre, up) => pre +: tapIds(up)
        case DirectBranch(up) => tapIds(up)
      }
    }
    graph.resultTapId +: tapIds(graph.source)
  }

  private def allBlockIds(graph: CaptureGraph): Seq[BlockId] = {
    val master = graph.spark.sparkContext.env.blockManager.master
    allTapIds(graph).flatMap { tapId =>
      master.getMatchingBlockIds({
        case RDDBlockId(id, _) => id == tapId
        case _ => false
      }, askStorageEndpoints = true)
    }
  }

  /**
   * Drop all lineage blocks captured for this query.
   * @group lifecycle
   */
  def releaseLineage(df: DataFrame): Unit = {
    // A query that AQE collapsed to an empty relation has no taps to release.
    if (!finalPlan(df).exists(_.isInstanceOf[TapResultExec])) return
    val graph = captureGraph(df)
    val master = graph.spark.sparkContext.env.blockManager.master
    allBlockIds(graph).foreach(master.removeBlock)
  }

  /**
   * Memory and disk bytes currently held by this query's lineage blocks.
   * @group lifecycle
   */
  def lineageSize(df: DataFrame): (Long, Long) = {
    if (!finalPlan(df).exists(_.isInstanceOf[TapResultExec])) return (0L, 0L)
    val graph = captureGraph(df)
    val master = graph.spark.sparkContext.env.blockManager.master
    allBlockIds(graph).foldLeft((0L, 0L)) { case ((mem, disk), bid) =>
      val statuses = master.getBlockStatus(bid, askStorageEndpoints = true).values
      (mem + statuses.map(_.memSize).sum, disk + statuses.map(_.diskSize).sum)
    }
  }

  /**
   * Packed lineage ids aligned with `df.collect()` row order, for Py4J callers.
   * @group api
   */
  def resultIds(df: DataFrame): Array[Long] = {
    val graph = captureGraph(df)
    resultAssociations(graph.spark, graph.resultTapId).map(_._1)
  }

  /**
   * Trace entry point for Py4J callers, taking a Java list of numbers.
   * @group api
   */
  def traceJava(df: DataFrame, outputIds: java.util.List[java.lang.Number]): TraceCursor = {
    import scala.jdk.CollectionConverters._
    trace(df, outputIds.asScala.map(_.longValue()).toSeq)
  }

  /**
   * Witnesses at one source level of a full-tree trace. `sourcePath` is the file
   * source's root location when the level is a file scan (None for cached relations
   * and empty-relation leaves), so callers can map witnesses back to their tables.
   */
  case class SourceWitnesses(
      sourcePath: Option[String],
      rows: Array[Row],
      schema: org.apache.spark.sql.types.StructType)

  /**
   * Trace the given result rows backward through every branch (all join inputs and
   * union children) and resolve witnesses at each reachable source. Returns one entry
   * per scan level.
   * @group api
   */
  def traceAllSources(df: DataFrame, outputIds: Seq[Long]): Seq[SourceWitnesses] =
    trace(df, outputIds).showAllSources()

  /** Tap ids of every prism UDF field-taint tap in `df`'s executed plan, including AQE stages. */
  private def udfProvTapIds(df: DataFrame): Seq[Int] = {
    val seen = scala.collection.mutable.ArrayBuffer[Int]()
    def rec(p: SparkPlan): Unit = {
      p match { case t: TapUdfProvExec => seen += t.tapId; case _ => }
      p match {
        case a: AdaptiveSparkPlanExec => rec(a.executedPlan)
        case _ => p.getClass.getSimpleName match {
          case "ShuffleQueryStageExec" | "BroadcastQueryStageExec" |
               "ResultQueryStageExec" | "TableCacheQueryStageExec" =>
            try rec(p.getClass.getMethod("plan").invoke(p).asInstanceOf[SparkPlan])
            catch { case _: Throwable => }
          case _ =>
        }
      }
      p.children.foreach(rec)
    }
    rec(df.queryExecution.executedPlan)
    seen.distinct.toSeq
  }

  /**
   * UDF field taint for Py4J callers. Maps each witness id to its provenance code vector,
   * one code per prism UDF in eval order. Only the witnesses' codes return to the driver.
   * @group api
   */
  def udfProvJava(
      df: DataFrame,
      witnessIds: java.util.List[java.lang.Number]): java.util.Map[java.lang.Long, Array[Long]] = {
    import scala.jdk.CollectionConverters._
    val idSet = witnessIds.asScala.map(_.longValue()).toSet
    val m = new java.util.LinkedHashMap[java.lang.Long, Array[Long]]()
    for (tapId <- udfProvTapIds(df)) {
      val pairs = mapBlocks(df.sparkSession, tapId) { b =>
        val blk = b.asInstanceOf[UdfProvBlock]
        blk.ids.indices.iterator.collect {
          case k if idSet.contains(PackIntIntoLong(blk.split, blk.ids(k))) =>
            (PackIntIntoLong(blk.split, blk.ids(k)),
              Array.tabulate(blk.nProv)(s => blk.codes(k * blk.nProv + s)))
        }
      }
      pairs.foreach { case (id, vec) => m.put(java.lang.Long.valueOf(id), vec) }
    }
    m
  }

  /**
   * Per-input influence for Py4J callers at any aggregate level. `depth` 0 is the
   * outermost aggregate, and each unit walks one level inward. `slot` selects the
   * aggregate within that level, and `aggName` names its function (sum/avg/count/max/min).
   */
  def influenceJava(
      df: DataFrame, outputIds: java.util.List[java.lang.Number],
      aggName: String, slot: Int, depth: Int): java.util.Map[java.lang.Long, java.lang.Double] = {
    import scala.jdk.CollectionConverters._
    var cur = trace(df, outputIds.asScala.map(_.longValue()).toSeq)
    var i = 0
    while (i < depth) { cur = cur.goBack(0); i += 1 }
    val m = new java.util.LinkedHashMap[java.lang.Long, java.lang.Double]()
    cur.influence(aggName, slot).foreach { case (id, s) =>
      m.put(java.lang.Long.valueOf(id), java.lang.Double.valueOf(s)) }
    m
  }
}

/**
 * A position in the backward/forward walk. `ids` are packed (partition, localIdx)
 * coordinates at the current level, and `path` records the branches taken so goNext can
 * retrace forward.
 */
class TraceCursor private[lineage] (
    spark: SparkSession,
    source: PrismSQL.SourceInfo,
    val ids: Array[Long],
    path: List[(PrismSQL.SourceInfo, Array[Long], Int)]) {

  import PrismSQL._

  def atScan: Boolean = source.isInstanceOf[ScanSource]

  // Each walk step ships a small id or hash set into a flatMap over the tap's blocks.
  // Filtering runs next to the block and only the matches return to the driver.

  /** Key hashes of the level's rows whose packed output id is in `idSet`. */
  private def keyHashesOf(level: KeyedSource, idSet: Set[Long]): Set[Int] =
    if (level.hasProbe) {
      PrismSQL.mapBlocks(spark, level.tapId) { b =>
        val (split, pairs) = TapBlock.joinPairs(b)
        pairs.indices.iterator.collect {
          case i if idSet.contains(PackIntIntoLong(split, i)) =>
            PackIntIntoLong.getLeft(pairs(i))
        }
      }.toSet
    } else {
      PrismSQL.mapBlocks(spark, level.tapId) { b =>
        val (split, values) = TapBlock.keyedValues(b)
        values.indices.iterator.collect {
          case i if idSet.contains(PackIntIntoLong(split, i)) => values(i)
        }
      }.toSet
    }

  /** Packed output ids of the level's rows whose key hash is in `hashes`. */
  private def idsForKeyHashes(level: KeyedSource, hashes: Set[Int]): Array[Long] =
    if (level.hasProbe) {
      PrismSQL.mapBlocks(spark, level.tapId) { b =>
        val (split, pairs) = TapBlock.joinPairs(b)
        pairs.indices.iterator.collect {
          case i if hashes.contains(PackIntIntoLong.getLeft(pairs(i))) =>
            PackIntIntoLong(split, i)
        }
      }
    } else {
      PrismSQL.mapBlocks(spark, level.tapId) { b =>
        val (split, values) = TapBlock.keyedValues(b)
        values.indices.iterator.collect {
          case i if hashes.contains(values(i)) => PackIntIntoLong(split, i)
        }
      }
    }

  /** One step backward. `branch` selects the join or union input at a multi-input level. */
  def goBack(branch: Int = 0): TraceCursor = source match {
    case ScanSource(_, _) =>
      throw new UnsupportedOperationException("already at the source level")
    case UnionSource(children, starts, counts) =>
      // Ids pass through unchanged, and the branch owns a stage-partition range.
      val (start, end) = (starts(branch), starts(branch) + counts(branch))
      val nextIds = ids.filter { id =>
        val p = PackIntIntoLong.getLeft(id); p >= start && p < end
      }
      new TraceCursor(spark, children(branch), nextIds, (source, ids, branch) :: path)
    case ks @ KeyedSource(tapId, _, branches) =>
      val idSet = ids.toSet
      val nextIds = branches(branch) match {
        case DirectBranch(_) =>
          // Exact probe-row provenance. The probe id lives in the post-join block, in
          // the same partition as the output row.
          PrismSQL.mapBlocks(spark, tapId) { b =>
            val (split, pairs) = TapBlock.joinPairs(b)
            pairs.indices.iterator.collect {
              case i if idSet.contains(PackIntIntoLong(split, i)) =>
                PackIntIntoLong(split, PackIntIntoLong.getRight(pairs(i)))
            }
          }.distinct
        case HashBranch(preTapId, _) =>
          val hashes = keyHashesOf(ks, idSet)
          PrismSQL.mapBlocks(spark, preTapId) { b =>
            val (split, blockHashes, idLists) = TapBlock.preEntries(b)
            hashes.iterator.flatMap { h =>
              val j = java.util.Arrays.binarySearch(blockHashes, h)
              if (j >= 0) idLists(j).iterator.map(id => PackIntIntoLong(split, id))
              else Iterator.empty
            }
          }.distinct
      }
      new TraceCursor(spark, branches(branch).upstream, nextIds,
        (source, ids, branch) :: path)
  }

  /**
   * Like `goBack`, but returns None if this hop's frontier would exceed `cap` ids. The
   * count uses a bounded take, so an oversized hop never collects its ids to the driver.
   */
  private def goBackCapped(branch: Int, cap: Int): Option[TraceCursor] = source match {
    case ScanSource(_, _) => None
    case UnionSource(_, _, _) =>
      // A union only filters ids, so the frontier cannot grow.
      Some(goBack(branch))
    case ks @ KeyedSource(tapId, _, branches) =>
      val idSet = ids.toSet
      val raw: Array[Long] = branches(branch) match {
        case DirectBranch(_) =>
          PrismSQL.mapBlocksTake(spark, tapId, cap + 1) { b =>
            val (split, pairs) = TapBlock.joinPairs(b)
            pairs.indices.iterator.collect {
              case i if idSet.contains(PackIntIntoLong(split, i)) =>
                PackIntIntoLong(split, PackIntIntoLong.getRight(pairs(i)))
            }
          }
        case HashBranch(preTapId, _) =>
          val hashes = keyHashesOf(ks, idSet)
          PrismSQL.mapBlocksTake(spark, preTapId, cap + 1) { b =>
            val (split, blockHashes, idLists) = TapBlock.preEntries(b)
            hashes.iterator.flatMap { h =>
              val j = java.util.Arrays.binarySearch(blockHashes, h)
              if (j >= 0) idLists(j).iterator.map(id => PackIntIntoLong(split, id))
              else Iterator.empty
            }
          }
      }
      if (raw.length > cap) None
      else Some(new TraceCursor(spark, branches(branch).upstream, raw.distinct,
        (source, ids, branch) :: path))
  }

  /**
   * Write the source witnesses to `path`, choosing the strategy by frontier size. Small
   * frontiers walk on the driver, and any hop above `threshold` ids switches to the
   * distributed walk. Call it on the result-level cursor. Returns the witness count.
   */
  def writeWitnessesAuto(path: String, threshold: Int): Long = {
    val cap = if (threshold > 0) threshold else 500000
    var cur: TraceCursor = this
    var overflow = false
    while (!cur.atScan && !overflow) {
      cur.goBackCapped(0, cap) match {
        case Some(next) => cur = next
        case None => overflow = true
      }
    }
    if (overflow) writeWitnessesDistributed(path, 0)
    else cur.writeWitnesses(path)  // cur is at the scan level
  }

  /** One step forward, retracing the branch taken by the last goBack. */
  def goNext(): TraceCursor = path match {
    case Nil => throw new UnsupportedOperationException("already at the result level")
    case (parent: UnionSource, _, _) :: rest =>
      // Union levels share coordinates, so the current ids are already union-level ids.
      new TraceCursor(spark, parent, ids, rest)
    case (parent: KeyedSource, _, branch) :: rest =>
      val idSet = ids.toSet
      val parentIds = parent.branches(branch) match {
        case DirectBranch(_) =>
          PrismSQL.mapBlocks(spark, parent.tapId) { b =>
            val (split, pairs) = TapBlock.joinPairs(b)
            pairs.indices.iterator.collect {
              case i if idSet.contains(
                  PackIntIntoLong(split, PackIntIntoLong.getRight(pairs(i)))) =>
                PackIntIntoLong(split, i)
            }
          }
        case HashBranch(preTapId, _) =>
          val hashes =
            PrismSQL.mapBlocks(spark, preTapId) { b =>
              val (split, blockHashes, idLists) = TapBlock.preEntries(b)
              blockHashes.indices.iterator.collect {
                case j if idLists(j)
                  .exists(id => idSet.contains(PackIntIntoLong(split, id))) =>
                  blockHashes(j)
              }
            }.toSet
          idsForKeyHashes(parent, hashes)
      }
      new TraceCursor(spark, parent, parentIds, rest)
    case _ => throw new IllegalStateException("corrupt trace path")
  }

  /** Resolve source rows. Valid only at a scan level. */
  def show(): Array[Row] = show(full = false)

  /**
   * Resolve source rows. With `full = true`, return complete source records (all
   * columns) instead of the query's pruned view when the scan shape allows it.
   */
  def show(full: Boolean): Array[Row] = source match {
    case ScanSource(scan, off) =>
      PrismSQL.showScanRows(spark, scan, ids.toSeq, full, off)
    case _ => throw new UnsupportedOperationException(
      "show() resolves source rows. Call goBack until the cursor is at a scan.")
  }

  /**
   * Per-input influence on the aggregate that produced this level's rows, computed from
   * contributions captured on the map side. The level must be a single-branch aggregate
   * whose pre tap captured influence. Groups split across map partitions are merged by
   * key hash before the closed form is applied. Returns pairs of packed input id and
   * influence score.
   */
  def influence(aggName: String, slot: Int): Array[(Long, Double)] = source match {
    case ks @ KeyedSource(_, false, Seq(HashBranch(preTapId, _))) =>
      val hashes = keyHashesOf(ks, ids.toSet)
      val partials = PrismSQL.mapBlocks(spark, preTapId) { b =>
        val (split, blockHashes, idLists, valLists, nAgg) = TapBlock.influenceEntries(b)
        hashes.iterator.flatMap { h =>
          val j = java.util.Arrays.binarySearch(blockHashes, h)
          if (j >= 0 && slot < nAgg) {
            val members = idLists(j).indices.iterator.map { k =>
              (PackIntIntoLong(split, idLists(j)(k)), valLists(j)(k * nAgg + slot))
            }.toArray
            Iterator.single((h, members))
          } else Iterator.empty
        }
      }
      partials.groupBy(_._1).iterator.flatMap { case (_, parts) =>
        TraceCursor.closedForm(aggName, parts.flatMap(_._2))
      }.toArray
    case _ => throw new UnsupportedOperationException(
      "influence(): not at a single-aggregate level, or influence capture was not " +
        "enabled (set spark.prism.sql.influence=true before running the query)")
  }

  /**
   * Resolve witnesses at every source level reachable from here, following every branch
   * of every join and union below.
   */
  def showAllSources(): Seq[PrismSQL.SourceWitnesses] = source match {
    case ScanSource(scan, off) =>
      val (rows, schema) =
        PrismSQL.showScanRowsWithSchema(spark, scan, ids.toSeq, full = false, off)
      val path = scan match {
        case fs: FileSourceScanExec =>
          fs.relation.location.rootPaths.headOption.map(_.toString)
        case _ => None
      }
      Seq(PrismSQL.SourceWitnesses(path, rows, schema))
    case KeyedSource(_, _, branches) =>
      branches.indices.flatMap(i => goBack(i).showAllSources())
    case UnionSource(children, _, _) =>
      children.indices.flatMap(i => goBack(i).showAllSources())
  }

  /** Source rows as JSON strings, for Py4J callers. */
  def showJson(full: Boolean): Array[String] = source match {
    case ScanSource(scan, off) =>
      val (rows, schema) =
        PrismSQL.showScanRowsWithSchema(spark, scan, ids.toSeq, full, off)
      import scala.jdk.CollectionConverters._
      spark.createDataFrame(rows.toSeq.asJava, schema).toJSON.collect()
    case _ => throw new UnsupportedOperationException(
      "show() resolves source rows. Call goBack until the cursor is at a scan.")
  }

  /**
   * Write the traced source witnesses to `path` with `saveAsTextFile` instead of
   * collecting them to the driver. Returns the witness count.
   */
  def writeWitnesses(path: String): Long = source match {
    case ScanSource(scan, off) =>
      PrismSQL.writeScanRows(spark, scan, ids.toSeq, off, path)
      ids.size.toLong
    case _ => throw new UnsupportedOperationException(
      "writeWitnesses() resolves source rows. Call goBack until the cursor is at a scan.")
  }

  /**
   * Fully distributed backward walk from this cursor to the scan, writing the source
   * witnesses to `path`. The frontier stays an `RDD[Long]`, so each hop is a shuffle join
   * against the tap associations and the driver never holds it. Follows branch 0 at each
   * hop. Returns the witness count.
   */
  def writeWitnessesDistributed(path: String, parallelism: Int): Long = {
    import PrismSQL._
    val sc = spark.sparkContext
    val par = if (parallelism > 0) parallelism else math.max(sc.defaultParallelism, 8)
    var src: SourceInfo = source
    var frontier: RDD[Long] =
      sc.parallelize(ids.toSeq, math.max(1, math.min(par, math.max(1, ids.length))))
    while (!src.isInstanceOf[ScanSource]) {
      src match {
        case UnionSource(children, starts, counts) =>
          val start = starts(0); val end = starts(0) + counts(0)
          frontier = frontier.filter { id =>
            val p = PackIntIntoLong.getLeft(id); p >= start && p < end
          }
          src = children(0)
        case ks @ KeyedSource(tapId, hasProbe, branches) =>
          branches(0) match {
            case DirectBranch(up) =>
              val level = directPairsRDD(spark, tapId)
              frontier = frontier.map(id => (id, ())).join(level, par)
                .map { case (_, (_, inId)) => inId }.distinct(par)
              src = up
            case HashBranch(preTapId, up) =>
              val level = levelKeyHashRDD(spark, tapId, hasProbe)
              val pre = preHashIdRDD(spark, preTapId)
              val hashes = frontier.map(id => (id, ())).join(level, par)
                .map { case (_, (_, h)) => h }.distinct(par)
              frontier = hashes.map(h => (h, ())).join(pre, par)
                .map { case (_, (_, inId)) => inId }.distinct(par)
              src = up
          }
      }
    }
    src match {
      case ScanSource(scan, off) =>
        PrismSQL.writeScanRowsJoin(spark, scan, frontier, off, path, par)
      case _ => throw new IllegalStateException("distributed walk did not end at a scan")
    }
  }

}

private[lineage] object TraceCursor {
  /**
   * Closed-form influence over a group's captured contributions, equal to leave-one-out
   * influence but computed in one pass. `members` holds (id, contribution) pairs. NaN
   * marks a NULL input, which gets influence 0.
   */
  def closedForm(aggName: String, members: Array[(Long, Double)]): Array[(Long, Double)] = {
    val valid = members.filterNot(_._2.isNaN)
    aggName.toLowerCase match {
      case "sum" =>
        members.map { case (id, v) => (id, if (v.isNaN) 0.0 else v) }
      case "count" =>
        members.map { case (id, v) => (id, if (v.isNaN) 0.0 else 1.0) }
      case "avg" | "mean" =>
        val n = valid.length
        if (n <= 1) members.map { case (id, _) => (id, 0.0) }
        else {
          val mean = valid.map(_._2).sum / n
          members.map { case (id, v) => (id, if (v.isNaN) 0.0 else (v - mean) / (n - 1)) }
        }
      case "max" =>
        if (valid.isEmpty) members.map { case (id, _) => (id, 0.0) }
        else {
          val sorted = valid.sortBy(-_._2)
          val (topId, topV) = sorted(0)
          val runner = if (sorted.length > 1) sorted(1)._2 else topV
          members.map { case (id, _) => (id, if (id == topId) topV - runner else 0.0) }
        }
      case "min" =>
        if (valid.isEmpty) members.map { case (id, _) => (id, 0.0) }
        else {
          val sorted = valid.sortBy(_._2)
          val (botId, botV) = sorted(0)
          val runner = if (sorted.length > 1) sorted(1)._2 else botV
          members.map { case (id, _) => (id, if (id == botId) botV - runner else 0.0) }
        }
      case "median" => percentileLeaveOneOut(members, 0.5)
      case name if name.startsWith("percentile:") =>
        percentileLeaveOneOut(members, name.stripPrefix("percentile:").toDouble)
      case other =>
        throw new IllegalArgumentException(s"no closed-form influence for aggregate: $other")
    }
  }

  /**
   * Leave-one-out influence for a percentile, which has no constant-size summary. The
   * group is sorted once, and the percentile without each member is read from the sorted
   * values with that position skipped, using the interpolation rule of Spark's `percentile`.
   */
  private def percentileLeaveOneOut(
      members: Array[(Long, Double)], p: Double): Array[(Long, Double)] = {
    val valid = members.filterNot(_._2.isNaN)
    val sorted = valid.map(_._2).sorted
    def at(skip: Int, k: Int): Double = sorted(if (skip >= 0 && k >= skip) k + 1 else k)
    def pct(skip: Int): Double = {
      val n = sorted.length - (if (skip >= 0) 1 else 0)
      if (n == 0) Double.NaN
      else {
        val pos = p * (n - 1)
        val lo = math.floor(pos).toInt
        val hi = math.ceil(pos).toInt
        at(skip, lo) + (pos - lo) * (at(skip, hi) - at(skip, lo))
      }
    }
    val full = pct(-1)
    val rank = valid.sortBy(_._2).map(_._1).zipWithIndex.toMap
    members.map { case (id, v) =>
      if (v.isNaN || sorted.length < 2) (id, 0.0) else (id, full - pct(rank(id)))
    }
  }
}

/**
 * Reads materialized tap blocks back as an RDD, one partition per captured block. Each
 * partition prefers the executor that holds its block so trace filtering runs in place.
 */
private[lineage] class TapBlockRDD[T: ClassTag](
    sc: SparkContext, tapId: Int, parts: Seq[Int], locations: Seq[Seq[String]])
  extends RDD[T](sc, Nil) {

  override def getPartitions: Array[Partition] =
    parts.zipWithIndex.map { case (block, i) =>
      new TapBlockPartition(i, block)
    }.toArray

  override def getPreferredLocations(split: Partition): Seq[String] =
    if (split.index < locations.length) locations(split.index) else Nil

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val block = split.asInstanceOf[TapBlockPartition].block
    SparkEnv.get.blockManager.get[Any](RDDBlockId(tapId, block)) match {
      case Some(result) => result.data.asInstanceOf[Iterator[T]]
      case None => Iterator.empty
    }
  }
}

private[lineage] class TapBlockPartition(val index: Int, val block: Int)
  extends Partition
