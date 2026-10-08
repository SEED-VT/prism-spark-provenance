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

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.lineage.LineageTaskState
import org.apache.spark.storage.{RDDBlockId, StorageLevel}
import org.apache.spark.util.PackIntIntoLong

/*
 * Per-task runtime objects invoked by the tap operators. One instance exists per task,
 * created in the generated class's init (codegen) or in mapPartitions (interpreted).
 * They are plain classes with simple methods so generated Java can call them directly.
 */

/**
 * Compact lineage block payload, one object of primitive arrays per (tapId, partition).
 * A row's packed output id is not stored, since the partition is constant per block and
 * the local index is the row's array position.
 */
private[lineage] sealed trait TapBlock extends Serializable { def split: Int }

/**
 * Post-keyed or result tap block. Row i has packed id (split, i), and values(i) is the
 * key hash (post-keyed) or the upstream input id (result).
 */
private[lineage] case class KeyedBlock(split: Int, values: Array[Int]) extends TapBlock

/** Broadcast-join tap block: pairs(i) packs (keyHash, probeInputId) for row i. */
private[lineage] case class JoinBlock(split: Int, pairs: Array[Long]) extends TapBlock

/**
 * Pre-exchange tap block. Hashes are sorted ascending for binary search, and
 * idLists(j) holds the input ids that produced key hash hashes(j).
 */
private[lineage] case class PreExchangeBlock(
    split: Int, hashes: Array[Int], idLists: Array[Array[Int]]) extends TapBlock

/**
 * Pre-exchange tap block that also carries influence for `nAgg` aggregates.
 * For key hash hashes(j), valLists(j) is member-major. Input id idLists(j)(k)
 * contributed valLists(j)(k*nAgg + a) to aggregate slot a, which is the per-row value
 * of that aggregate's input expression. NaN marks a SQL NULL.
 */
private[lineage] case class InfluencePreExchangeBlock(
    split: Int, hashes: Array[Int], idLists: Array[Array[Int]],
    valLists: Array[Array[Double]], nAgg: Int) extends TapBlock

/**
 * UDF field-taint block. ids(k) is a captured input row id, and codes(k*nProv + s) is
 * the provenance code emitted by prism UDF s for that row. Each code holds bitmasks over
 * the UDF's parameters (data and control dependencies) plus the regime, as decoded on
 * the Python side.
 */
private[lineage] case class UdfProvBlock(
    split: Int, ids: Array[Int], codes: Array[Long], nProv: Int) extends TapBlock

// Ablation-only block shapes that store one boxed tuple per captured row.

/** Ablation (boxedBlocks): rows are (packedOutId: Long, value: Long) tuples. */
private[lineage] case class BoxedKeyedBlock(split: Int, rows: Array[Any]) extends TapBlock

/** Ablation (boxedBlocks): rows are (packedOutId, (keyHash, probeId)) tuples. */
private[lineage] case class BoxedJoinBlock(split: Int, rows: Array[Any]) extends TapBlock

/** Ablation (boxedBlocks): rows are (keyHash, Array[inputId]) tuples, hash-sorted. */
private[lineage] case class BoxedPreExchangeBlock(split: Int, rows: Array[Any])
  extends TapBlock

private[lineage] object TapBlock {
  /**
   * Storage level for lineage blocks, from `spark.prism.lineage.storageLevel`. It is a
   * cluster conf, so set it at submit time.
   */
  def storageLevel: StorageLevel = StorageLevel.fromString(
    SparkEnv.get.conf.get("spark.prism.lineage.storageLevel", "MEMORY_AND_DISK_SER"))

  def store(tapId: Int, split: Int, block: TapBlock): Unit =
    SparkEnv.get.blockManager.putSingle[Any](
      RDDBlockId(tapId, split), block, storageLevel, tellMaster = true)

  // Trace logic reads every block through these normalizers, so the compact and boxed
  // (ablation) formats are interchangeable.

  def keyedValues(b: TapBlock): (Int, Array[Int]) = b match {
    case KeyedBlock(s, v) => (s, v)
    case BoxedKeyedBlock(s, rows) =>
      (s, rows.map(_.asInstanceOf[(Long, Long)]._2.toInt))
    case other => throw new IllegalStateException(s"not a keyed block: $other")
  }

  def joinPairs(b: TapBlock): (Int, Array[Long]) = b match {
    case JoinBlock(s, p) => (s, p)
    case BoxedJoinBlock(s, rows) =>
      (s, rows.map { r =>
        val (_, hp) = r.asInstanceOf[(Long, (Int, Int))]
        PackIntIntoLong(hp._1, hp._2)
      })
    case other => throw new IllegalStateException(s"not a join block: $other")
  }

  def preEntries(b: TapBlock): (Int, Array[Int], Array[Array[Int]]) = b match {
    case PreExchangeBlock(s, h, ids) => (s, h, ids)
    case InfluencePreExchangeBlock(s, h, ids, _, _) => (s, h, ids)
    case BoxedPreExchangeBlock(s, rows) =>
      val typed = rows.map(_.asInstanceOf[(Int, Array[Int])])
      (s, typed.map(_._1), typed.map(_._2))
    case other => throw new IllegalStateException(s"not a pre-exchange block: $other")
  }

  /**
   * Hashes, parallel idLists, and member-major valLists with stride `nAgg`. Only an
   * [[InfluencePreExchangeBlock]] carries values.
   */
  def influenceEntries(
      b: TapBlock): (Int, Array[Int], Array[Array[Int]], Array[Array[Double]], Int) = b match {
    case InfluencePreExchangeBlock(s, h, ids, vals, n) => (s, h, ids, vals, n)
    case other => throw new IllegalStateException(
      s"not an influence pre-exchange block (was influence capture enabled?): $other")
  }
}

/** Scan-side tap that assigns the per-task input row id. */
class ScanTapRuntime {
  private val state = LineageTaskState.get(TaskContext.get())
  private var nextId: Int = -1

  /** Called once per input row, before downstream operators consume it. */
  def tap(): Unit = {
    nextId += 1
    state.currentInputId = nextId
  }
}

/**
 * Pre-exchange tap that maps each key hash to a bitmap of input ids on the map side of
 * a shuffle. It sits below the partial aggregate when one exists, otherwise directly
 * below the exchange, so it always sees pre-combine rows.
 */
class PreExchangeTapRuntime(tapId: Int) {
  private val ctx = TaskContext.get()
  private val state = LineageTaskState.get(ctx)
  private val split = ctx.partitionId()
  private val map = new org.apache.spark.lineage.Int2RoaringBitMapOpenHashMap(16384)

  ctx.addTaskCompletionListener[Unit] { _ =>
    val hashes = map.keySet().toIntArray
    java.util.Arrays.sort(hashes)
    val block =
      if (org.apache.spark.lineage.PrismAblation.boxedBlocks) {
        BoxedPreExchangeBlock(split,
          hashes.map(k => (k, map.get(k).toArray): Any))
      } else {
        PreExchangeBlock(split, hashes, hashes.map(map.get(_).toArray))
      }
    TapBlock.store(tapId, split, block)
  }

  /** Called once per pre-combine row with the murmur hash of its partition/group key. */
  def tap(keyHash: Int): Unit = map.put(keyHash, state.currentInputId)
}

/**
 * Pre-exchange tap that also records each pre-combine row's contribution to its
 * group's aggregates. Per group it keeps parallel id and value lists rather than a bitmap.
 */
class InfluencePreExchangeTapRuntime(tapId: Int, nAgg: Int) {
  private val ctx = TaskContext.get()
  private val state = LineageTaskState.get(ctx)
  private val split = ctx.partitionId()
  private val ids =
    new it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap[
      it.unimi.dsi.fastutil.ints.IntArrayList](16384)
  // Member-major contributions per group, nAgg doubles appended per row.
  private val vals =
    new it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap[
      it.unimi.dsi.fastutil.doubles.DoubleArrayList](16384)

  ctx.addTaskCompletionListener[Unit] { _ =>
    val hashes = ids.keySet().toIntArray
    java.util.Arrays.sort(hashes)
    val idLists = hashes.map(h => ids.get(h).toIntArray)
    val valLists = hashes.map(h => vals.get(h).toDoubleArray)
    TapBlock.store(tapId, split,
      InfluencePreExchangeBlock(split, hashes, idLists, valLists, nAgg))
  }

  /**
   * Called once per pre-combine row with its group key hash and its contribution to
   * each captured aggregate (NaN for a NULL input).
   */
  def tap(keyHash: Int, contributions: Array[Double]): Unit = {
    var il = ids.get(keyHash)
    if (il == null) {
      il = new it.unimi.dsi.fastutil.ints.IntArrayList()
      ids.put(keyHash, il)
      vals.put(keyHash, new it.unimi.dsi.fastutil.doubles.DoubleArrayList())
    }
    il.add(state.currentInputId)
    val dv = vals.get(keyHash)
    var a = 0
    while (a < contributions.length) { dv.add(contributions(a)); a += 1 }
  }
}

/**
 * Post-keyed tap above a final aggregate or reduce-side join. It records the key hash
 * of each output row and threads the output index downstream as currentInputId.
 */
class PostKeyedTapRuntime(tapId: Int) {
  private val ctx = TaskContext.get()
  private val state = LineageTaskState.get(ctx)
  private val split = ctx.partitionId()
  private val hashes = new it.unimi.dsi.fastutil.ints.IntArrayList()

  ctx.addTaskCompletionListener[Unit] { _ =>
    TapBlock.store(tapId, split, keyedBlockOf(split, hashes.toIntArray))
  }

  /** Called once per combined output row with the murmur hash of its key. */
  def tap(keyHash: Int): Unit = {
    state.currentInputId = hashes.size()
    hashes.add(keyHash)
  }
}

/** Builds a compact (or, under ablation, boxed) keyed block from per-row int values. */
private object keyedBlockOf {
  def apply(split: Int, values: Array[Int]): TapBlock =
    if (org.apache.spark.lineage.PrismAblation.boxedBlocks) {
      BoxedKeyedBlock(split,
        values.zipWithIndex.map { case (v, i) =>
          (PackIntIntoLong(split, i), v.toLong): Any
        })
    } else KeyedBlock(split, values)
}

/**
 * Saves the probe row's input id into the join's mark slot as the row enters a
 * BroadcastHashJoin. Fan-out matches are traversed depth-first, so the first match's
 * downstream taps overwrite `currentInputId` before the second match emerges.
 */
class ProbeMarkTapRuntime(pairId: Int) {
  private val state = LineageTaskState.get(TaskContext.get())

  def tap(): Unit = state.probeMarks.put(pairId, state.currentInputId)
}

/**
 * Broadcast-join tap above a BroadcastHashJoin. Records the key hash and probe input id
 * of each output row. The key hash locates the build side's lineage, and the probe id is
 * exact, read from the mark saved by [[ProbeMarkTapRuntime]].
 */
class PostJoinTapRuntime(tapId: Int, pairId: Int) {
  private val ctx = TaskContext.get()
  private val state = LineageTaskState.get(ctx)
  private val split = ctx.partitionId()
  // One packed (keyHash, probeId) long per row. The boxedJoinBuffer ablation uses a
  // boxed tuple buffer instead.
  private val boxedBuffer = org.apache.spark.lineage.PrismAblation.boxedJoinBuffer
  private val pairs =
    if (boxedBuffer) null else new it.unimi.dsi.fastutil.longs.LongArrayList()
  private val boxed =
    if (boxedBuffer) new scala.collection.mutable.ArrayBuffer[Any]() else null

  ctx.addTaskCompletionListener[Unit] { _ =>
    val longs =
      if (boxedBuffer) {
        boxed.map { r =>
          val (_, hp) = r.asInstanceOf[(Long, (Int, Int))]
          PackIntIntoLong(hp._1, hp._2)
        }.toArray
      } else pairs.toLongArray
    val block =
      if (org.apache.spark.lineage.PrismAblation.boxedBlocks) {
        BoxedJoinBlock(split, longs.zipWithIndex.map { case (p, i) =>
          (PackIntIntoLong(split, i),
            (PackIntIntoLong.getLeft(p), PackIntIntoLong.getRight(p))): Any
        })
      } else JoinBlock(split, longs)
    TapBlock.store(tapId, split, block)
  }

  def tap(keyHash: Int): Unit = {
    val outIdx = if (boxedBuffer) boxed.size else pairs.size()
    if (boxedBuffer) {
      boxed += ((PackIntIntoLong(split, outIdx),
        (keyHash, state.probeMarks.get(pairId))))
    } else {
      pairs.add(PackIntIntoLong(keyHash, state.probeMarks.get(pairId)))
    }
    state.currentInputId = outIdx
  }
}

/**
 * Below a buffered one-to-one operator (Python UDF eval), records the input-id
 * sequence so the matching ReseqTap above can replay it positionally.
 */
class SeqTapRuntime(pairId: Int) {
  private val state = LineageTaskState.get(TaskContext.get())
  private val buf = new it.unimi.dsi.fastutil.ints.IntArrayList()
  state.seqBuffers.put(pairId, buf)

  def tap(): Unit = buf.add(state.currentInputId)
}

/**
 * UDF field-taint tap that records the provenance codes each prism UDF emitted for a
 * row, keyed by currentInputId. It sits above the Python eval's reseq tap, so
 * currentInputId is the upstream row id.
 */
class UdfProvTapRuntime(tapId: Int, nProv: Int) {
  private val ctx = TaskContext.get()
  private val state = LineageTaskState.get(ctx)
  private val split = ctx.partitionId()
  private val ids = new it.unimi.dsi.fastutil.ints.IntArrayList()
  private val codes = new it.unimi.dsi.fastutil.longs.LongArrayList()

  ctx.addTaskCompletionListener[Unit] { _ =>
    TapBlock.store(tapId, split,
      UdfProvBlock(split, ids.toIntArray, codes.toLongArray, nProv))
  }

  /** Called once per row with the provenance codes of that row (one per prism UDF). */
  def tap(rowCodes: Array[Long]): Unit = {
    ids.add(state.currentInputId)
    var i = 0
    while (i < rowCodes.length) { codes.add(rowCodes(i)); i += 1 }
  }
}

/** Above the buffered operator, restores currentInputId row by row. */
class ReseqTapRuntime(pairId: Int) {
  private val state = LineageTaskState.get(TaskContext.get())
  private var i: Int = -1

  def tap(): Unit = {
    i += 1
    state.currentInputId = state.seqBuffers.get(pairId).getInt(i)
  }
}

/**
 * Result-side tap that records currentInputId for each output row and stores the
 * buffer in the BlockManager at task end.
 */
class ResultTapRuntime(tapId: Int) {
  private val ctx = TaskContext.get()
  private val state = LineageTaskState.get(ctx)
  private val split = ctx.partitionId()
  private val inputIds = new it.unimi.dsi.fastutil.ints.IntArrayList()

  ctx.addTaskCompletionListener[Unit] { _ =>
    TapBlock.store(tapId, split, keyedBlockOf(split, inputIds.toIntArray))
  }

  /** Called once per output row. */
  def tap(): Unit = inputIds.add(state.currentInputId)
}
