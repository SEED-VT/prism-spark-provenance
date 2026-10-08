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

package org.apache.spark.lineage

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.lineage.util.LongIntByteBuffer
import org.apache.spark.util.collection._
import org.apache.spark.{Aggregator, TaskContext}

/**
 * Lineage-aware aggregator. When `isLineage` is on, each incoming value carries a packed
 * lineage id as `(K, (V, Long))`. While combining, the aggregator records
 * `(lineageId, hashKey(key))` into the task's capture buffer.
 *
 * A recording iterator strips the lineage id before the stock `ExternalAppendOnlyMap.insertAll`
 * consumes the records. Whether a read serves cached shuffle data comes from
 * [[LineageTaskState.isShuffleCache]].
 *
 * @param createCombiner function to create the initial value of the aggregation.
 * @param mergeValue function to merge a new value into the aggregation result.
 * @param mergeCombiners function to merge outputs from multiple mergeValue function.
 */
@DeveloperApi
class LAggregator[K, V, C] (
    val userCreateCombiner: V => C,
    val userMergeValue: (C, V) => C,
    val userMergeCombiners: (C, C) => C,
    val isLineage: Boolean = false)
  extends Aggregator[K, V, C](
    // The stock shuffle write path applies these functions during map-side combine. In
    // lineage mode the records are (K, (V, packedId)), so the wrappers carry the tag
    // through the combine and collapse it to (sourcePartition, 0).
    if (isLineage) LAggregator.wrapCreate(userCreateCombiner) else userCreateCombiner,
    if (isLineage) LAggregator.wrapMergeValue(userMergeValue) else userMergeValue,
    if (isLineage) LAggregator.wrapMergeCombiners(userMergeCombiners) else userMergeCombiners) {


  override def combineValuesByKey(
      iter: Iterator[_ <: Product2[K, V]],
      context: TaskContext): Iterator[(K, C)] = {
    val combiners = new ExternalAppendOnlyMap[K, V, C](userCreateCombiner, userMergeValue, userMergeCombiners)

    if (!isLineage) {
      combiners.insertAll(iter)
    } else {
      val state = LineageTaskState.get(context)
      val buffer = new LongIntByteBuffer(state.getFromBufferPool())
      val tappedIter = iter.asInstanceOf[Iterator[Product2[K, Product2[V, Long]]]]
      val recordingIter = tappedIter.map { pair =>
        buffer.put(pair._2._2, LineageHashing.hashKey(pair._1))
        (pair._1, pair._2._1)
      }
      combiners.insertAll(recordingIter)
      state.currentBuffer = buffer
    }

    updateMetrics(context, combiners)
    combiners.iterator
  }

  override def combineCombinersByKey(
      iter: Iterator[_ <: Product2[K, C]],
      context: TaskContext): Iterator[(K, C)] = {
    val combiners = new ExternalAppendOnlyMap[K, C, C](identity, userMergeCombiners, userMergeCombiners)

    if (!isLineage) {
      combiners.insertAll(iter)
    } else {
      val state = LineageTaskState.get(context)
      val tappedIter = iter.asInstanceOf[Iterator[Product2[K, Product2[C, Long]]]]
      if (state.isShuffleCache) {
        // Reading already-captured shuffle data back as a cache: strip lineage, no capture
        combiners.insertAll(tappedIter.map(pair => (pair._1, pair._2._1)))
      } else {
        val buffer = new LongIntByteBuffer(state.getFromBufferPool())
        val recordingIter = tappedIter.map { pair =>
          buffer.put(pair._2._2, LineageHashing.hashKey(pair._1))
          (pair._1, pair._2._1)
        }
        combiners.insertAll(recordingIter)
        state.currentBuffer = buffer
      }
    }

    updateMetrics(context, combiners)
    combiners.iterator
  }

  def setLineage(lineage: Boolean): Aggregator[K, V, C] = {
    // The map-side wrappers are fixed at construction, so flipping the flag means
    // building a fresh instance from the original user functions.
    new LAggregator(userCreateCombiner, userMergeValue, userMergeCombiners, lineage)
      .asInstanceOf[Aggregator[K, V, C]]
  }

  private def updateMetrics(context: TaskContext, map: ExternalAppendOnlyMap[_, _, _]): Unit = {
    Option(context).foreach { c =>
      c.taskMetrics().incMemoryBytesSpilled(map.memoryBytesSpilled)
      c.taskMetrics().incDiskBytesSpilled(map.diskBytesSpilled)
      c.taskMetrics().incPeakExecutionMemory(map.peakMemoryUsedBytes)
    }
  }
}

object LAggregator {

  import org.apache.spark.util.PackIntIntoLong

  /**
   * Mutable carrier for a combined value plus its lineage tag. A combiner lives in
   * exactly one map slot, so merges can update `value` in place instead of allocating
   * a fresh Tuple2 per record. Implements Product2 because everything downstream
   * (reduce-side aggregation, trace machinery) reads tagged combiners through
   * `Product2[C, Long]`.
   */
  private final class TaggedCombiner(var value: Any, val tag: Long)
    extends Product2[Any, Long] with Serializable {
    override def _1: Any = value
    override def _2: Long = tag
    override def canEqual(that: Any): Boolean = that.isInstanceOf[TaggedCombiner]
  }

  // The casts are deliberate type punning, because in lineage mode the runtime objects in
  // an RDD[(K, V)]-typed shuffle are (K, (V, Long)). The boxedCombiner ablation allocates
  // a Tuple2 per merge instead. Both shapes are read downstream through Product2.

  private def wrapCreate[V, C](f: V => C): V => C = {
    if (PrismAblation.boxedCombiner) { (tagged: Any) =>
      val pair = tagged.asInstanceOf[Product2[V, Long]]
      (f(pair._1), PackIntIntoLong(PackIntIntoLong.getLeft(pair._2), 0))
    } else { (tagged: Any) =>
      val pair = tagged.asInstanceOf[Product2[V, Long]]
      new TaggedCombiner(f(pair._1), PackIntIntoLong(PackIntIntoLong.getLeft(pair._2), 0))
    }
  }.asInstanceOf[V => C]

  private def wrapMergeValue[V, C](f: (C, V) => C): (C, V) => C = {
    if (PrismAblation.boxedCombiner) { (combined: Any, tagged: Any) =>
      val c = combined.asInstanceOf[Product2[C, Long]]
      val pair = tagged.asInstanceOf[Product2[V, Long]]
      (f(c._1, pair._1), c._2)
    } else { (combined: Any, tagged: Any) =>
      val c = combined.asInstanceOf[TaggedCombiner]
      val pair = tagged.asInstanceOf[Product2[V, Long]]
      c.value = f(c.value.asInstanceOf[C], pair._1)
      c
    }
  }.asInstanceOf[(C, V) => C]

  private def wrapMergeCombiners[C](f: (C, C) => C): (C, C) => C = {
    if (PrismAblation.boxedCombiner) { (left: Any, right: Any) =>
      val a = left.asInstanceOf[Product2[C, Long]]
      val b = right.asInstanceOf[Product2[C, Long]]
      (f(a._1, b._1), a._2)
    } else { (left: Any, right: Any) =>
      val a = left.asInstanceOf[TaggedCombiner]
      val b = right.asInstanceOf[TaggedCombiner]
      a.value = f(a.value.asInstanceOf[C], b.value.asInstanceOf[C])
      a
    }
  }.asInstanceOf[(C, C) => C]
}
