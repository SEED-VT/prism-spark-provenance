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

import org.apache.spark._
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.lineage.util.LongIntByteBuffer
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleReader}
import org.apache.spark.storage.{BlockManager, ShuffleBlockFetcherIterator}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

/**
 * Lineage-aware shuffle reader used by `ShuffledLRDD.compute`.
 *
 * The fetch plumbing mirrors Spark 4.1.2's `BlockStoreShuffleReader.read()`, so this class
 * is tied to that Spark version. The `isCache` argument selects the read mode.
 *  - `Some(true)` is a pre-shuffle cache read. It returns the raw fetched records, since
 *    the data was already combined when written.
 *  - `Some(false)` is a post-shuffle cache read. Empty partitions return early, and the
 *    rest go through the `LAggregator` capture path.
 *  - `None` is a normal capture-time read. `LAggregator` captures lineage while
 *    aggregating when its lineage flag is on.
 */
private[spark] class LBlockStoreShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    var lineage: Boolean = false,
    serializerManager: SerializerManager = SparkEnv.get.serializerManager,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker)
  extends ShuffleReader[K, C] with Logging {

  private val dep = handle.dependency

  override def read(): Iterator[Product2[K, C]] = read(None)

  /** Read the combined key-values for this reduce task */
  def read(isCache: Option[Boolean], shuffId: Int = 0): Iterator[Product2[K, C]] = {
    val readMetrics = context.taskMetrics().createTempShuffleReadMetrics()
    val blocksByAddress = mapOutputTracker.getMapSizesByExecutorId(
      handle.shuffleId, 0, Int.MaxValue, startPartition, endPartition)

    val wrappedStreams = new ShuffleBlockFetcherIterator(
      context,
      blockManager.blockStoreClient,
      blockManager,
      mapOutputTracker,
      blocksByAddress,
      serializerManager.wrapStream,
      // Sizes without a suffix are read as MB for backwards compatibility.
      SparkEnv.get.conf.get(config.REDUCER_MAX_SIZE_IN_FLIGHT) * 1024 * 1024,
      SparkEnv.get.conf.get(config.REDUCER_MAX_REQS_IN_FLIGHT),
      SparkEnv.get.conf.get(config.REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS),
      SparkEnv.get.conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM),
      SparkEnv.get.conf.get(config.SHUFFLE_MAX_ATTEMPTS_ON_NETTY_OOM),
      SparkEnv.get.conf.get(config.SHUFFLE_DETECT_CORRUPT),
      SparkEnv.get.conf.get(config.SHUFFLE_DETECT_CORRUPT_MEMORY),
      SparkEnv.get.conf.get(config.SHUFFLE_CHECKSUM_ENABLED),
      SparkEnv.get.conf.get(config.SHUFFLE_CHECKSUM_ALGORITHM),
      readMetrics,
      doBatchFetch = false).toCompletionIterator

    val serializerInstance = dep.serializer.newInstance()

    // Create a key/value iterator for each stream
    val recordIter = wrappedStreams.flatMap { case (blockId, wrappedStream) =>
      // asKeyValueIterator wraps the records in a NextIterator, which closes the
      // underlying InputStream once all records have been read.
      serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
    }

    // Update the context task metrics for each record read.
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        record
      },
      context.taskMetrics().mergeShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    if (isCache.isDefined) {
      if (isCache.get) {
        return interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
      } else {
        if (interruptibleIter.isEmpty) {
          return Iterator.empty
        }
        lineage = true
      }
    }

    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        // We are reading values that are already combined
        val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
        dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
      } else {
        // The value type is unknown here. The dependency ensures it is compatible with
        // this aggregator, which converts it to the combined type C.
        val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    } else if (dep.aggregator.isEmpty && dep.mapSideCombine) {
      throw new IllegalStateException("Aggregator is empty for map-side combine")
    } else {
      // Convert the Product2s to pairs since this is what downstream RDDs currently expect
      interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]].map(pair => (pair._1, pair._2))
    }

    // Sort the output if there is a sort ordering defined.
    dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        // In lineage mode, move each record's lineage tag into the task capture buffer
        // before the stock sorter consumes the records.
        val toSort: Iterator[Product2[K, C]] = if (lineage) {
          val state = LineageTaskState.get(context)
          val buffer = new LongIntByteBuffer(state.getFromBufferPool())
          state.currentBuffer = buffer
          aggregatedIter.asInstanceOf[Iterator[Product2[K, Product2[C, Long]]]].map { pair =>
            buffer.put(pair._2._2, LineageHashing.hashKey(pair._1))
            (pair._1, pair._2._1)
          }
        } else {
          aggregatedIter
        }
        val sorter =
          new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = dep.serializer)
        sorter.insertAllAndUpdateMetrics(toSort.asInstanceOf[Iterator[(K, C)]])
      case None =>
        aggregatedIter
    }
  }
}
