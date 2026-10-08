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

package org.apache.spark

/**
 * Record-level data provenance for the RDD API, running on stock Spark.
 *
 * ==Usage==
 * Wrap a `SparkContext` in a [[org.apache.spark.lineage.LineageContext]], turn capture
 * on, run a job through the lineage-aware RDD operators, and then trace.
 * {{{
 * val lc = new LineageContext(sc)
 * lc.setCaptureLineage(true)
 * val counts = lc.textFile(path).map(parse).reduceByKey(_ + _)
 * val out = counts.collectWithId()
 * lc.setCaptureLineage(false)
 *
 * // Trace an aggregate back to its source records.
 * counts.getLineage().filter(_ == suspectId).goBack().goBack().show().collect()
 * }}}
 *
 * ==Mechanism==
 * Capture inserts tap RDDs at stage boundaries by splicing the job's dependency DAG
 * ([[org.apache.spark.lineage.LineageContext.tapJob]]). Each tap records compact
 * `(inputId, outputId)` associations keyed by a murmur key hash
 * ([[org.apache.spark.lineage.LineageHashing]]). The lineage-aware aggregator
 * ([[org.apache.spark.lineage.LAggregator]]) carries the tag through map-side combine.
 * Traces use a distributed selective join, routing the trace cursor by partition id and
 * joining with `zipPartitions`, so they never reshuffle the data.
 *
 * ==Tuning==
 * Five capture and trace optimizations are on by default. Each one can be switched off by
 * naming its flag in the comma-separated `spark.prism.ablation` setting, which
 * `PrismAblation` reads. Every fallback path produces identical lineage.
 *
 *   - `legacyHash` hashes the bytes of `key.toString` instead of passing
 *     `key.hashCode` through `Murmur3.hashInt`.
 *   - `boxedCombiner` allocates a `Tuple2` per map-side merge instead of using the
 *     mutable tagged carrier in [[LAggregator]].
 *   - `boxedJoinBuffer` uses an `ArrayBuffer` of boxed tuples in the broadcast-join tap
 *     instead of a primitive `long` buffer.
 *   - `boxedBlocks` stores one boxed tuple per row instead of compact primitive-array
 *     lineage blocks ([[org.apache.spark.sql.lineage.TapBlock]]). The block storage level
 *     is `spark.prism.lineage.storageLevel`, with default `MEMORY_AND_DISK_SER`.
 *   - `driverTrace` collects whole lineage tables to the driver instead of filtering
 *     the blocks on executors with block-locality scheduling.
 *
 * The SQL / DataFrame counterpart is [[org.apache.spark.sql.lineage]].
 */
package object lineage
