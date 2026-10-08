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

package org.apache.spark.sql

/**
 * Record-level data provenance for Spark SQL / DataFrames, captured through
 * whole-stage codegen.
 *
 * ==Usage==
 * Register the extension and toggle capture per query:
 * {{{
 * val spark = SparkSession.builder()
 *   .config("spark.sql.extensions",
 *     "org.apache.spark.sql.lineage.PrismSQLExtension")
 *   .config("spark.prism.sql.capture", "true")
 *   .getOrCreate()
 *
 * val df = spark.sql("SELECT category, SUM(amount) FROM sales GROUP BY category")
 * val rowsWithIds = PrismSQL.collectWithLineage(df)         // (Row, packedId)
 *
 * // trace one output row back to its source rows
 * val cursor = PrismSQL.trace(df, Seq(rowsWithIds.head._2))
 * cursor.goBack().show()                                     // witness records
 *
 * PrismSQL.releaseLineage(df)                               // drop the blocks
 * }}}
 *
 * ==Entry points==
 *   - [[org.apache.spark.sql.lineage.PrismSQL]] is the driver-side API for capture,
 *     trace, footprint, and release.
 *   - [[org.apache.spark.sql.lineage.TraceCursor]] is a position in the
 *     backward/forward walk (`goBack(branch)`, `goNext`, `show`, `showAllSources`).
 *   - [[org.apache.spark.sql.lineage.PrismSQLExtension]] is the
 *     `SparkSessionExtensions` that injects the tap operators.
 *
 * ==Mechanism==
 * A columnar rule ([[org.apache.spark.sql.lineage.PrismSQLExtension]]) inserts tap
 * operators ([[org.apache.spark.sql.lineage.TapExec]]) into the physical plan before
 * `CollapseCodegenStages`, so each tap's per-row capture is fused into Spark's
 * generated Java. Taps record into compact primitive-array blocks
 * ([[org.apache.spark.sql.lineage.TapBlock]]) materialized in executor
 * `BlockManager`s. Traces resolve by an executor-side filtering walk over those
 * blocks, and source rows are recovered by deterministically re-scanning the inputs.
 *
 * Capture covers a defined operator set (scans, aggregates, the four shuffle/broadcast
 * join families, windows, `UNION ALL`, `ORDER BY ... LIMIT`, Python UDFs, cached
 * relations). Any other operator raises `PrismUnsupportedOperatorException` rather
 * than producing wrong lineage.
 *
 * The RDD counterpart is [[org.apache.spark.lineage]].
 */
package object lineage
