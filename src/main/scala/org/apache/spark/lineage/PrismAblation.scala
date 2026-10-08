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

import org.apache.spark.SparkEnv

/**
 * Performance-tuning flags. Every capture and trace optimization is on by default, and
 * `spark.prism.ablation` switches individual ones back to the unoptimized path. All
 * paths produce identical lineage.
 *
 * `spark.prism.ablation` is a comma-separated list of these flags.
 *  - `legacyHash` hashes the bytes of key.toString on the RDD side.
 *  - `boxedCombiner` allocates a Tuple2 per combine instead of a mutable TaggedCombiner.
 *  - `boxedJoinBuffer` uses a boxed tuple ArrayBuffer in the broadcast-join tap.
 *  - `boxedBlocks` stores one boxed tuple per row in lineage blocks.
 *  - `driverTrace` collects whole blocks to the driver and filters there, without
 *    block-locality preferred locations.
 *
 * The setting is fixed when the application or session starts. After the first call per
 * SparkEnv, a lookup costs two volatile reads.
 */
private[spark] object PrismAblation {

  @volatile private var cachedFor: SparkEnv = _
  @volatile private var cached: Set[String] = Set.empty

  private def flags: Set[String] = {
    val env = SparkEnv.get
    if (env ne cachedFor) {
      cached =
        if (env == null) Set.empty
        else env.conf.get("spark.prism.ablation", "")
          .split(",").map(_.trim).filter(_.nonEmpty).toSet
      cachedFor = env
    }
    cached
  }

  def legacyHash: Boolean = flags.contains("legacyHash")
  def boxedCombiner: Boolean = flags.contains("boxedCombiner")
  def boxedJoinBuffer: Boolean = flags.contains("boxedJoinBuffer")
  def boxedBlocks: Boolean = flags.contains("boxedBlocks")
  def driverTrace: Boolean = flags.contains("driverTrace")
}
