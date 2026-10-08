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

package org.apache.spark.lineage.rdd

import org.apache.spark._
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.rdd._

import scala.language.existentials
import scala.reflect._

/**
 * :: DeveloperApi ::
 * A RDD that cogroups its parents. For each key k in parent RDDs, the resulting RDD contains a
 * tuple with the list of values for that key.
 *
 * This is an internal API. Use RDD.cogroup instead of instantiating it directly.
 *
 * @param lrdds parent RDDs.
 * @param part partitioner used to partition the shuffle output
 */

class CoGroupedLRDD[K: ClassTag](var lrdds: Seq[RDD[_ <: Product2[K, _]]], part: Partitioner)
  extends CoGroupedRDD[K](lrdds, part) with Lineage[(K, Array[Iterable[_]])] {

  override def lineageContext = lrdds.head.lineageContext

  override def ttag: ClassTag[(K, Array[Iterable[_]])] = classTag[(K, Array[Iterable[_]])]

  // A queue, so that successive tapLeft calls wrap the parents in dependency order. Tap i
  // must feed shuffle dependency i, or the cogroup value slots come out crossed.
  private[spark] var newDeps = new scala.collection.mutable.Queue[Dependency[_]]

  private[spark] def computeTapDependencies() =
    dependencies.foreach(dep => newDeps.enqueue(new OneToOneDependency(dep.rdd)))

  override def tapRight(): TapLRDD[(K, Array[Iterable[_]])] = {
    val tap = new TapPostCoGroupLRDD[(K, Array[Iterable[_]])](
      lineageContext, Seq(new OneToOneDependency[(K, Array[Iterable[_]])](this)))
    setTap(tap)
    setCaptureLineage(true)
    tap.setCached(this)
  }

  override def tapLeft(): TapLRDD[(K, Array[Iterable[_]])] = {
    if(newDeps.isEmpty) computeTapDependencies
    new TapPreCoGroupLRDD[(K, Array[Iterable[_]])](lineageContext, Seq(newDeps.dequeue()))
      .setCached(this)
  }
}