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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Cast, Expression, UnsafeProjection, UnsafeRow, XXH64}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{CodegenSupport, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.{DoubleType, LongType}

/**
 * Base for Prism tap operators in SQL plans. Taps are row pass-throughs that join
 * whole-stage codegen, so per-record capture is one method call on a per-task runtime
 * object fused into the generated loop. Each tap also implements the interpreted path
 * for stages where codegen falls back.
 */
trait PrismTapExec extends UnaryExecNode with CodegenSupport {
  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[org.apache.spark.sql.catalyst.expressions.SortOrder] =
    child.outputOrdering

  override def inputRDDs(): Seq[RDD[InternalRow]] =
    child.asInstanceOf[CodegenSupport].inputRDDs()

  override def doProduce(ctx: CodegenContext): String =
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
}

/**
 * Assigns the per-task input row id as rows leave the scan. It sits directly above
 * each row-based leaf scan.
 */
case class TapScanExec(child: SparkPlan) extends PrismTapExec {

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val tap = ctx.addMutableState(
      classOf[ScanTapRuntime].getName, "prismScanTap",
      v => s"$v = new ${classOf[ScanTapRuntime].getName}();")
    s"""
       |$tap.tap();
       |${consume(ctx, input)}
     """.stripMargin
  }

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val tap = new ScanTapRuntime()
      iter.map { r => tap.tap(); r }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): TapScanExec =
    copy(child = newChild)
}

/**
 * 64-bit hash of a row's full projection. A join's post tap and the pre taps below its
 * inputs hash the same attributes, so equal hashes identify the input row a match used.
 */
object RowHash {
  def of(r: UnsafeRow): Long =
    XXH64.hashUnsafeBytes(r.getBaseObject, r.getBaseOffset, r.getSizeInBytes, 42L)

  /** Generated code that evaluates the row hash of `attrs` over the current input. */
  def code(ctx: CodegenContext, input: Seq[ExprCode], attrs: Seq[Expression],
      childOutput: Seq[Attribute]): (String, String) = {
    ctx.currentVars = input
    val ev = GenerateUnsafeProjection.createCode(
      ctx, BindReferences.bindReferences[Expression](attrs, childOutput))
    val h = ctx.freshName("prismRowHash")
    (s"""
        |${ev.code}
        |long $h = ${RowHash.getClass.getName.stripSuffix("$")}.of(${ev.value});
      """.stripMargin, h)
  }
}

/**
 * Base for taps that hash a key projection per row (pre-exchange and post-keyed).
 * The hash is the murmur3 `UnsafeRow.hashCode` of the projected key, which is stable for
 * equal values and allocates no strings.
 */
trait KeyedTapExec extends PrismTapExec {
  def keyExprs: Seq[Expression]
  def tapId: Int
  protected def runtimeClass: Class[_]

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val tap = ctx.addMutableState(
      runtimeClass.getName, "prismKeyedTap",
      v => s"$v = new ${runtimeClass.getName}($tapId);")
    ctx.currentVars = input
    val boundKeys = BindReferences.bindReferences[Expression](keyExprs, child.output)
    val keyEv = GenerateUnsafeProjection.createCode(ctx, boundKeys)
    s"""
       |${keyEv.code}
       |$tap.tap(${keyEv.value}.hashCode());
       |${consume(ctx, input)}
     """.stripMargin
  }

  protected def interpretedTap(tapper: (Any, Int) => Unit): RDD[InternalRow] = {
    val keys = keyExprs
    val childOutput = child.output
    child.execute().mapPartitionsInternal { iter =>
      val proj = UnsafeProjection.create(keys, childOutput)
      val tap = runtimeClass.getConstructor(classOf[Int])
        .newInstance(Integer.valueOf(tapId))
      iter.map { r => tapper(tap, proj(r).hashCode()); r }
    }
  }
}

/**
 * Map-side capture below an exchange. It maps each key hash to a bitmap of input ids.
 * When `valueExprs` (the inputs of the query's aggregates) is set, it also records each
 * row's contribution to every aggregate of its group in the same pass. See
 * [[InfluencePreExchangeTapRuntime]].
 */
case class TapPreExchangeExec(
    tapId: Int, keyExprs: Seq[Expression], child: SparkPlan,
    valueExprs: Seq[Expression] = Nil,
    focus: Option[Expression] = None,
    rowHash: Boolean = false)
  extends KeyedTapExec {

  // `rowHash` marks a join input. Each row then also records the hash of its full
  // projection, which lets a trace select exact matches. See [[RowPreExchangeTapRuntime]].
  override protected def runtimeClass: Class[_] =
    if (valueExprs.nonEmpty) classOf[InfluencePreExchangeTapRuntime]
    else if (rowHash) classOf[RowPreExchangeTapRuntime]
    else classOf[PreExchangeTapRuntime]

  // When `focus` is set (a Boolean predicate on the group key, resolved against
  // child.output), a row records lineage only if the predicate holds. The row always
  // flows downstream unchanged.
  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    // The row hash reads every input column, so the inputs are evaluated once up front.
    val inputsEval = if (rowHash && valueExprs.isEmpty) evaluateVariables(input) else ""
    ctx.currentVars = input
    val boundKeys = BindReferences.bindReferences[Expression](keyExprs, child.output)
    val keyEv = GenerateUnsafeProjection.createCode(ctx, boundKeys)
    val record = if (valueExprs.isEmpty && rowHash) {
      val tap = ctx.addMutableState(runtimeClass.getName, "prismRowTap",
        v => s"$v = new ${runtimeClass.getName}($tapId);")
      val (rowCode, h) = RowHash.code(ctx, input, child.output, child.output)
      s"""
         |$inputsEval
         |${keyEv.code}
         |$rowCode
         |$tap.tap(${keyEv.value}.hashCode(), $h);
       """.stripMargin
    } else if (valueExprs.isEmpty) {
      val tap = ctx.addMutableState(runtimeClass.getName, "prismKeyedTap",
        v => s"$v = new ${runtimeClass.getName}($tapId);")
      s"""
         |${keyEv.code}
         |$tap.tap(${keyEv.value}.hashCode());
       """.stripMargin
    } else {
      val n = valueExprs.length
      val tap = ctx.addMutableState(runtimeClass.getName, "prismInflTap",
        s => s"$s = new ${runtimeClass.getName}($tapId, $n);")
      val arr = ctx.freshName("prismVals")
      val sets = valueExprs.zipWithIndex.map { case (v, i) =>
        ctx.currentVars = input
        val ev = BindReferences.bindReference(Cast(v, DoubleType), child.output).genCode(ctx)
        s"""
           |${ev.code}
           |$arr[$i] = ${ev.isNull} ? Double.NaN : ${ev.value};
         """.stripMargin
      }.mkString
      s"""
         |${keyEv.code}
         |double[] $arr = new double[$n];
         |$sets
         |$tap.tap(${keyEv.value}.hashCode(), $arr);
       """.stripMargin
    }
    val gated = focus match {
      case Some(pred) =>
        ctx.currentVars = input
        val fe = BindReferences.bindReference(pred, child.output).genCode(ctx)
        s"""
           |${fe.code}
           |if (!${fe.isNull} && ${fe.value}) {
           |$record
           |}
         """.stripMargin
      case None => record
    }
    s"""
       |$gated
       |${consume(ctx, input)}
     """.stripMargin
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val keys = keyExprs
    val childOutput = child.output
    val tid = tapId
    val focusExpr = focus
    if (valueExprs.isEmpty && rowHash) {
      child.execute().mapPartitionsInternal { iter =>
        val proj = UnsafeProjection.create(keys, childOutput)
        val rproj = UnsafeProjection.create(childOutput, childOutput)
        val tap = new RowPreExchangeTapRuntime(tid)
        iter.map { r => tap.tap(proj(r).hashCode(), RowHash.of(rproj(r))); r }
      }
    } else if (valueExprs.isEmpty) {
      child.execute().mapPartitionsInternal { iter =>
        val proj = UnsafeProjection.create(keys, childOutput)
        val pred = focusExpr.map(f => org.apache.spark.sql.catalyst.expressions
          .Predicate.create(f, childOutput))
        val tap = new PreExchangeTapRuntime(tid)
        iter.map { r => if (pred.forall(_.eval(r))) tap.tap(proj(r).hashCode()); r }
      }
    } else {
      val vexprs = valueExprs.map(v => Cast(v, DoubleType))
      val n = vexprs.length
      child.execute().mapPartitionsInternal { iter =>
        val kproj = UnsafeProjection.create(keys, childOutput)
        val vproj = UnsafeProjection.create(vexprs, childOutput)
        val pred = focusExpr.map(f => org.apache.spark.sql.catalyst.expressions
          .Predicate.create(f, childOutput))
        val tap = new InfluencePreExchangeTapRuntime(tid, n)
        iter.map { r =>
          if (pred.forall(_.eval(r))) {
            val vr = vproj(r)
            val arr = new Array[Double](n)
            var i = 0
            while (i < n) { arr(i) = if (vr.isNullAt(i)) Double.NaN else vr.getDouble(i); i += 1 }
            tap.tap(kproj(r).hashCode(), arr)
          }
          r
        }
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): TapPreExchangeExec =
    copy(child = newChild)
}

/**
 * Reduce-side capture above a final aggregate or join. Maps each packed output index
 * to its key hash.
 */
case class TapPostKeyedExec(tapId: Int, keyExprs: Seq[Expression], child: SparkPlan)
  extends KeyedTapExec {

  override protected def runtimeClass: Class[_] = classOf[PostKeyedTapRuntime]

  override protected def doExecute(): RDD[InternalRow] =
    interpretedTap((tap, h) => tap.asInstanceOf[PostKeyedTapRuntime].tap(h))

  override protected def withNewChildInternal(newChild: SparkPlan): TapPostKeyedExec =
    copy(child = newChild)
}

/**
 * Above a sort-merge or shuffled hash join, records each output row's join key hash and
 * the row hashes of its left and right inputs. `leftAttrs` and `rightAttrs` are the
 * join's input schemas, the same attributes the pre taps below its exchanges hash.
 */
case class TapPostShuffleJoinExec(
    tapId: Int, keyExprs: Seq[Expression], leftAttrs: Seq[Attribute],
    rightAttrs: Seq[Attribute], child: SparkPlan)
  extends KeyedTapExec {

  override protected def runtimeClass: Class[_] = classOf[PostShuffleJoinTapRuntime]

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val tap = ctx.addMutableState(
      runtimeClass.getName, "prismJoinTap",
      v => s"$v = new ${runtimeClass.getName}($tapId);")
    val inputsEval = evaluateVariables(input)
    ctx.currentVars = input
    val boundKeys = BindReferences.bindReferences[Expression](keyExprs, child.output)
    val keyEv = GenerateUnsafeProjection.createCode(ctx, boundKeys)
    val (lCode, lh) = RowHash.code(ctx, input, leftAttrs, child.output)
    val (rCode, rh) = RowHash.code(ctx, input, rightAttrs, child.output)
    s"""
       |$inputsEval
       |${keyEv.code}
       |$lCode
       |$rCode
       |$tap.tap(${keyEv.value}.hashCode(), $lh, $rh);
       |${consume(ctx, input)}
     """.stripMargin
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val (keys, la, ra, childOutput, tid) = (keyExprs, leftAttrs, rightAttrs, child.output, tapId)
    child.execute().mapPartitionsInternal { iter =>
      val kp = UnsafeProjection.create(keys, childOutput)
      val lp = UnsafeProjection.create(la, childOutput)
      val rp = UnsafeProjection.create(ra, childOutput)
      val tap = new PostShuffleJoinTapRuntime(tid)
      iter.map { r => tap.tap(kp(r).hashCode(), RowHash.of(lp(r)), RowHash.of(rp(r))); r }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): TapPostShuffleJoinExec =
    copy(child = newChild)
}

/**
 * Saves the probe row's id into the join's mark slot as it enters a BroadcastHashJoin.
 * A fan-out join emits matches depth-first, so downstream taps of the first match have
 * already overwritten `currentInputId` when the second match emerges.
 */
case class TapProbeMarkExec(pairId: Int, child: SparkPlan) extends PrismTapExec {

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val tap = ctx.addMutableState(
      classOf[ProbeMarkTapRuntime].getName, "prismProbeMarkTap",
      v => s"$v = new ${classOf[ProbeMarkTapRuntime].getName}($pairId);")
    s"""
       |$tap.tap();
       |${consume(ctx, input)}
     """.stripMargin
  }

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val tap = new ProbeMarkTapRuntime(pairId)
      iter.map { r => tap.tap(); r }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): TapProbeMarkExec =
    copy(child = newChild)
}

/**
 * Above a BroadcastHashJoin, maps each packed output index to the stream key hash and
 * probe input id. `keyExprs` must be the join's rewritten streamed keys so the hash
 * matches the build-side tap. The probe id comes from the mark slot written by the
 * [[TapProbeMarkExec]] with the same `pairId`.
 */
case class TapPostBroadcastJoinExec(
    tapId: Int, pairId: Int, keyExprs: Seq[Expression], child: SparkPlan,
    buildAttrs: Seq[Attribute] = Nil)
  extends KeyedTapExec {

  override protected def runtimeClass: Class[_] = classOf[PostJoinTapRuntime]

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val tap = ctx.addMutableState(
      runtimeClass.getName, "prismKeyedTap",
      v => s"$v = new ${runtimeClass.getName}($tapId, $pairId);")
    val inputsEval = if (buildAttrs.isEmpty) "" else evaluateVariables(input)
    ctx.currentVars = input
    val boundKeys = BindReferences.bindReferences[Expression](keyExprs, child.output)
    val keyEv = GenerateUnsafeProjection.createCode(ctx, boundKeys)
    // With `buildAttrs`, the tap also records the matched build row's hash.
    val (bCode, call) =
      if (buildAttrs.isEmpty) ("", s"$tap.tap(${keyEv.value}.hashCode());")
      else {
        val (c, h) = RowHash.code(ctx, input, buildAttrs, child.output)
        (c, s"$tap.tap(${keyEv.value}.hashCode(), $h);")
      }
    s"""
       |$inputsEval
       |${keyEv.code}
       |$bCode
       |$call
       |${consume(ctx, input)}
     """.stripMargin
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val keys = keyExprs
    val childOutput = child.output
    val build = buildAttrs
    val (tid, pid) = (tapId, pairId)
    child.execute().mapPartitionsInternal { iter =>
      val proj = UnsafeProjection.create(keys, childOutput)
      val bproj = if (build.isEmpty) null else UnsafeProjection.create(build, childOutput)
      val tap = new PostJoinTapRuntime(tid, pid)
      iter.map { r =>
        if (bproj == null) tap.tap(proj(r).hashCode())
        else tap.tap(proj(r).hashCode(), RowHash.of(bproj(r)))
        r
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): TapPostBroadcastJoinExec =
    copy(child = newChild)
}

/** Below a Python-UDF eval node, buffers the sequence of input ids (one per row). */
case class TapSeqExec(pairId: Int, child: SparkPlan) extends PrismTapExec {

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val tap = ctx.addMutableState(
      classOf[SeqTapRuntime].getName, "prismSeqTap",
      v => s"$v = new ${classOf[SeqTapRuntime].getName}($pairId);")
    s"""
       |$tap.tap();
       |${consume(ctx, input)}
     """.stripMargin
  }

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val tap = new SeqTapRuntime(pairId)
      iter.map { r => tap.tap(); r }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): TapSeqExec =
    copy(child = newChild)
}

/**
 * Above a prism Python-UDF eval, after its reseq tap restores currentInputId. Records
 * each row's provenance codes (the last subfield of each prism UDF's struct output),
 * keyed by row id, into per-partition blocks. A trace fetches only its witnesses' codes.
 */
case class TapUdfProvExec(tapId: Int, provExprs: Seq[Expression], child: SparkPlan)
  extends PrismTapExec {

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val n = provExprs.length
    val tap = ctx.addMutableState(
      classOf[UdfProvTapRuntime].getName, "prismProvTap",
      s => s"$s = new ${classOf[UdfProvTapRuntime].getName}($tapId, $n);")
    val arr = ctx.freshName("prismProv")
    val sets = provExprs.zipWithIndex.map { case (e, i) =>
      ctx.currentVars = input
      val ev = BindReferences.bindReference(Cast(e, LongType), child.output).genCode(ctx)
      s"""
         |${ev.code}
         |$arr[$i] = ${ev.isNull} ? 0L : ${ev.value};
       """.stripMargin
    }.mkString
    s"""
       |long[] $arr = new long[$n];
       |$sets
       |$tap.tap($arr);
       |${consume(ctx, input)}
     """.stripMargin
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val exprs = provExprs.map(e => Cast(e, LongType))
    val n = exprs.length
    val childOutput = child.output
    val tid = tapId
    child.execute().mapPartitionsInternal { iter =>
      val proj = UnsafeProjection.create(exprs, childOutput)
      val tap = new UdfProvTapRuntime(tid, n)
      iter.map { r =>
        val pr = proj(r)
        val a = new Array[Long](n)
        var i = 0
        while (i < n) { a(i) = if (pr.isNullAt(i)) 0L else pr.getLong(i); i += 1 }
        tap.tap(a)
        r
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): TapUdfProvExec =
    copy(child = newChild)
}

/** Above the Python-UDF eval node, replays the recorded input-id sequence. */
case class TapReseqExec(pairId: Int, child: SparkPlan) extends PrismTapExec {

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val tap = ctx.addMutableState(
      classOf[ReseqTapRuntime].getName, "prismReseqTap",
      v => s"$v = new ${classOf[ReseqTapRuntime].getName}($pairId);")
    s"""
       |$tap.tap();
       |${consume(ctx, input)}
     """.stripMargin
  }

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val tap = new ReseqTapRuntime(pairId)
      iter.map { r => tap.tap(); r }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): TapReseqExec =
    copy(child = newChild)
}

/**
 * Records each result row's packed (partition, output index) with its currentInputId
 * and materializes the association at task end. It sits at the root of the final stage.
 */
case class TapResultExec(tapId: Int, child: SparkPlan) extends PrismTapExec {

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val tap = ctx.addMutableState(
      classOf[ResultTapRuntime].getName, "prismResultTap",
      v => s"$v = new ${classOf[ResultTapRuntime].getName}($tapId);")
    s"""
       |$tap.tap();
       |${consume(ctx, input)}
     """.stripMargin
  }

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val tap = new ResultTapRuntime(tapId)
      iter.map { r => tap.tap(); r }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): TapResultExec =
    copy(child = newChild)
}
