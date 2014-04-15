package edu.berkeley.cs.boom.molly.symmetry

import edu.berkeley.cs.boom.molly.ast.Program
import edu.berkeley.cs.boom.molly.FailureSpec
import edu.berkeley.cs.boom.molly.symmetry.SymmetryChecker._
import scalaz.Memo
import com.typesafe.scalalogging.slf4j.Logging
import com.codahale.metrics.MetricRegistry
import nl.grons.metrics.scala.InstrumentedBuilder

// Note: this assumes that all FailureSpecs differ only in crashes and omissions, not in EFF, etc.

private class SymmetryAwareMemo[V](program: Program, failureSpec: FailureSpec)
                                  (implicit val metricRegistry: MetricRegistry)
  extends Logging with InstrumentedBuilder {

  private val cache = new SymmetryAwareMap[V](program)

  def apply(computeV: FailureSpec => V): FailureSpec => V = {
    def inner(failureSpec: FailureSpec): V = {
     cache.getOrElseUpdate(failureSpec, computeV(failureSpec))
    }
    Memo.mutableHashMapMemo(inner)  // Extra memo here handles exact matches
  }
}

object SymmetryAwareMemo extends Logging {
  def apply[V](program: Program, failureSpec: FailureSpec)
              (implicit metricRegistry: MetricRegistry) = {
    if (locationLiteralsOnlyOccurInEDB(program, failureSpec.nodes)) {
      new SymmetryAwareMemo[V](program, failureSpec).apply _
    } else {
      logger.warn("Program contains location literals in rule bodies; skipping symmetry optimizations")
      Memo.mutableHashMapMemo[FailureSpec, V].apply _
    }
  }
}
