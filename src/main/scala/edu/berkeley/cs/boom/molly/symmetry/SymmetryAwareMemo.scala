package edu.berkeley.cs.boom.molly.symmetry

import edu.berkeley.cs.boom.molly.ast.Program
import edu.berkeley.cs.boom.molly.FailureSpec
import edu.berkeley.cs.boom.molly.symmetry.SymmetryChecker._
import scalaz.Memo
import scala.collection.mutable
import com.typesafe.scalalogging.slf4j.Logging
import com.codahale.metrics.MetricRegistry
import nl.grons.metrics.scala.InstrumentedBuilder

// Note: this assumes that all FailureSpecs differ only in crashes and omissions, not in EFF, etc.

private class SymmetryAwareMemo[V](program: Program, failureSpec: FailureSpec)
                                  (implicit val metricRegistry: MetricRegistry)
  extends Logging with InstrumentedBuilder {


  private type SpecHash = (Set[(Int, Int)], Set[(Int, Int)])
  private val cache = mutable.HashMap[SpecHash, Set[(FailureSpec, V)]]()
  private val symmetricScenariosSkipped = metrics.counter("symmetric-scenarios-skipped")

  /**
   * To avoid all-pairs comparisons, we partition the cache based on the number of
   * crash failures and message omissions at different timesteps:
   */
  private def hashSpec(f: FailureSpec): SpecHash = {
    val crashesByTime = f.crashes.seq.map(c => c.time).groupBy(identity).mapValues(_.size).toSet
    val omissionsByTime = f.omissions.seq.map(_.time).groupBy(identity).mapValues(_.size).toSet
    (crashesByTime, omissionsByTime)
  }

  def apply(computeV: FailureSpec => V): FailureSpec => V = {
    def inner(failureSpec: FailureSpec): V = {
      val hash = hashSpec(failureSpec)
      val possibleMatches = cache.get(hash)
      if (possibleMatches.isEmpty) {
        logger.debug(s"No candidates for symmetry for $failureSpec")
        val v = computeV(failureSpec)
        cache.put(hash, Set(failureSpec -> v))
        v
      } else {
        logger.debug(s"Found ${possibleMatches.get.size} possible symmetric scenarios for $failureSpec:\n${possibleMatches.get.map(_._1.toString()).mkString("\n")}")
        for ((cachedSpec, cachedValue) <- possibleMatches.get) {
          if (areEquivalentForEDB(program)(failureSpec, cachedSpec)) {
            logger.debug(s"Found scenario $cachedSpec that is symmetric to $failureSpec")
            symmetricScenariosSkipped.inc()
            return cachedValue
          }
        }
        logger.debug(s"No candidate scenarios were symmetric to $failureSpec")
        val v = computeV(failureSpec)
        cache(hash) += failureSpec -> v
        v
      }
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
