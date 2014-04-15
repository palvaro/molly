package edu.berkeley.cs.boom.molly.symmetry

import edu.berkeley.cs.boom.molly.FailureSpec
import com.codahale.metrics.MetricRegistry
import scala.collection.mutable
import edu.berkeley.cs.boom.molly.symmetry.SymmetryChecker._
import edu.berkeley.cs.boom.molly.ast.Program
import com.typesafe.scalalogging.slf4j.Logging
import nl.grons.metrics.scala.InstrumentedBuilder


class SymmetryAwareMap[V](program: Program)(implicit val metricRegistry: MetricRegistry)
  extends Logging with InstrumentedBuilder {


  private type SpecHash = (Set[(Int, Int)], Set[(Int, Int)])
  private val backingMap = mutable.HashMap[SpecHash, mutable.Set[(FailureSpec, V)]]()
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

  def get(f: FailureSpec): Option[V] = {
    val hash = hashSpec(f)
    val possibleMatches = backingMap.get(hash)
    if (possibleMatches.isEmpty) {
      logger.debug(s"No candidates for symmetry for $f")
      None
    } else {
      logger.debug(s"Found ${possibleMatches.get.size} possible symmetric scenarios for $f:\n${possibleMatches.get.map(_._1.toString()).mkString("\n")}")
      for ((cachedSpec, cachedValue) <- possibleMatches.get) {
        if (areEquivalentForEDB(program)(f, cachedSpec)) {
          logger.debug(s"Found scenario $cachedSpec that is symmetric to $f")
          symmetricScenariosSkipped.inc()
          return Some(cachedValue)
        }
      }
      None
    }
  }

  // TODO: handle duplicate insertions / updates
  def put(f: FailureSpec, v: V) {
    val hash = hashSpec(f)
    val bucket = backingMap.get(hash)
    if (bucket.isEmpty) {
      backingMap.put(hash, mutable.Set(f -> v))
    } else {
      bucket.get += f -> v
    }
  }

  def getOrElseUpdate(f: FailureSpec, default: => V): V = {
    get(f) match {
      case Some(v) => v
      case None =>
        put(f, default)
        default
    }
  }

}
