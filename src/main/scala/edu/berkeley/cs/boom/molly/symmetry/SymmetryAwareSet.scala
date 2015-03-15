package edu.berkeley.cs.boom.molly.symmetry

import edu.berkeley.cs.boom.molly.FailureSpec
import com.codahale.metrics.MetricRegistry
import scala.collection.mutable
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics.scala.InstrumentedBuilder


class SymmetryAwareSet(symmetryChecker: SymmetryChecker)(implicit val metricRegistry: MetricRegistry)
  extends mutable.Set[FailureSpec] with LazyLogging with InstrumentedBuilder {

  private type SpecHash = (Set[(Int, Int)], Set[(Int, Int)], Set[Int])
  private val backingMap = mutable.HashMap[SpecHash, mutable.Set[FailureSpec]]()
  private val symmetricScenariosSkipped = metrics.counter("symmetric-scenarios-skipped")

  /**
   * To avoid all-pairs comparisons, we partition the cache based on the number of
   * crash failures and message omissions at different timesteps.  As in hash tables,
   * we want few entries per bucket.
   */
  private def hashSpec(f: FailureSpec): SpecHash = {
    val crashesByTime = f.crashes.seq.map(c => c.time).groupBy(identity).mapValues(_.size).toSet
    val omissionsByTime = f.omissions.seq.map(_.time).groupBy(identity).mapValues(_.size).toSet
    val omissionPairCounts = f.omissions.groupBy(o => (o.from, o.to)).values.map(_.size).toSet
    (crashesByTime, omissionsByTime, omissionPairCounts)
  }

  def contains(f: FailureSpec): Boolean = {
    val hash = hashSpec(f)
    val possibleMatches = backingMap.get(hash)
    if (possibleMatches.isEmpty) {
      logger.debug(s"No candidates for symmetry for $f")
      false
    } else {
      logger.info(s"Found ${possibleMatches.get.size} possible symmetric scenarios for $f.")
      logger.debug(s"Possible symmetries are: \n${possibleMatches.get.map(_.toString).mkString("\n")}")
      for (cachedSpec <- possibleMatches.get) {
        if (cachedSpec == f) {
          return true
        } else if (symmetryChecker.areEquivalentForEDB(f, cachedSpec)) {
          logger.info(s"Found scenario $cachedSpec that is symmetric to $f")
          symmetricScenariosSkipped.inc()
          return true
        }
      }
      false
    }
  }

  override def +=(elem: FailureSpec) = {
    val hash = hashSpec(elem)
    val bucket = backingMap.get(hash)
    if (bucket.isEmpty) {
      backingMap.put(hash, mutable.Set(elem))
    } else {
      bucket.get += elem
    }
    this
  }

  override def clear(): Unit = {
    backingMap.clear()
  }

  override def seq = {
    throw new NotImplementedError("SymmetryAwareSet doesn't support seq")
  }

  override def iterator = {
    throw new NotImplementedError("SymmetryAwareSet doesn't support iterator")
  }

  override def -=(elem: FailureSpec) = {
    throw new NotImplementedError("SymmetryAwareSet doesn't support deletions")
  }

  override def empty = {
    throw new NotImplementedError("SymmetryAwareSet doesn't support empty")
  }
}
