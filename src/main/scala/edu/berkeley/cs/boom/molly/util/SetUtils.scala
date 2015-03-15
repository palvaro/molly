package edu.berkeley.cs.boom.molly.util

import scala.annotation.tailrec

object SetUtils {

  /**
   * @return true iff the first set is a (non-strict) superset of the second set.
   */
  def isSuperset[T](superset: Set[T], set: Set[T]): Boolean = {
    set.forall(e => superset.contains(e))
  }

  /**
   * Given a sequence of sets, returns a new sequence containing only minimal sets, sets
   * which are not supersets of other sets.
   */
  def minimalSets[T](sets: Seq[Set[T]]): Seq[Set[T]] = {
    // The naive approach to this is O(N^2).
    // There are two simple optimizations that help:
    //    - A set can be a superset of MANY smaller sets, so exclude it as soon as
    //      we find the first subset.
    //    - A set can only be a superset of smaller sets, so group the sets by size.å
    val setsBySize = sets.groupBy(_.size).toSeq.sortBy(- _._1) // minus sign -> descending sizes
    @tailrec
    def removeSupersets(
        setsBySize: Seq[(Int, Seq[Set[T]])],
        accum: Seq[Set[T]] = Seq.empty): Seq[Set[T]] = {
      if (setsBySize.isEmpty) {
        accum
      } else {
        val smallerModels: Seq[Set[T]] = setsBySize.tail.map(_._2).flatten
        val minimalSets = setsBySize.head._2.toSeq.filterNot {
          sup => smallerModels.exists(sub => isSuperset(sup, sub))
        }
        removeSupersets(setsBySize.tail, minimalSets ++ accum)
      }
    }
    removeSupersets(setsBySize)
  }
}
