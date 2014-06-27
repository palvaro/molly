package edu.berkeley.cs.boom.molly.derivations

import edu.berkeley.cs.boom.molly.FailureSpec
import com.codahale.metrics.MetricRegistry
import nl.grons.metrics.scala.{MetricName, MetricBuilder}
import scala.annotation.tailrec
import scala.language.implicitConversions
import com.typesafe.scalalogging.slf4j.Logging

sealed trait SolverVariable
case class CrashFailure(node: String, time: Int) extends SolverVariable
case class NeverCrashed(node: String) extends SolverVariable
case class MessageLoss(from: String, to: String, time: Int) extends SolverVariable {
  require (from != to, "Can't lose messages sent to self")
}
case class Not(v: SolverVariable) extends SolverVariable


trait Solver extends Logging {

  /**
   * @param failureSpec a description of failures.
   * @param goals a list of goals whose derivations we'll attempt to falsify
   * @param messages a list of messages sent during the program's execution
   * @param seed a set of message failures and crashes that we already know have occurred,
   *             e.g. from previous runs.
   * @return all solutions to the SAT problem, formulated as failure specifications
   */
  def solve(failureSpec: FailureSpec, goals: List[GoalNode], messages: Seq[Message],
            seed: Set[SolverVariable] = Set.empty)(implicit metricRegistry: MetricRegistry):
  Set[FailureSpec] = {
    implicit val metrics = new MetricBuilder(MetricName(getClass), metricRegistry)
    val firstMessageSendTimes =
      messages.groupBy(_.from).mapValues(_.minBy(_.sendTime).sendTime)
    val models = goals.flatMap{ goal => solve(failureSpec, goal, firstMessageSendTimes, seed)}.toSet
    logger.info(s"Problem has ${models.size} solutions")
    logger.debug(s"Solutions are:\n${models.map(_.toString()).mkString("\n")}")
    // Keep only the minimal models by excluding models that are supersets of other models.
    // The naive approach is O(N^2).
    // There are two simple optimizations that help:
    //    - A model can be a superset of MANY smaller models, so exclude it as soon as
    //      we find the first subset.
    //    - A model can only be a superset of smaller models, so group the models by size.
    def isSuperset[T](superset: Set[T], set: Set[T]): Boolean = set.forall(e => superset.contains(e))
    val modelsBySize = models.groupBy(_.size).toSeq.sortBy(- _._1) // minus sign -> descending sizes
    logger.debug(s"Non minimal models by size: ${modelsBySize.map(x => (x._1, x._2.size))}")
    @tailrec
    def removeSupersets(modelsBySize: Seq[(Int, Set[Set[SolverVariable]])],
                        accum: Seq[Set[SolverVariable]] = Seq.empty): Seq[Set[SolverVariable]] = {
      if (modelsBySize.isEmpty) {
        accum
      } else {
        val smallerModels: Seq[Set[SolverVariable]] = modelsBySize.tail.map(_._2).flatten
        val minimalModels = modelsBySize.head._2.toSeq.filterNot {
          sup => smallerModels.exists(sub => isSuperset(sup, sub))
        }
        removeSupersets(modelsBySize.tail, minimalModels ++ accum)
      }
    }
    val minimalModels = removeSupersets(modelsBySize)
    logger.info(s"SAT problem has ${minimalModels.size} minimal solutions")
    logger.debug(s"Minimal SAT solutions are:\n${minimalModels.map(_.toString()).mkString("\n")}")

    minimalModels.flatMap { vars =>
      val crashes = vars.collect { case cf: CrashFailure => cf }
      // If the seed contained a message loss, then it's possible that the SAT solver found
      // a solution where that message's sender crashes before that message loss.
      // Such message losses are redundant, so we'll remove them:
      def subsumedByCrash(ml: MessageLoss) =
        crashes.collectFirst {
          case cf @ CrashFailure(ml.from, t) if t <= ml.time => cf
          case cf @ CrashFailure(ml.to, t) if t + 1 >= ml.time => cf
        }.isDefined
      val omissions = vars.collect { case ml: MessageLoss => ml }.filterNot(subsumedByCrash)
      if (crashes.isEmpty && omissions.isEmpty) {
        None
      } else {
        Some(failureSpec.copy (crashes = crashes, omissions = omissions))
      }
    }.toSet
  }

  protected def solve(failureSpec: FailureSpec, goal: GoalNode,
                      firstMessageSendTimes: Map[String, Int], seed: Set[SolverVariable])
                     (implicit metrics: MetricBuilder): Traversable[Set[SolverVariable]]
}
