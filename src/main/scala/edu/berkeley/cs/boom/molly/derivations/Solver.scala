package edu.berkeley.cs.boom.molly.derivations

import edu.berkeley.cs.boom.molly.FailureSpec
import com.codahale.metrics.MetricRegistry
import edu.berkeley.cs.boom.molly.util.SetUtils
import nl.grons.metrics.scala.{MetricName, MetricBuilder}
import scala.annotation.tailrec
import scala.language.implicitConversions
import com.typesafe.scalalogging.slf4j.Logging

/**
 * SolverVariables are used to map message losses and failures into SAT / SMT formula variables.
 */
sealed trait SolverVariable
case class CrashFailure(node: String, time: Int) extends SolverVariable
case class NeverCrashed(node: String) extends SolverVariable
case class MessageLoss(from: String, to: String, time: Int) extends SolverVariable {
  require (from != to, "Can't lose messages sent to self")
}
case class Not(v: SolverVariable) extends SolverVariable

/**
 * Interface for pluggable SAT / SMT solver backends.
 */
trait Solver extends Logging {

  /**
   * Given the derivation of a good outcome, computes a set of potential falsifiers of that outcome.
   *
   * @param failureSpec a description of failures.
   * @param goals a list of goals whose derivations we'll attempt to falsify
   * @param messages a list of messages sent during the program's execution
   * @param seed a set of message failures and crashes that we already know have occurred,
   *             e.g. from previous runs.
   * @return all solutions to the SAT problem, formulated as failure specifications
   */
  def solve(
      failureSpec: FailureSpec,
      goals: List[GoalNode],
      messages: Seq[Message],
      seed: Set[SolverVariable] = Set.empty)
      (implicit metricRegistry: MetricRegistry): Set[FailureSpec] = {

    implicit val metrics = new MetricBuilder(MetricName(getClass), metricRegistry)

    val firstMessageSendTimes = messages.groupBy(_.from).mapValues(_.minBy(_.sendTime).sendTime)
    val models = goals.flatMap{ goal => solve(failureSpec, goal, firstMessageSendTimes, seed)}.toSet
    logger.info(s"Problem has ${models.size} solutions")
    logger.debug(s"Solutions are:\n${models.map(_.toString()).mkString("\n")}")
    val minimalModels = SetUtils.minimalSets(models.toSeq)
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

  /**
   * Solver method implemented by subclasses.
   */
  protected def solve(
      failureSpec: FailureSpec,
      goal: GoalNode,
      firstMessageSendTimes: Map[String, Int],
      seed: Set[SolverVariable])
      (implicit metrics: MetricBuilder): Traversable[Set[SolverVariable]]
}
