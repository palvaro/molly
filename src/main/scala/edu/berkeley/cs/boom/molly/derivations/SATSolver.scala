package edu.berkeley.cs.boom.molly.derivations

import org.sat4j.minisat.SolverFactory
import java.util.concurrent.atomic.AtomicInteger
import edu.berkeley.cs.boom.molly.FailureSpec
import scala.collection.mutable
import org.sat4j.specs.IVecInt
import org.sat4j.core.VecInt
import com.typesafe.scalalogging.slf4j.Logging

object SATSolver extends Logging {
  private val nextVar = new AtomicInteger(1)
  private val idToSatVariable = mutable.HashMap[Int, SATVariable]()
  private val satVariableToId = mutable.HashMap[SATVariable, Int]()

  private implicit def satVarToInt(satVar: SATVariable): Int = satVar.id
  private implicit def satVarsToVecInt(clause: Iterable[SATVariable]): IVecInt =
    new VecInt(clause.map(_.id).toArray)

  sealed trait SATVariable extends Product {
    val id = satVariableToId.getOrElseUpdate(this, nextVar.incrementAndGet())
    idToSatVariable(id) = this
  }
  case class CrashFailure(node: String, time: Int) extends SATVariable
  case class NeverCrashed(node: String) extends SATVariable
  case class MessageLoss(from: String, to: String, time: Int) extends SATVariable

  /**
   * @param failureSpec a description of failures.
   * @param goals a list of goals whose derivations we'll attempt to falsify
   * @param seed a set of message failures that we already know have occurred,
   *             e.g. from previous runs.
   */
  def solve(failureSpec: FailureSpec, goals: List[GoalNode], seed: Set[MessageLoss] = Set.empty):
    Set[SATVariable] = {
    val solver = SolverFactory.newLight()

    // Basic variable creation and constraints

    // Crash failures:
    for (node <- failureSpec.nodes) {
      // Create one variable for every time at which the node could crash:
      val crashVars = (0 to failureSpec.eff).map(t => CrashFailure(node, t))
      // An extra variable for scenarios where the node didn't crash:
      val neverCrashed = NeverCrashed(node)
      // Each node crashes at a single time, or never crashes:
      solver.addExactly(crashVars ++ Seq(neverCrashed), 1)
    }
    // If there are at most C crashes, then at least (N - C) nodes never crash:
    solver.addExactly(failureSpec.nodes.map(NeverCrashed), failureSpec.nodes.size - failureSpec.crashes)

    // Message losses:
    for (
      goal <- goals;
      derivation <- goal.enumerateDistinctDerivations;
      importantClocks = derivation.importantClocks;
      // Only allow failures before the EFF:
      failures = importantClocks.filter(_._3 < failureSpec.eff)
      if !failures.isEmpty
    ) {
      logger.debug(s"Goal ${goal.tuple} has possible failures $failures")
      // The message could be missing due to a message loss or due to its
      // sender having crashed at an earlier timestamp:
      val messageLosses = failures.map(MessageLoss.tupled)
      val crashes = failures.flatMap { case (from, _, time) =>
        val crashTimes = 0 to time
        crashTimes.map ( t => CrashFailure(from, t))
      }
      solver.addClause(messageLosses ++ crashes)
    }

    // Assume any message losses that have already occurred:
    val assumptions = seed

    if (solver.isSatisfiable(assumptions)) {
      val model = solver.model()
      val trueVars = model.filter(_ > 0).map(idToSatVariable).toSet
      logger.info("SAT problem has model: " + trueVars)
      trueVars
    } else {
      logger.info("SAT problem is unsatisfiable")
      Set.empty
    }
  }
}
