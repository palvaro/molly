package edu.berkeley.cs.boom.molly.derivations

import org.sat4j.minisat.SolverFactory
import edu.berkeley.cs.boom.molly.FailureSpec
import scala.collection.mutable
import org.sat4j.specs.IVecInt
import org.sat4j.core.VecInt
import org.sat4j.tools.ModelIterator
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import nl.grons.metrics.scala.MetricBuilder

import optimus.optimization._

import pprint.Config.Defaults._

import sext._

object SAT4JSolver extends Solver {

  protected def solve(failureSpec: FailureSpec, goal: GoalNode,
                    firstMessageSendTimes: Map[String, Int], seed: Set[SolverVariable])
                   (implicit metrics: MetricBuilder):
  Traversable[Set[SolverVariable]] = {
    val solver = SolverFactory.newLight()
    val idToSatVariable = mutable.HashMap[Int, SolverVariable]()
    val satVariableToId = mutable.HashMap[SolverVariable, Int]()

    implicit def satVarToInt(satVar: SolverVariable): Int = {
      val id = satVariableToId.getOrElseUpdate(satVar, {
        satVar match {
          case Not(v) => -1 * satVarToInt(v)
          case _ => solver.nextFreeVarId(true)
        }
      })
      idToSatVariable(id) = satVar
      id
    }
    implicit def satVarsToVecInt(clause: Iterable[SolverVariable]): IVecInt =
      new VecInt(clause.map(satVarToInt).toArray)

    var timer = System.currentTimeMillis();
    val bf = BooleanFormula(goal.booleanFormula).simplifyAll.flipPolarity
    logger.debug(s"initial formula \n${bf.treeString}")
    logger.debug(s"${System.currentTimeMillis()-timer} millis -- simplification")
    timer = System.currentTimeMillis();
    val formula = bf.convertToCNFAll
    logger.debug(s"${System.currentTimeMillis()-timer} millis -- CNF")

    val importantNodes: Set[String] =
      formula.root.vars.filter(_._3 < failureSpec.eot).map(_._1).toSet ++
    seed.collect { case cf: CrashFailure => cf.node }
    if (importantNodes.isEmpty) {
      logger.debug(s"Goal ${goal.tuple} has no important nodes; skipping SAT solver")
      return Set.empty
    } else {
      logger.debug(s"Goal ${goal.tuple} has important nodes $importantNodes")
    }

    // Add constraints to ensure that each node crashes at a single time, or never crashes:
    for (node <- importantNodes) {
      // There's no point in considering crashes before the first time that a node sends a message,
      // since all such scenarios will be equivalent to crashing when sending the first message:
      val firstSendTime = firstMessageSendTimes.getOrElse(node, 1)
      // Create one variable for every time at which the node could crash
      val crashVars = (firstSendTime to failureSpec.eot - 1).map(t => CrashFailure(node, t))
      //val crashVars = (firstSendTime to failureSpec.eff).map(t => CrashFailure(node, t))
      // Include any crashes specified in the seed, since they might be excluded by the
      // "no crashes before the first message was sent" constraint:
      val seedCrashes = seed.collect { case c: CrashFailure => c }
      // An extra variable for scenarios where the node didn't crash:
      val neverCrashed = NeverCrashed(node)
      // Each node crashes at a single time, or never crashes:
      solver.addExactly((crashVars ++ seedCrashes).toSet ++ Seq(neverCrashed), 1)
    }
    // If there are at most C crashes, then at least (N - C) nodes never crash:
    solver.addAtLeast(failureSpec.nodes.map(NeverCrashed), failureSpec.nodes.size - failureSpec.maxCrashes)

    for (disjunct <- formula.conjuncts.conjunctz;
        if !disjunct.disjuncts.isEmpty
    ) {
      val messageLosses = disjunct.disjuncts.map(MessageLoss.tupled)
      val crashes = messageLosses.flatMap { loss =>
        val firstSendTime = firstMessageSendTimes.getOrElse(loss.from, 1)
        val crashTimes = firstSendTime to loss.time
        crashTimes.map ( t => CrashFailure(loss.from, t))
      }
      //logger.warn(s"loss possibility: $messageLosses")
      //logger.warn(s"crash possibility: $crashes")

      solver.addClause(messageLosses ++ crashes)
    }

    // Assume any message losses that have already occurred & disallow failures at or after the EFF
    val nonCrashes = formula.root.vars.filter(_._3 >= failureSpec.eff).map(MessageLoss.tupled).map(Not)
    val assumptions = seed ++ nonCrashes

    val models = ArrayBuffer[Set[SolverVariable]]()
    metrics.timer("sat4j-time").time {
      val modelIterator = new ModelIterator(solver)
      while (modelIterator.isSatisfiable(assumptions)) {
        val newModel = modelIterator.model().filter(_ > 0).map(idToSatVariable).toSet
        // Exclude models where no failures or crashes occurred:
        if (!newModel.filter(!_.isInstanceOf[NeverCrashed]).isEmpty) {
          models += newModel
        }
      }
    }
    solver.reset()  // Required to allow the solver to be GC'ed.
    //logger.de("RETURNING with " + models.size)
    models
  }
}
