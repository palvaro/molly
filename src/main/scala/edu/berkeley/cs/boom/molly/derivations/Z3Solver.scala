package edu.berkeley.cs.boom.molly.derivations

import edu.berkeley.cs.boom.molly.FailureSpec
import scala.collection.mutable
import scala.language.implicitConversions
import nl.grons.metrics.scala.MetricBuilder
import z3.scala._
import scalaz._


object Z3Solver extends Solver {

  protected def solve(failureSpec: FailureSpec, goal: GoalNode,
                    firstMessageSendTimes: Map[String, Int], seed: Set[SolverVariable])
                   (implicit metrics: MetricBuilder):
    Traversable[Set[SolverVariable]] = {

    val encoding = metrics.timer("encoding")
    val solving = metrics.timer("solving")
    val iterations = metrics.counter("iterations")

    // Crash failures:
    // Only nodes that sent messages (or that are assumed to have crashed as part of the seed)
    // will be candidates for crashing:
    val importantNodes: Set[String] =
      goal.importantClocks.filter(_._3 < failureSpec.eot).map(_._1).toSet ++
        seed.collect { case cf: CrashFailure => cf.node }
    if (importantNodes.isEmpty) {
      logger.debug(s"Goal ${goal.tuple} has no important nodes; skipping SAT solver")
      return Set.empty
    } else {
      logger.debug(s"Goal ${goal.tuple} has important nodes $importantNodes")
    }

    val config = new Z3Config("MODEL" -> true)
    val z3 = new Z3Context(config)

    val z3ConstNameToVar = mutable.HashMap[String, SolverVariable]()
    val varToZ3Map = mutable.HashMap[SolverVariable, Z3AST]()

    implicit def varToZ3(satVar: SolverVariable): Z3AST = {
      val constName = satVar.toString
      val id = varToZ3Map.getOrElseUpdate(satVar, {
        satVar match {
          case Not(v) => z3.mkNot(varToZ3(v))
          case _ => z3.mkBoolConst(constName)
        }
      })
      z3ConstNameToVar(constName) = satVar
      id
    }

    def goalToZ3Internal(goal: GoalNode): Z3AST = {
      iterations.inc()
      if (goal.rules.isEmpty) {
        goal.ownImportantClock match {
          case None => z3.mkFalse()
          case Some((from, to, time)) =>
            // The message could be missing due to a message loss or due to its
            // sender having crashed at an earlier timestamp:
            val loss = varToZ3(MessageLoss(from, to, time))
            val firstSendTime = firstMessageSendTimes.getOrElse(from, 1)
            val crashTimes = firstSendTime to time
            val crashes = crashTimes.map ( t => varToZ3(CrashFailure(from, t)))
            val lossAST = if (time < failureSpec.eff) loss else z3.mkFalse
            val crashAST = if (crashes.toSet.isEmpty) z3.mkFalse else z3.mkOr(crashes: _*)
            z3.mkOr(lossAST, crashAST)
        }
      } else {
        val ruleASTs = goal.rules.toSeq.map(ruleToZ3)
        z3.mkAnd(ruleASTs: _*)
      }
    }

    lazy val goalToZ3: GoalNode => Z3AST =
      Memo.mutableHashMapMemo(goalToZ3Internal(_))

    def ruleToZ3(rule: RuleNode): Z3AST = {
      val subgoalASTs = rule.subgoals.toSeq.map(goalToZ3)
      z3.mkOr(subgoalASTs: _*)
    }

    def exactlyOne(vars: Seq[Z3AST]): Z3AST = {
      val options = for (selected <- vars;
           notSelected = vars.filter(_ != selected))
      yield {
        z3.mkAnd(selected, z3.mkAnd(notSelected.map(z3.mkNot): _*))
      }
      z3.mkOr(options: _*)

    }

    def assertAtLeastK(solver: Z3Solver, bools: List[Z3AST], k: Int) {
      // "At least k" is easy to express as a pseudo-boolean constraint.
      // Let's introduce PB variables that are logically equivalent to our original variables:
      val intSort = z3.mkIntSort()
      val zero = z3.mkInt(0, intSort)
      val one = z3.mkInt(1, intSort)
      val kay = z3.mkInt(k, intSort)
      def toPB(bool: Z3AST): Z3AST = {
        // Encoding based on the one from http://stackoverflow.com/a/20550445/590203
        val pb = z3.mkIntConst("PB_" + bool.toString())
        // This integer is a pseudo-boolean:
        solver.assertCnstr(z3.mkAnd(z3.mkGE(pb, zero), z3.mkLE(pb, one)))
        // The psuedo-boolean is related to the original boolean:
        solver.assertCnstr(z3.mkIff(z3.mkEq(pb, one), bool))
        pb
      }
      val pbs = bools.map(toPB)
      val sum = z3.mkAdd(pbs: _*)
      solver.assertCnstr(z3.mkGE(sum, kay))
    }

    implicit val solver = z3.mkSolver()
    encoding.time{ solver.assertCnstr(goalToZ3(goal)) }

    // Assume any message losses that have already occurred
    for (assumption <- seed) {
      solver.assertCnstr(varToZ3(assumption))
    }
    // Add constraints to ensure that each node crashes at a single time, or never crashes:

    for (node <- importantNodes) {
      // There's no point in considering crashes before the first time that a node sends a message,
      // since all such scenarios will be equivalent to crashing when sending the first message:
      val firstSendTime = firstMessageSendTimes.getOrElse(node, 1)
      // Create one variable for every time at which the node could crash
      val crashVars = (firstSendTime to failureSpec.eot - 1).map(t => CrashFailure(node, t))
      // Include any crashes specified in the seed, since they might be excluded by the
      // "no crashes before the first message was sent" constraint:
      val seedCrashes = seed.collect { case c: CrashFailure => c }
      // An extra variable for scenarios where the node didn't crash:
      val neverCrashed = NeverCrashed(node)
      // Each node crashes at a single time, or never crashes:
      solver.assertCnstr(exactlyOne(((crashVars ++ seedCrashes).toSet ++ Seq(neverCrashed)).map(varToZ3).toSeq))
    }

    // If there are at most C crashes, then at least (N - C) nodes never crash:

    assertAtLeastK(solver, failureSpec.nodes.map(NeverCrashed).map(varToZ3),
      failureSpec.nodes.size - failureSpec.maxCrashes)

    def modelToVars(model: Z3Model): Seq[SolverVariable] = {
      val True = z3.mkTrue()
      val False = z3.mkFalse()
      model.getModelConstantInterpretations.flatMap {
        case (decl, boolValue) =>
          val declName = decl.getName.toString()
          if (declName.startsWith("PB_")) {
            None
          } else if (boolValue == False) {
            Some(Not(z3ConstNameToVar(declName)))
          } else if (boolValue == True) {
            Some(z3ConstNameToVar(declName))
          } else {
            throw new IllegalStateException()
          }
      }.filterNot(_.isInstanceOf[NeverCrashed]).toSeq
      // Less clutter if we don't emit the internal "never crashed" variables
    }

    val allModels: Seq[Set[SolverVariable]] = {
      val results = mutable.ArrayBuffer[Set[SolverVariable]]()
      while (true) {
        solving.time{ solver.check() }
        if (solver.isModelAvailable) {
          val model = solver.getModel()
          val allModelVars = modelToVars(model)
          val newSolution = allModelVars.filterNot(_.isInstanceOf[Not]).toSet
          // Skip empty models
          if (!newSolution.isEmpty) {
            results += newSolution
          }
          // Add a new "give me a different model" constraint:
          solver.assertCnstr(z3.mkNot(z3.mkAnd(allModelVars.map(varToZ3): _*)))
          model.delete  // TODO: apparently this is deprecated.
        } else {
          return results

        }
      }
      results
    }

    z3.delete()
    allModels
  }
}
