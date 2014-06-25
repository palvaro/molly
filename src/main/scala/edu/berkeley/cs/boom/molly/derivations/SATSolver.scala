package edu.berkeley.cs.boom.molly.derivations

import edu.berkeley.cs.boom.molly.FailureSpec
import scala.collection.mutable
import com.typesafe.scalalogging.slf4j.Logging
import scala.language.implicitConversions
import com.codahale.metrics.MetricRegistry
import nl.grons.metrics.scala.{MetricName, MetricBuilder}
import scala.annotation.tailrec
import z3.scala._

object SATSolver extends Logging {
  sealed trait SATVariable
  case class CrashFailure(node: String, time: Int) extends SATVariable
  case class NeverCrashed(node: String) extends SATVariable
  case class MessageLoss(from: String, to: String, time: Int) extends SATVariable {
    require (from != to, "Can't lose messages sent to self")
  }
  case class Not(v: SATVariable) extends SATVariable

  /**
   * @param failureSpec a description of failures.
   * @param goals a list of goals whose derivations we'll attempt to falsify
   * @param messages a list of messages sent during the program's execution
   * @param seed a set of message failures and crashes that we already know have occurred,
   *             e.g. from previous runs.
   * @return all solutions to the SAT problem, formulated as failure specifications
   */
  def solve(failureSpec: FailureSpec, goals: List[GoalNode], messages: Seq[Message],
            seed: Set[SATVariable] = Set.empty)(implicit metricRegistry: MetricRegistry):
    Set[FailureSpec] = {
    implicit val metrics = new MetricBuilder(MetricName(getClass), metricRegistry)
    val firstMessageSendTimes =
      messages.groupBy(_.from).mapValues(_.minBy(_.sendTime).sendTime)
    val models = goals.flatMap { goal => solve(failureSpec, goal, firstMessageSendTimes, seed) }.toSet
    logger.info(s"SAT problem has ${models.size} solutions")
    logger.debug(s"SAT solutions are:\n${models.map(_.toString()).mkString("\n")}")
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
    def removeSupersets(modelsBySize: Seq[(Int, Set[Set[SATVariable]])],
                        accum: Seq[Set[SATVariable]] = Seq.empty): Seq[Set[SATVariable]] = {
      if (modelsBySize.isEmpty) {
        accum
      } else {
        val smallerModels: Seq[Set[SATVariable]] = modelsBySize.tail.map(_._2).flatten
        val minimalModels = modelsBySize.head._2.toSeq.filterNot {
          sup => smallerModels.exists(sub => isSuperset(sup, sub))
        }
        removeSupersets(modelsBySize.tail, minimalModels ++ accum)
      }
    }
    val minimalModels = removeSupersets(modelsBySize)
    logger.info(s"SAT problem has ${minimalModels.size} minimal solutions")
    logger.debug(s"SAT solutions are:\n${models.map(_.toString()).mkString("\n")}")

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

  private def solve(failureSpec: FailureSpec, goal: GoalNode,
                    firstMessageSendTimes: Map[String, Int], seed: Set[SATVariable])
                   (implicit metrics: MetricBuilder):
    Traversable[Set[SATVariable]] = {

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

    val z3ConstNameToVar = mutable.HashMap[String, SATVariable]()
    val varToZ3Map = mutable.HashMap[SATVariable, Z3AST]()

    implicit def varToZ3(satVar: SATVariable): Z3AST = {
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

    def goalToZ3(goal: GoalNode): Z3AST = {
      if (goal.rules.isEmpty) {
        goal.ownImportantClock match {
          case None => z3.mkTrue()
          case Some((from, to, time)) =>
            // The message could be missing due to a message loss or due to its
            // sender having crashed at an earlier timestamp:
            val loss = varToZ3(MessageLoss(from, to, time))
            val firstSendTime = firstMessageSendTimes.getOrElse(from, 1)
            val crashTimes = firstSendTime to time
            val crashes = crashTimes.map ( t => varToZ3(CrashFailure(from, t)))
            z3.mkOr(loss, z3.mkOr(crashes: _*))
            //loss  // TODO: handle crashes
        }
      } else {
        val ruleASTs = goal.rules.toSeq.map(ruleToZ3)
        z3.mkOr(ruleASTs: _*)
      }
    }

    def ruleToZ3(rule: RuleNode): Z3AST = {
      val subgoalASTs = rule.subgoals.toSeq.map(goalToZ3)
      z3.mkAnd(subgoalASTs: _*)
    }

    def exactlyOne(vars: Seq[Z3AST]): Z3AST = {
      val options = for (selected <- vars;
           notSelected = vars.filter(_ != selected))
      yield {
        z3.mkAnd(selected, z3.mkAnd(notSelected.map(z3.mkNot): _*))
      }
      z3.mkOr(options: _*)

    }

    def addAtLeast(nodes: List[NeverCrashed], keep: Int): Z3AST = {
      logger.warn(s"keep $keep nodes $nodes")
      val possibleUp = nodes.combinations(keep).map { combo =>
        logger.warn(s"combo $combo")
        z3.mkAnd(combo.map(varToZ3): _*)
      }.toList
      logger.warn(s"possible $possibleUp")
      z3.mkOr(possibleUp: _*)
    }

    val solver = z3.mkSolver()
    solver.assertCnstr(goalToZ3(goal))

    // Assume any message losses that have already occurred & disallow failures at or after the EFF
    // TODO: maybe we could simply not generate message loss variables at or after the EFF
    // so we don't have to add these assumptions.
    val nonCrashes = goal.importantClocks.filter(_._3 >= failureSpec.eff).map(MessageLoss.tupled).map(Not)
    val assumptions = seed ++ nonCrashes
    for (assumption <- assumptions) {
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
    // TODO: implement this constraint
    solver.assertCnstr(addAtLeast(failureSpec.nodes.map(NeverCrashed), failureSpec.nodes.size - failureSpec.maxCrashes))

    def modelToVars(model: Z3Model): Seq[SATVariable] = {
      val True = z3.mkTrue()
      val False = z3.mkFalse()
      model.getModelConstantInterpretations.map {
        case (decl, True) =>
          z3ConstNameToVar(decl.getName.toString())
        case (decl, False) =>
          Not(z3ConstNameToVar(decl.getName.toString()))
      }.toSeq
    }

    val allModels: Seq[Set[SATVariable]] = {
      val results = mutable.ArrayBuffer[Set[SATVariable]]()
      while (true) {
        solver.check()
        if (solver.isModelAvailable) {
          val model = solver.getModel()
          val allModelVars = modelToVars(model)
          results += allModelVars.filterNot(_.isInstanceOf[Not]).toSet
          // Add a new "give me a different model" constraint:
          solver.assertCnstr(z3.mkNot(z3.mkAnd(allModelVars.map(varToZ3): _*)))
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
