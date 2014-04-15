package edu.berkeley.cs.boom.molly.derivations

import edu.berkeley.cs.boom.molly.ast._
import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.cs.boom.molly.ast.StringLiteral
import edu.berkeley.cs.boom.molly.ast.Rule
import edu.berkeley.cs.boom.molly.{FailureSpec, UltimateModel}
import edu.berkeley.cs.boom.molly.ast.Identifier
import scala.Some
import edu.berkeley.cs.boom.molly.ast.Program
import java.util.concurrent.atomic.AtomicInteger
import scalaz._
import Scalaz._
import nl.grons.metrics.scala.InstrumentedBuilder
import com.codahale.metrics.MetricRegistry
import edu.berkeley.cs.boom.molly.util.HashcodeCaching


case class GoalTuple(table: String, cols: List[String]) {
  override def toString: String = table + "(" + cols.mkString(", ") + ")"
}

/**
 * Represents a goal (fact to be proved).  Appears at the root
 * of the rule-goal graph.
 *
 * @param tuple the fact being proved.
 * @param rules a list of rules that derived this fact.  If this contains
 *              more than one rule, then there are multiple derivations of
 *              this fact.
 */
case class GoalNode(id: Int, tuple: GoalTuple, rules: Set[RuleNode]) extends HashcodeCaching {
  lazy val importantClocks: Set[(String, String, Int)] = {
    val childrenClocks = rules.flatMap(_.subgoals).flatMap(_.importantClocks)
    val newClock = tuple match {
      case GoalTuple("clock", List(from, to, time, _))
        if from != to && to != ProvenanceReader.WILDCARD => Set((from, to, time.toInt))
      case _ => Set.empty
    }
    childrenClocks ++ newClock
  }

  lazy val enumerateDistinctDerivations: Set[GoalNode] = {
    if (rules.isEmpty) {
      Set(this)
    } else {
      rules.flatMap(_.enumerateDistinctDerivationsOfSubGoals).map(r => this.copy(rules = Set(r)))
    }
  }
}

/**
 * Represents a concrete application of a rule.
 *
 * @param rule the rule that was applied.
 * @param subgoals the facts that were used in this rule application.
 */
case class RuleNode(id: Int, rule: Rule, subgoals: Set[GoalNode]) extends HashcodeCaching {
  require (!subgoals.isEmpty, "RuleNode must have subgoals")
  lazy val enumerateDistinctDerivationsOfSubGoals: List[RuleNode] = {
    val choices: List[List[GoalNode]] = subgoals.map(_.enumerateDistinctDerivations.toList).toList
    choices.sequence.map(subgoalDerivations => this.copy(subgoals = subgoalDerivations.toSet))
  }
}

case class Message(table: String, from: String, to: String, sendTime: Int, receiveTime: Int)

object ProvenanceReader {
  val WILDCARD = "__WILDCARD__"
}

/**
 * Constructs rule-goal graphs from provenance captured during execution.
 */
class ProvenanceReader(program: Program,
                       failureSpec: FailureSpec,
                       model: UltimateModel)
                      (implicit val metricRegistry: MetricRegistry) extends Logging with InstrumentedBuilder {
  import ProvenanceReader._
  private val nextRuleNodeId = new AtomicInteger(0)
  private val nextGoalNodeId = new AtomicInteger(0)

  private val derivationBuilding = metrics.timer("derivation-tree-building")

  val getDerivationTree: GoalTuple => GoalNode =
    Memo.mutableHashMapMemo { derivationBuilding.time { buildDerivationTree(_) }}

  def getDerivationTreesForTable(goal: String): List[GoalNode] = {
    model.tableAtTime(goal, failureSpec.eot).map(GoalTuple(goal, _)).map(getDerivationTree)
  }

  def getMessages: List[Message] = {
    val tableNamePattern = """^(.*)_prov\d+$""".r
    def isProvRule(rule: Rule): Boolean =
      tableNamePattern.findFirstMatchIn(rule.head.tableName).isDefined
    val asyncRules = program.rules.filter(isProvRule).filter(_.head.time == Some(Async()))
    logger.debug(s"Async rules are ${asyncRules.map(_.head.tableName)}")
    asyncRules.flatMap { rule =>
      val clockPred = rule.bodyPredicates.filter(_.tableName == "clock")(0)
      val fromIdent = clockPred.cols(0).asInstanceOf[Identifier].name
      val toIdent = clockPred.cols(1).asInstanceOf[Identifier].name
      val tableName = tableNamePattern.findFirstMatchIn(rule.head.tableName).get.group(1)
      model.tables(rule.head.tableName).flatMap { tuple =>
        val bindings = provRowToVariableBindings(rule, tuple)
        val from = bindings(fromIdent)
        val to = bindings(toIdent)
        val sendTime = bindings("NRESERVED").toInt
        val receiveTime = bindings("MRESERVED").toInt
        if (sendTime != failureSpec.eot)
          Some(Message(tableName, from, to, sendTime, receiveTime))
        else
          None
      }
    }
  }

  private def buildDerivationTree(goalTuple: GoalTuple): GoalNode = {
    logger.debug(s"Reading provenance for tuple $goalTuple")
    // First, check whether the goal tuple is part of the EDB:
    if (isInEDB(goalTuple)) {
      logger.debug(s"Found $goalTuple in EDB")
      return GoalNode(nextGoalNodeId.getAndIncrement, goalTuple, Set.empty)
    }
    // Otherwise, a rule must have derived it:
    val ruleFirings = findRuleFirings(goalTuple)
    assert (!ruleFirings.isEmpty, s"Couldn't find rules to derive tuple $goalTuple")
    logger.debug(s"Rule firings: $ruleFirings")
    val ruleNodes = ruleFirings.map { case (provRuleName, provTableRow) =>
      val provRule = program.rules.find(_.head.tableName == provRuleName).get
      val bindings = provRowToVariableBindings(provRule, provTableRow)
      val time = bindings("NRESERVED").toInt
      // Substitute the variable bindings to determine the set of goals used by this rule.
      def substituteBindings(pred: Predicate): GoalTuple = {
        val goalCols = pred.cols.map {
          case StringLiteral(s) => s
          case IntLiteral(i) => i.toString
          case Identifier(ident) => bindings.getOrElse(ident, WILDCARD)
          case agg: Aggregate =>
            throw new NotImplementedError("Don't know how to substitute for aggregate")
          case expr: Expr =>
            throw new NotImplementedError("Didn't expect expression to appear in predicate")
        }
        GoalTuple(pred.tableName, goalCols)
      }

      val (negativePreds, positivePreds) = provRule.bodyPredicates.partition(_.notin)
      val negativeGoals = negativePreds.map(substituteBindings)
      val positiveGoals = {
        val aggVars = provRule.head.variablesInAggregates
        if (aggVars.isEmpty) {
          positivePreds.map(substituteBindings)
        } else {
          // If the rule contains aggregates, add each tuple that contributed to the aggregate
          // as a goal.
          val (predsWithoutAggVars, predsWithAggVars) =
            positivePreds.partition(_.variables.intersect(aggVars).isEmpty)
          val aggGoals = aggVars.flatMap { aggVar =>
            val aggPreds = predsWithAggVars.map(substituteBindings)
            aggPreds.flatMap { pred =>
              val tuples = model.tableAtTime(pred.table, time)
              val matchingTuples = tuples.filter(matchesPattern(pred.cols))
              matchingTuples.map(t => GoalTuple(pred.table, t))
            }
          }
          predsWithoutAggVars.map(substituteBindings) ++ aggGoals
        }
      }

      logger.debug(s"Positive subgoals: $positiveGoals")
      logger.debug(s"Negative subgoals: $negativeGoals")

      // As a sanity check, ensure that negative goals don't have any derivations:
      for (goal <- negativeGoals) {
        val matchingTuples = model.tables(goal.table).filter(matchesPattern(goal.cols))
        val ruleFirings = findRuleFirings(goal)
        val internallyConsistent = matchingTuples.isEmpty == ruleFirings.isEmpty
        assert(internallyConsistent, s"Tuple $goal found in table without derivation (or vice-versa)")
        assert(matchingTuples.isEmpty, s"Found derivation ${matchingTuples(0)} of negative goal $goal")
        assert(ruleFirings.isEmpty, s"Found rule firings $ruleFirings for negative goal $goal")
      }

      // Recursively compute the provenance of the new goals:
      RuleNode(nextRuleNodeId.getAndIncrement, provRule, positiveGoals.map(getDerivationTree).toSet)
    }
    GoalNode(nextGoalNodeId.getAndIncrement, goalTuple, ruleNodes)
  }

  private def findRuleFirings(goalTuple: GoalTuple): Set[(String, List[String])] = {
    // Find provenance tables that might explain how we derived `goalTuple`:
    val provTables = program.tables.map(_.name).filter(_.matches(s"^${goalTuple.table}_prov\\d+"))
    logger.debug(s"Table '${goalTuple.table}' has provenance tables $provTables")
    // Check which of them have matching facts:
    provTables.map(table => (table, searchProvTable(goalTuple.cols, model.tables(table))))
      .filter(_._2.isDefined).map(x => (x._1, x._2.get))
  }

  private def isInEDB(goalTuple: GoalTuple): Boolean = {
    program.facts.filter(_.tableName == goalTuple.table).exists { fact =>
      val list = fact.cols.map {
        case IntLiteral(i) => i.toString
        case StringLiteral(s) => s
        case v => throw new IllegalStateException(
          s"Facts shouldn't contain aggregates, expressions, or variables, but found $v")
      }
      matchesPattern(goalTuple.cols)(list)
    }
  }

  /**
   * Check tuples for compatibility while accounting for wildcards.
   */
  private def matchesPattern(goal: List[String])(tuple: List[String]): Boolean = {
    require(goal.size == tuple.size, "Mismatched sizes")
    goal.zip(tuple).forall {
      case (a, WILDCARD) => true
      case (WILDCARD, b) => true
      case (x, y) => x == y
    }
  }

  private def provRowToVariableBindings(provRule: Rule, provTableRow: List[String]):
    Map[String, String] = {
    // Given a row from the provenance table, we need to reconstruct the variable bindings.
    // Most of the time there will be a 1-to-1 mapping between row values and variable
    // binding values, but in some cases rules may have arithmetic in the head, such as
    //    bcast_prov4(N, P, NRESERVED + 1) :- log(N, P, MRESERVED), clock(N, _, NRESERVED, _);
    // In these cases, we might need to invert that arithmetic to find the actual variable binding.
    // Fortunately, the current method of generating the provenance rules ensures that those
    // bindings will also be recorded in the head, so we can just skip over expressions:
    require(provRule.head.cols.size == provTableRow.size, "Incorrect number of columns")
    val bindings = provRule.head.cols.zip(provTableRow).collect {
      case (Identifier(ident), provValue) => (ident, provValue)
    } ++ List("_" -> WILDCARD)
    bindings.toMap
  }

  private def searchProvTable(target: List[String], provFacts: List[List[String]]):
    Option[List[String]] = {
    // The provenance tables record _all_ variable bindings used in the rule firing, not just those
    // that appear in the rule head, so the provenance table's schema won't necessarily match the
    // original table.  Because of how we perform the rewriting, the two tables agree on the first
    // N-1 columns, then the provenance table may have extra columns.  For both tables, the last
    // column will always record the time.
    def matchesTarget(fact: List[String]): Boolean = {
      matchesPattern(target)(fact.take(target.size - 1) ++ List(fact.last))
    }
    provFacts.find(matchesTarget)
  }
}
