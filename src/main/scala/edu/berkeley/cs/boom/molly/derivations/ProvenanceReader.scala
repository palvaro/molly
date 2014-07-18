package edu.berkeley.cs.boom.molly.derivations

import edu.berkeley.cs.boom.molly.ast._
import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.cs.boom.molly.ast.StringLiteral
import edu.berkeley.cs.boom.molly.ast.Rule
import edu.berkeley.cs.boom.molly.{FailureSpec, UltimateModel}
import edu.berkeley.cs.boom.molly.ast.Identifier
import edu.berkeley.cs.boom.molly.ast.Program
import java.util.concurrent.atomic.AtomicInteger
import scalaz._
import nl.grons.metrics.scala.InstrumentedBuilder
import com.codahale.metrics.MetricRegistry
import scala.collection.immutable._


object ProvenanceReader {
  val WILDCARD = "__WILDCARD__"

  /**
   * Check tuples for compatibility while accounting for wildcards.
   */
  def matchesPattern(goal: List[String])(tuple: List[String]): Boolean = {
    require(goal.size == tuple.size, "Mismatched sizes")
    goal.zip(tuple).forall {
      case (a, WILDCARD) => true
      case (WILDCARD, b) => true
      case (x, y) => x == y
    }
  }
}

/**
 * Constructs rule-goal graphs from provenance captured during execution.
 */
class ProvenanceReader(program: Program,
                       failureSpec: FailureSpec,
                       model: UltimateModel, negativeSupport: Boolean)
                      (implicit val metricRegistry: MetricRegistry) extends Logging with InstrumentedBuilder {
  import ProvenanceReader._
  private val nextRuleNodeId = new AtomicInteger(0)
  private val nextGoalNodeId = new AtomicInteger(0)

  private val derivationBuilding = metrics.timer("derivation-tree-building")

  private val provTableManager = new ProvenanceTableManager(program, model, failureSpec)

  val getDerivationTree: GoalTuple => GoalNode =
    Memo.mutableHashMapMemo { derivationBuilding.time { buildDerivationTree(_) }}

  val getAntiDerivationTree: GoalTuple => GoalNode =
    Memo.mutableHashMapMemo { derivationBuilding.time { buildDerivationTree(_) }}

  val getPhonyDerivationTree: GoalTuple => PhonyGoalNode =
    Memo.mutableHashMapMemo { derivationBuilding.time { buildPhonyDerivationTree(_) }}

  def getDerivationTreesForTable(goal: String): List[GoalNode] = {
    model.tableAtTime(goal, failureSpec.eot).map(GoalTuple(goal, _)).map(getDerivationTree)
  }

  def getPhonyDerivationTreesForTable(goal: String): List[PhonyGoalNode] = {
    model.tableAtTime(goal, failureSpec.eot).map(GoalTuple(goal, _)).map(getPhonyDerivationTree)
  }

  val messages: List[Message] = provTableManager.messages

  private def buildPhonyDerivationTree(goalTuple: GoalTuple): PhonyGoalNode = {
    val msgs = getContributingMessages(goalTuple)
    logger.debug(s"$goalTuple phony msgs: $msgs")
    PhonyGoalNode(nextGoalNodeId.getAndIncrement, goalTuple, msgs.map(getPhonyDerivationTree))
  }

  private def buildDerivationTree(goalTuple: GoalTuple): GoalNode = {
    logger.debug(s"Reading provenance for tuple $goalTuple")
    val tupleWasDerived = model.tables(goalTuple.table).exists(matchesPattern(goalTuple.cols))
    // First, check whether the goal tuple is part of the EDB:
    if (goalTuple.negative && tupleWasDerived) {
      logger.debug(s"Prov. table for ${goalTuple.table} contains $goalTuple !!")
      return RealGoalNode(nextGoalNodeId.getAndIncrement, goalTuple.copy(tombstone = true), Set.empty)
      // if it's a neg tuple, discharge it.
    } else if (isInEDB(goalTuple)) {
      logger.debug(s"Found $goalTuple in EDB")
      return RealGoalNode(nextGoalNodeId.getAndIncrement, goalTuple, Set.empty)
    }
    // Otherwise, a rule must have derived it:
    val ruleFirings = findRuleFirings(goalTuple)
    if (goalTuple.negative) {
      if (ruleFirings.isEmpty) return RealGoalNode(nextGoalNodeId.getAndIncrement, goalTuple, Set.empty)
      logger.debug(s"Negative goal $goalTuple has potential firings; falling through to explore them")
    } else if (ruleFirings.isEmpty && tupleWasDerived) {
      throw new IllegalStateException(s"Couldn't find rules to explain derivation of $goalTuple")
    }
    val ruleNodes = ruleFirings.map { case (provRule, bindings) =>
      logger.debug(s"Exploring firing of ${provRule.head.tableName} with bindings $bindings")
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
      val pos = goalTuple.copy(negative = false)
      val positiveGoals = {
        val aggVars = provRule.head.variablesInAggregates
        if (aggVars.isEmpty) {
          positivePreds.map(substituteBindings).filter{g => g != pos}
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

          predsWithoutAggVars.map(substituteBindings).filter{g => g != pos} ++ aggGoals
        }
      }

      logger.debug(s"Positive subgoals: $positiveGoals")
      logger.debug(s"Negative subgoals: $negativeGoals")

      // As a sanity check, ensure that negative goals don't have any derivations:
      for (goal <- negativeGoals) {
        val matchingTuples = model.tables(goal.table).filter(matchesPattern(goal.cols))
        val ruleFirings = findRuleFirings(goal)
        val internallyConsistent = matchingTuples.isEmpty == ruleFirings.isEmpty
        if (!goalTuple.negative) {
          assert(internallyConsistent, s"Tuple $goal found in table without derivation (or vice-versa)")
          assert(matchingTuples.isEmpty, s"Found derivation ${matchingTuples(0)} of negative goal $goal")
          assert(ruleFirings.isEmpty, s"Found rule firings $ruleFirings for negative goal $goal")
        }
        logger.debug(s"negative goal: $goal}")
      }

      // Recursively compute the provenance of the new goals:
      if (negativeSupport) {
        if (goalTuple.negative) {
          // We're considering a "rule firing" that describes a potential way in which a positive
          // goalTuple could have been derived.  By applying DeMorgan's law to this firing, we
          // produce one rule per subgoal describing how goalTuple couldn't have been derived
          // through application of THIS rule.
          val subgoals = positiveGoals.map(_.copy(negative = true)) ++ negativeGoals.map(_.copy(negative = false))
          // TODO: possibly a more meaningful choice of provRule here?
          subgoals.map(sg => RuleNode(nextRuleNodeId.getAndIncrement, provRule, Set(getDerivationTree(sg)))).toSet
        } else {
          val subgoals = positiveGoals.map(_.copy(negative = false)) ++ negativeGoals.map(_.copy(negative = true))
          Set(RuleNode(nextRuleNodeId.getAndIncrement, provRule, subgoals.map(getDerivationTree).toSet))
        }
      } else { // If negative provenance is disabled, just ignore negative subgoals.
        val subgoals = positiveGoals.map(_.copy(negative = false))
        Set(RuleNode(nextRuleNodeId.getAndIncrement, provRule, subgoals.map(getDerivationTree).toSet))
      }
    }
    if (negativeSupport && goalTuple.negative) {
      // Negative subgoals involve an interesting duality: NOT(A) is a goal that's derived through
      // a single rule whose subgoals represent the non-derivation of A through particular rules.
      // So, a particular rule may fail to derive A for any one of several reasons and every such
      // potential rule firing must fail to occur.
      val nonFiringsOfSubgoals = ruleNodes.map { causesOfNonFiring =>
        // TODO: cleanup of internal special goal tuples, or maybe make new goal node types
        RealGoalNode(nextGoalNodeId.getAndIncrement, GoalTuple("AllfiringsFail", Nil), causesOfNonFiring)
      }
      val dummyRule = program.rules.head.copy(program.rules.head.head.copy("AllFiringsFail"))
      // ^^^ TODO: _terrible_ hack; should put in a real rule or allow
      // for rule nodes that don't correspond to program rules
      val failureOfAllRules = RuleNode(nextRuleNodeId.getAndIncrement, dummyRule, nonFiringsOfSubgoals.toSet)
      RealGoalNode(nextGoalNodeId.getAndIncrement, goalTuple, Set(failureOfAllRules))
    } else {
      RealGoalNode(nextGoalNodeId.getAndIncrement, goalTuple, ruleNodes.flatten.toSet)
    }
  }

  private def getContributingMessages(tuple: GoalTuple): Set[GoalTuple] = {
    val nodes = messages.map{m => m.from} ++ messages.map{m => m.to}
    val msgs = messages.filter{m => m.receiveTime != FailureSpec.NEVER}.map{m => GoalTuple("meta", List(m.to, m.from, m.sendTime.toString))}
    if (nodes.contains(tuple.cols.head)) {
      msgs.filter { m => m != tuple && m.cols.head == tuple.cols.head && m.cols.last.toInt < tuple.cols.last.toInt}.toSet
    } else {
      // a goal that doesn't have a location depends on all previous messages at all locations, no?
      msgs.filter{m => m != tuple && m.cols.last.toInt < tuple.cols.last.toInt}.toSet
    }
  }

  /**
   * Find all rule firings that might explain how we derived `goalTuple` and return
   * the matching rules and variable bindings extracted from the provenance table entries.
   */
  private def findRuleFirings(goalTuple: GoalTuple): Seq[(Rule, Map[String, String])] = {
    assert(goalTuple.cols.last != WILDCARD, "Time shouldn't be a wildcard")
    if (goalTuple.negative) {
      for (
        table <- provTableManager.provTables.getOrElse(goalTuple.table, Seq.empty);
        time = if (table.rule.isAsync) goalTuple.cols.last.toInt - 1 else goalTuple.cols.last.toInt
        if time > 0
        if table.search(goalTuple.cols).isEmpty
      ) yield {
        val nonTimeColNames = table.rule.head.cols.take(goalTuple.cols.size - 1)
        val bindings = nonTimeColNames.zip(goalTuple.cols.init).collect {
          case (Identifier(ident), provValue) => (ident, provValue)
        } ++ List("_" -> WILDCARD, "NRESERVED" -> time.toString, "MRESERVED" -> (time + 1).toString)
        (table.rule, bindings.toMap)
      }
    } else {
      provTableManager.search(goalTuple)
    }
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

}


class ProvenanceTableManager(program: Program, model: UltimateModel, failureSpec: FailureSpec)
  extends Logging {
  import ProvenanceReader._

  private val tableNamePattern = """^(.*)_prov\d+$""".r
  private def isProvRule(rule: Rule): Boolean =
    tableNamePattern.findFirstMatchIn(rule.head.tableName).isDefined
  val provRules = program.rules.filter(isProvRule)
  val provTables: Map[String, Seq[ProvenanceTable]] = provRules.map { rule =>
    val provRuleName = rule.head.tableName
    val provTableEntries = model.tables(provRuleName)
    ProvenanceTable(rule, provTableEntries)
  }.groupBy(t => tableNamePattern.findFirstMatchIn(t.rule.head.tableName).get.group(1))

  logger.debug(s"Provenance tables are: ${provTables.mapValues(_.map(x => x.rule.head.tableName))}")

  private def provRowToVariableBindings(provRule: Rule, provTableRow: List[String]):  Map[String, String] = {
    // Given a row from the provenance table, we need to reconstruct the variable bindings.
    // Most of the time there will be a 1-to-1 mapping between row values and variable
    // binding values, but in some cases rules may have arithmetic in the head, such as
    //    bcast_prov4(N, P, NRESERVED + 1) :- log(N, P, MRESERVED), clock(N, _, NRESERVED, _);
    // In these cases, we might need to invert that arithmetic to find the actual variable binding.
    // Fortunately, the current method of generating the provenance rules ensures that those
    // bindings will also be recorded in the head, so we can just skip over expressions:
    require(provRule.head.cols.size == provTableRow.size, s"Incorrect number of columns $provRule.head vs $provTableRow")
    val bindings = provRule.head.cols.zip(provTableRow).collect {
      case (Identifier(ident), provValue) => (ident, provValue)
    } ++ List("_" -> WILDCARD)
    bindings.toMap
  }

  /**
   * Search all provenance tables for entries that match the given goal tuple and return
   * any matching rules and the bindings derived from them.
   */
  def search(goal: GoalTuple): Seq[(Rule, Map[String, String])] = {
    for (
      provTable <- provTables.getOrElse(goal.table, Seq.empty);
      matchingRow <- provTable.search(goal.cols)
    ) yield {
      (provTable.rule, provRowToVariableBindings(provTable.rule, matchingRow))
    }
  }

  lazy val messages: List[Message] = {
    val msgs = for (
      (tableName, provTables) <- provTables;
      table <- provTables
      if table.rule.isAsync;
      clockPred = table.rule.bodyPredicates.filter(_.tableName == "clock")(0);
      fromIdent = clockPred.cols(0).asInstanceOf[Identifier].name;
      toIdent = clockPred.cols(1).asInstanceOf[Identifier].name;
      row <- table.facts;
      bindings = provRowToVariableBindings(table.rule, row);
      from = bindings(fromIdent);
      to = bindings(toIdent);
      sendTime = bindings("NRESERVED").toInt;
      receiveTime = bindings("MRESERVED").toInt;
      if sendTime != failureSpec.eot
    ) yield {
      Message(tableName, from, to, sendTime, receiveTime)
    }
    msgs.toList
  }
}

case class ProvenanceTable(rule: Rule, facts: List[List[String]]) {
  import ProvenanceReader._

  /**
   * Search this provenance table for facts matching the given pattern
   */
  def search(pattern: List[String]): List[List[String]] = {
    // The provenance tables record _all_ variable bindings used in the rule firing, not just those
    // that appear in the rule head, so the provenance table's schema won't necessarily match the
    // original table.  Because of how we perform the rewriting, the two tables agree on the first
    // N-1 columns, then the provenance table may have extra columns.  For both tables, the last
    // column will always record the time.
    def matchesTarget(fact: List[String]): Boolean = {
      matchesPattern(pattern)(fact.take(pattern.size - 1) ++ List(fact.last))
    }
    facts.filter(matchesTarget)
  }
}