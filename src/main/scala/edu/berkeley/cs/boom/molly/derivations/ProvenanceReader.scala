package edu.berkeley.cs.boom.molly.derivations

import edu.berkeley.cs.boom.molly.ast._
import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.cs.boom.molly.ast.StringLiteral
import edu.berkeley.cs.boom.molly.ast.Rule
import edu.berkeley.cs.boom.molly.{FailureSpec, UltimateModel}
import edu.berkeley.cs.boom.molly.ast.Identifier
import edu.berkeley.cs.boom.molly.ast.Program
import edu.berkeley.cs.boom.molly.ast.Atom

import edu.berkeley.cs.boom.molly.derivations._
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

  /**
   * Used by internal sanity tests to check for conflicting evidence over whether negative
   * subgoals were derived.
   */
  private def assertNotDerived(goal: GoalTuple) {
    val matchingTuples = model.tables(goal.table).filter(matchesPattern(goal.cols))
    val ruleFirings = findRuleFirings(goal)
    val internallyConsistent = matchingTuples.isEmpty == ruleFirings.isEmpty
    assert(internallyConsistent, s"Tuple $goal found in table without derivation (or vice-versa)")
    assert(matchingTuples.isEmpty, s"Found derivation ${matchingTuples(0)} of negative goal $goal")
    assert(ruleFirings.isEmpty, s"Found rule firings $ruleFirings for negative goal $goal")
  }

  private def buildPhonyDerivationTree(goalTuple: GoalTuple): PhonyGoalNode = {
    val msgs = getContributingMessages(goalTuple)
    logger.debug(s"$goalTuple phony msgs: $msgs")
    PhonyGoalNode(goalTuple, msgs.map(getPhonyDerivationTree))
  }

  private def buildDerivationTree(goalTuple: GoalTuple): GoalNode = {
    val tupleWasDerived = model.tables(goalTuple.table).exists(matchesPattern(goalTuple.cols))
    lazy val ruleFirings = findRuleFirings(goalTuple)
    logger.debug(s"Reading provenance for tuple $goalTuple $tupleWasDerived")
    if (goalTuple.negative) {
      // a conservative overapproximation of the facts whose existence make goalTuple false
      val causes = provTableManager.possibleCauses(goalTuple)
      /* we need a stub rule node, which requires at least one stub table,
         to capture the conservative assumption that *any* existing records from which
         goalTuple is negatively reachable could, if falsified, make goalTuple true */
      val phonyPred = Predicate("phonyGoal", List(StringLiteral("someplace")), false, None)
      val phonyRule = Rule(Predicate("phony",List(),false, None),List(Left(phonyPred)))
      if (causes.isEmpty) {
        logger.debug(s"no causes for $goalTuple")
        RealGoalNode(goalTuple, Set())
      } else {
        logger.warn(s"possible causes of $goalTuple: $causes")
        RealGoalNode(goalTuple, Set(RuleNode(phonyRule, causes.map(getDerivationTree).toSet)))
      }
    } else { // goalTuple is positive
      if (isInEDB(goalTuple)) {
        logger.debug(s"Found $goalTuple in EDB")
        RealGoalNode(goalTuple, Set.empty)
      } else if (ruleFirings.isEmpty && tupleWasDerived) {
        throw new IllegalStateException(s"Couldn't find rules to explain derivation of $goalTuple")
      } else {
        val ruleNodes = ruleFirings.map { case (provRule, bindings) =>
          val (positiveGoals, negativeGoals) = ruleFiringToSubgoals(provRule, bindings)
          negativeGoals.foreach(assertNotDerived) // Since we assume !goalTuple.negative here
          // Recursively compute the provenance of the new goals:
          val subgoals = if (negativeSupport) {
            positiveGoals.map(_.copy(negative = false)) ++ negativeGoals.map(_.copy(negative = true))
          } else {
            positiveGoals.map(_.copy(negative = false))
          }
          Set(RuleNode(provRule, subgoals.map(getDerivationTree).toSet))
        }
        RealGoalNode(goalTuple, ruleNodes.flatten.toSet)
      }
    }
  }

  /**
   * Given a rule firing, construct sets of positive and negative subgoals.
   * @return (positiveGoals, negativeGoals)
   */
  private def ruleFiringToSubgoals(provRule: Rule, bindings: Map[String, String]): (Seq[GoalTuple], Seq[GoalTuple]) = {
    logger.debug(s"Extracting subgoals from firing of ${provRule.head.tableName} with bindings $bindings")
    val time = bindings("NRESERVED").toInt

    val (negativePreds, positivePreds) = provRule.bodyPredicates.partition(_.notin)
    val negativeGoals = negativePreds.map(predicateToGoal(bindings))
    val positiveGoals = {
      val aggVars = provRule.head.variablesInAggregates
      val (predsWithoutAggVars, predsWithAggVars) =
        positivePreds.partition(_.variables.intersect(aggVars).isEmpty)
      val aggGoals =
        predsWithAggVars.flatMap(predicateToGoal(bindings) _ andThen getAggregateSupport(time))
      predsWithoutAggVars.map(predicateToGoal(bindings)) ++ aggGoals
    }
    logger.debug(s"Positive subgoals: $positiveGoals")
    logger.debug(s"Negative subgoals: $negativeGoals")
    (positiveGoals, negativeGoals)
  }

  /**
   * Determine the support for the value of an aggregate.
   *
   * @param time the time that the aggregate was calculated at
   * @param pattern a goal / pattern that defines what fields are being aggregated.
   * @return a GoalTuple for each tuple that contributed to the aggregate.
   */
  private def getAggregateSupport(time: Int)(pattern: GoalTuple): Seq[GoalTuple] = {
    val tuples = model.tableAtTime(pattern.table, time)
    val matchingTuples = tuples.filter(matchesPattern(pattern.cols))
    matchingTuples.map(t => GoalTuple(pattern.table, t))
  }

  private def predicateToGoal(bindings: Map[String, String])(pred: Predicate): GoalTuple = {
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

case class DependsInfo(from: Predicate, to: Predicate, nonmonotonic: Boolean, temporality: Option[Time])

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

  val depends = program.rules.filter(r => r.head.tableName.indexOf("_prov") == -1).flatMap { r =>
    r.body.filter(b => b.isLeft && b.left.get.tableName != "clock").map { s =>
      val nm = s.isRight || (s.isLeft && s.left.get.notin)
      DependsInfo(s.left.get, r.head, nm, r.head.time)
    }
  }.toSet
  val prett = depends.map(d => d.from.tableName + "-->" + d.to.tableName + "(" + d.temporality + ")")
  logger.debug(s"compute reach for $prett")
  // FIXME
  val reach1 = reachability(depends, depends)//.filter(r => r.nonmonotonic)
  // there must be an idiomatic way to do this, eg using flatmap
   val reach = reach1.filter{d =>
    d.temporality match {
      case Some(_) => true
      case None => !reach1.exists{inner =>
        (d.from == inner.from && d.to == inner.to) && (d.temporality match {
          case Some(_) => true
          case None => false
        })
      }
    }
  }
  val pretty = reach.map(r => r.from.tableName + " -> " + r.to.tableName + "(" + r.temporality + ")")
  logger.debug(s"REACHES is $pretty")
  logger.debug(s"Provenance tables are: ${provTables.mapValues(_.map(x => x.rule.head.tableName))}")

  private def reachability(deps: Set[DependsInfo], deltas: Set[DependsInfo]): Set[DependsInfo] = {
    val newRecs = for (
      delt <- deltas;
      dep <- deps.filter(d => d.from == delt.to)
    ) yield {
      val nm = ((dep.nonmonotonic && !delt.nonmonotonic) || (!dep.nonmonotonic && delt.nonmonotonic))
      // again, there must be an idiomatic way to do this...
      val tmp = dep.temporality match {
        case None => delt.temporality
        case x => x
      }
      DependsInfo(dep.from, delt.to, nm, tmp)
    }
    logger.debug(s"SEMI-n.  DEP: ${deps.size} DELTAS: ${deltas.size} NEW: ${newRecs.size}")
    if (newRecs.isEmpty) {
      deps ++ deltas ++ newRecs
    } else {
      reachability(deps ++ deltas, newRecs -- deltas)
    }
  }

  def possibleCauses(goal: GoalTuple): Set[GoalTuple] = {

    val predecessorTables = reach.filter(r => goal.table == r.to.tableName).map(r => (r.from.tableName, r.temporality))
    for (
      p <- predecessorTables;
      r <- model.tables(p._1)
      if (r.last.toInt < goal.cols.last.toInt || (r.last.toInt == goal.cols.last.toInt && p._2 == None))
    ) yield {GoalTuple(p._1, r, false, false)}
  }

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
