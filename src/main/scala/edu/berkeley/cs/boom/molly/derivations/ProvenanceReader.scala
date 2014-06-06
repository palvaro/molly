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
import scala.collection.immutable._


case class GoalTuple(table: String, cols: List[String], negative: Boolean = false, tombstone: Boolean = false) {
  override def toString: String = (if (negative) "NOT!" else "") +  (if (tombstone) "TOMB" else "") + table + "(" + cols.mkString(", ") + ")"

}

trait GoalNode {
  /* this can't be the right way */
  lazy val id: Int = 0
  lazy val tuple: GoalTuple = GoalTuple("dummy", List("Dummy"))
  lazy val importantClocks: Set[(String, String, Int)] = Set()
  lazy val enumerateDistinctDerivations: Set[Option[GoalNode]] = Set(None)
  lazy val rules: Set[RuleNode] = Set()
  def allTups: Set[GoalTuple] = Set()
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
case class RealGoalNode(pId: Int,  pTuple: GoalTuple, pRules: Set[RuleNode], negative: Boolean = false) extends HashcodeCaching with GoalNode {
  override lazy val id = pId
  override lazy val tuple = pTuple
  override lazy val rules = pRules
  override lazy val importantClocks: Set[(String, String, Int)] = {
    val childrenClocks = rules.flatMap(_.subgoals).flatMap(_.importantClocks)
    val newClock = tuple match {
      case GoalTuple("clock", List(from, to, time, _), _, _)
        if from != to && to != ProvenanceReader.WILDCARD  && from != ProvenanceReader.WILDCARD => Set((from, to, time.toInt))
      case _ => Set.empty
    }

    childrenClocks ++ newClock
  }

  override lazy val enumerateDistinctDerivations: Set[Option[GoalNode]] = {
    if (rules.isEmpty) {
      if (tuple.tombstone) Set(None) else Set(Some(this))
    } else {
      rules.flatMap{r =>
        val dd = r.enumerateDistinctDerivationsOfSubGoals.getOrElse(Set())
        dd.map(d => Some(this.copy(pRules = Set(d))))
      }
    }
  }

  override def allTups: Set[GoalTuple] = {
    Set(tuple) ++ rules.flatMap(r => r.subgoals.flatMap(s => s.allTups))
  }
}

case class PhonyGoalNode(pId: Int, pTuple: GoalTuple, history: Set[GoalNode]) extends GoalNode {
  override lazy val id = pId
  override lazy val tuple = pTuple

  override lazy val importantClocks: Set[(String, String, Int)] = {
    val childrenClocks = history.flatMap(_.importantClocks)
    val newClock = tuple match {
      case GoalTuple("meta", List(to, from, time), _, _)
        if from != to && to != ProvenanceReader.WILDCARD => Set((from, to, time.toInt))
      case _ => Set.empty
    }
    childrenClocks ++ newClock
  }

  override lazy val enumerateDistinctDerivations: Set[Option[GoalNode]] = {
    Set(Some(this))
  }

}

/**
 * Represents a concrete application of a rule.
 *
 * @param rule the rule that was applied.
 * @param subgoals the facts that were used in this rule application.
 */
case class RuleNode(id: Int, rule: Rule, positiveSubgoals: Set[GoalNode], negativeSubgoals: Set[GoalNode]) extends HashcodeCaching {
  require (!positiveSubgoals.isEmpty, "RuleNode must have subgoals")
  lazy val enumerateDistinctDerivationsOfSubGoals: Option[List[RuleNode]] = {
    val posChoices: List[List[GoalNode]] = positiveSubgoals.map(_.enumerateDistinctDerivations.flatten.toList).toList
    val negChoices: List[List[GoalNode]] = negativeSubgoals.map(_.enumerateDistinctDerivations.flatten.toList).toList

    if (posChoices.isEmpty || posChoices.forall(s => s.isEmpty)) {
      None
    } else {
      val pos = posChoices.filter(!_.isEmpty).map(subgoalDerivations => this.copy(positiveSubgoals = subgoalDerivations.toSet)).toList
      val neg = negChoices.filter(!_.isEmpty).map(subgoalDerivations => this.copy(negativeSubgoals = subgoalDerivations.toSet)).toList
      //System.out.println(s"pos is $pos . neg is $neg")
      //if (pos.isEmpty) { System.out.println("POS is empty foo")}
      Some(pos ++ neg)
    }
  }
  lazy val subgoals: Set[GoalNode] = {
    positiveSubgoals ++ negativeSubgoals
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

  val messages = getMessages

  private val derivationBuilding = metrics.timer("derivation-tree-building")

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

  private def buildPhonyDerivationTree(goalTuple: GoalTuple): PhonyGoalNode = {
    val msgs = getContributingMessages(goalTuple)
    logger.debug(s"$goalTuple phony msgs: $msgs")
    PhonyGoalNode(nextGoalNodeId.getAndIncrement, goalTuple, msgs.map(getPhonyDerivationTree))
  }

  private def buildDerivationTree(goalTuple: GoalTuple): GoalNode = {
    logger.debug(s"Reading provenance for tuple $goalTuple")
    // First, check whether the goal tuple is part of the EDB:
    if (isInEDB(goalTuple)) {
      logger.debug(s"Found $goalTuple in EDB")
      return RealGoalNode(nextGoalNodeId.getAndIncrement, goalTuple, Set.empty)
    }  else if (goalTuple.negative && model.tables(goalTuple.table).contains(goalTuple.cols)) {
      logger.debug(s"$goalTuple.table contains $goalTuple !!")
      return RealGoalNode(nextGoalNodeId.getAndIncrement, goalTuple.copy(tombstone = true), Set.empty)
      // if it's a neg tuple, discharge it.
    }

    // Otherwise, a rule must have derived it:
    val ruleFirings = findRuleFirings(goalTuple)
    if (goalTuple.negative) {
      logger.debug(s"NEG ($goalTuple) anti-rf $ruleFirings")
      if (ruleFirings.isEmpty) return RealGoalNode(nextGoalNodeId.getAndIncrement, goalTuple.copy(tombstone = true), Set.empty)
      logger.debug(s"$goalTuple FALLTHRU")
    } else {
      if (ruleFirings.isEmpty) {
        logger.debug("NO DERIV $goalTuple")
        return RealGoalNode(nextGoalNodeId.getAndIncrement, goalTuple.copy(tombstone = true), Set())
        //assert (!ruleFirings.isEmpty, s"Couldn't find rules to derive tuple $goalTuple")
      }
    }
    logger.debug(s"Rule firings: $ruleFirings")
    val ruleNodes = ruleFirings.map { case (provRuleName, provTableRow) =>
      val provRule = program.rules.find(_.head.tableName == provRuleName).get
      val bindings = provRowToVariableBindings(provRule, provTableRow)
      logger.debug(s"look up for $provRule AND $provTableRow")
      // PAA
      //val time = bindings("NRESERVED").toInt
      logger.debug(s"bindings: $bindings")
      val timevar = bindings.getOrElse("NRESERVED", "-1")
      if (timevar == ProvenanceReader.WILDCARD) {
        // punt on this for now.
        return RealGoalNode(nextGoalNodeId.getAndIncrement, goalTuple, Set.empty)
      }
      val time = timevar.toInt

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

      val nsg = negativeGoals.map(g => getDerivationTree(g))
      val psg = positiveGoals.map(g => getDerivationTree(g))
      // Recursively compute the provenance of the new goals:
      if (goalTuple.negative) {
        RuleNode(nextRuleNodeId.getAndIncrement, provRule, positiveGoals.map(g => getDerivationTree(g.copy(negative = true))).toSet,
          negativeGoals.map(g => getDerivationTree(g.copy(negative = false))).toSet)
      } else{
        RuleNode(nextRuleNodeId.getAndIncrement, provRule, positiveGoals.map(getDerivationTree).toSet,
          negativeGoals.map(g => getDerivationTree(g.copy(negative = true))).toSet)
      }
    }
    RealGoalNode(nextGoalNodeId.getAndIncrement, goalTuple, ruleNodes)
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

  private def findRuleFirings(goalTuple: GoalTuple): Set[(String, List[String])] = {
    // Find provenance tables that might explain how we derived `goalTuple`:
    val provTables = program.tables.map(_.name).filter(_.matches(s"^${goalTuple.table}_prov\\d+"))
    logger.debug(s"Table '${goalTuple.table}' has provenance tables $provTables")
    // Check which of them have matching facts:
    if (goalTuple.negative) {
      provTables.map { table =>
        val res = searchProvTable(goalTuple.cols, model.tables(table))
        logger.debug(s"res $res.  prov table schema is $table ")
        val provRule = program.rules.find(_.head.tableName == table).get
        val cols = if (provRule.head.time.isDefined && goalTuple.cols.last != ProvenanceReader.WILDCARD) {
          // unless time itself is a wildcard (in which case, what do?)  keep it on
          goalTuple.cols.init ++ List((goalTuple.cols.last.toInt - 1).toString)
        } else {
          goalTuple.cols
        }
        (table, res, cols)
      }.filter(x => !x._2.isDefined && (x._3.last == ProvenanceReader.WILDCARD || x._3.last.toInt > 0)).map(x => (x._1, x._3))
    } else {
      provTables.map(table => (table, searchProvTable(goalTuple.cols, model.tables(table))))
        .filter(_._2.isDefined).map(x => (x._1, x._2.get))
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
    //require(provRule.head.cols.size == provTableRow.size, s"Incorrect number of columns $provRule.head vs $provTableRow")
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
