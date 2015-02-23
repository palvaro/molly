package edu.berkeley.cs.boom.molly.derivations

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.immutable.{Nil, Set, List}

import scalaz._
import Scalaz._

import edu.berkeley.cs.boom.molly.ast.{Identifier, Predicate, Rule}
import edu.berkeley.cs.boom.molly.util.HashcodeCaching

object DerivationTrees {
  val nextRuleNodeId = new AtomicInteger(0)
  val nextGoalNodeId = new AtomicInteger(0)

  /**
   * Constructs a dummy rule node that doesn't correspond to a program rule.
   * Used by neg. provenance extraction.
   */
  def dummyRuleNode(name: String, subgoals: Set[GoalNode]): RuleNode = {
    val dummyPred = new Predicate(name, List(Identifier("_")), false, None)
    val dummyRule = new Rule(dummyPred, List(Left(dummyPred)))
    RuleNode(dummyRule, subgoals)
  }
}

case class GoalTuple(
  table: String,
  cols: List[String],
  negative: Boolean = false,
  tombstone: Boolean = false) {

  override def toString: String = {
    (if (negative) "NOT!" else "") +
    (if (tombstone) "TOMB" else "") +
    table +
    "(" + cols.mkString(", ") + ")"
  }
}

trait DerivationTreeNode

/**
 * Represents a goal (fact to be proved).  Appears at the root of the rule-goal graph.
 */
trait GoalNode extends DerivationTreeNode {
  /**
   * A unique identifier for this goal node.
   */
  val id: Int = DerivationTrees.nextGoalNodeId.getAndIncrement

  /**
   * The tuple that this goal represents.
   */
  val tuple: GoalTuple

  /**
   * The set of messages which, if omitted, _might_ invalidate this goal.
   */
  lazy val importantClocks: Set[(String, String, Int)] = Set()

  /**
   * If this goal node is an important clock, returns that clock
   */
  lazy val ownImportantClock: Option[(String, String, Int)] = None

  /**
   * For the derivation tree rooted at this node, yield a set of trees where each tree
   * corresponds to a distinct derivation of this goal. For example, if this goal is
   * provable with two rules, then split this tree into two trees, one which considers
   * derivations using the first goal and one which considers the other goal, then
   * continue this process recursively.
   */
  lazy val enumerateDistinctDerivations: Set[(GoalNode)] = Set()

  /**
   * The set of rule firings that derive this goal.
   */
  lazy val rules: Set[RuleNode] = Set()

  /**
   * Return all tuples appearing anywhere in the tree rooted at this GoalTuple, including
   * the root of the tree itself.
   */
  def allTups: Set[GoalTuple] = Set()
}

case class RealGoalNode(
  tuple: GoalTuple,
  pRules: Set[RuleNode],
  negative: Boolean = false
) extends HashcodeCaching with GoalNode {

  override lazy val rules = pRules

  override lazy val ownImportantClock = {
    tuple match {
      case GoalTuple("clock", List(from, to, time, _), _, _)
        if from != to
          && to != ProvenanceReader.WILDCARD
          && from != ProvenanceReader.WILDCARD => Some((from, to, time.toInt))
      case _ => None
    }
  }

  override lazy val importantClocks: Set[(String, String, Int)] = {
    val childrenClocks = rules.flatMap(_.subgoals).flatMap(_.importantClocks)
    childrenClocks ++ ownImportantClock.toSet
  }

  override lazy val enumerateDistinctDerivations: Set[GoalNode] = {
    if (rules.isEmpty) {
      if (tuple.tombstone) Set() else Set(this)
    } else {
      rules.flatMap{ r =>
        val dd = r.enumerateDistinctDerivationsOfSubGoals
        dd.map(d => this.copy(pRules = Set(d)))
      }
    }
  }

  override def allTups: Set[GoalTuple] = {
    Set(tuple) ++ rules.flatMap(r => r.subgoals.flatMap(s => s.allTups))
  }
}

case class PhonyGoalNode(
  tuple: GoalTuple,
  history: Set[GoalNode]
) extends GoalNode {

  override lazy val ownImportantClock = {
    tuple match {
      case GoalTuple("meta", List(to, from, time), _, _)
        if from != to && to != ProvenanceReader.WILDCARD => Some((from, to, time.toInt))
      case _ => None
    }
  }

  override lazy val importantClocks: Set[(String, String, Int)] = {
    val childrenClocks = history.flatMap(_.importantClocks)
    childrenClocks ++ ownImportantClock.toSet
  }

  override lazy val enumerateDistinctDerivations: Set[GoalNode] = {
    Set(this)
  }

}

/**
 * Represents a concrete application of a rule.
 */
case class RuleNode(
  rule: Rule,
  subgoals: Set[GoalNode]
) extends HashcodeCaching with DerivationTreeNode {

  require (!subgoals.isEmpty, "RuleNode must have subgoals")
  val id = DerivationTrees.nextRuleNodeId.getAndIncrement
  lazy val enumerateDistinctDerivationsOfSubGoals: List[RuleNode] = {
    val choices: List[List[GoalNode]] = subgoals.map(_.enumerateDistinctDerivations.toList).toList
    if (choices.exists(_.isEmpty)) {
      // There's an underivable subgoal, so this rule couldn't have fired.
      Nil
    } else {
      choices.sequence.map(subgoalDerivations => this.copy(subgoals = subgoalDerivations.toSet))
    }
  }
}

case class Message(
  table: String,
  from: String,
  to: String,
  sendTime: Int,
  receiveTime: Int)