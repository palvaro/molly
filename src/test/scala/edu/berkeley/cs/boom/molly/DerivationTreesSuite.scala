package edu.berkeley.cs.boom.molly

import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSuite, Matchers}

import edu.berkeley.cs.boom.molly.ast.Rule
import edu.berkeley.cs.boom.molly.derivations._

class DerivationTreesSuite extends FunSuite with Matchers with MockitoSugar {

  private def clockTuple(from: String, to: String, sendTime: Int): GoalTuple = {
    GoalTuple("clock", List(from, to, sendTime.toString, (sendTime + 1).toString),
      negative = false, tombstone = false)
  }

  private def edb(tuple: GoalTuple): GoalNode = {
    new RealGoalNode(tuple, Set.empty, negative = false)
  }

  private def goal(tuple: GoalTuple, rules: RuleNode*): GoalNode =
    RealGoalNode(tuple, rules.toSet, negative = false)

  private def rule(goals: GoalNode*): RuleNode = RuleNode(mock[Rule], goals.toSet)

  test("edb fact") {
    val tuple = GoalTuple("fact", List("a"), negative = false, tombstone = false)
    val goal = edb(tuple)
    goal.ownImportantClock should be (None)
    goal.importantClocks should be (empty)
    goal.enumerateDistinctDerivations should be (Set(goal))
    goal.allTups should be (Set(tuple))
  }

  test("clock fact") {
    val tuple = clockTuple("from", "to", 1)
    val goal = edb(tuple)
    goal.ownImportantClock should be (Some("from", "to", 1))
    goal.importantClocks should be (Set(("from", "to", 1)))
    goal.enumerateDistinctDerivations should be (Set(goal))
    goal.allTups should be (Set(goal.tuple))
  }

  test("goal with two rule firings") {
    val logA1 = GoalTuple("log", List("A", "data", "1"), negative = false, tombstone = false)
    val logA2 = GoalTuple("log", List("A", "data", "2"), negative = false, tombstone = false)
    val logB1 = GoalTuple("log", List("B", "data", "1"), negative = false, tombstone = false)
    val persistenceRule = RuleNode(mock[Rule], Set(edb(logA1), edb(clockTuple("A", "A", 1))))
    val sendRule = RuleNode(mock[Rule], Set(edb(logB1), edb(clockTuple("B", "A", 1))))
    val goal = RealGoalNode(logA2, Set(persistenceRule, sendRule), negative = false)
    goal.importantClocks should be (Set(("B", "A", 1)))
    goal.enumerateDistinctDerivations should be (Set(
      goal.copy(pRules = Set(persistenceRule)),
      goal.copy(pRules = Set(sendRule))
    ))
    goal.allTups should be (Set(
      logA1, logA2, logB1, clockTuple("A", "A", 1), clockTuple("B", "A", 1)
    ))
  }

  test("cross-product of trees") {
    val root = GoalTuple("root", Nil, negative = false, tombstone = false)
    val a = GoalTuple("a", Nil, negative = false, tombstone = false)
    val b = GoalTuple("b", Nil, negative = false, tombstone = false)
    val c = GoalTuple("c", Nil, negative = false, tombstone = false)
    val d = GoalTuple("d", Nil, negative = false, tombstone = false)

    val tree = goal(root,
      rule(
        goal(a,
          rule(edb(c)),
          rule(edb(d))
        )
      ),
      rule(
        goal(b,
          rule(edb(c)),
          rule(edb(d))
        )
      )
    )
    tree.enumerateDistinctDerivations.size should be (4)
    tree.enumerateDistinctDerivations.map(_.allTups) should be (Set(
      Set(root, a, c), Set(root, a, d), Set(root, b, c), Set(root, b, d)
    ))
  }
}
