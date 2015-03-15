package edu.berkeley.cs.boom.molly.symmetry

import org.scalatest.FunSuite

import edu.berkeley.cs.boom.molly.FailureSpec
import edu.berkeley.cs.boom.molly.ast._
import edu.berkeley.cs.boom.molly.derivations.{MessageLoss, CrashFailure}

class SymmetryCheckerSuite extends FunSuite {
  test("simple symmetry with an empty EDB and no rules") {
    val program = Program(
      rules = Nil,
      facts = Nil,
      includes = Nil,
      tables = Set.empty
    )
    val nodes = List("A", "B", "C")
    val symmetryChecker = new SymmetryChecker(program, nodes)
    val a = FailureSpec(4, 3, 2, nodes, crashes = Set(CrashFailure("A", 2)))
    val b = FailureSpec(4, 3, 2, nodes, crashes = Set(CrashFailure("B", 2)))
    val c = FailureSpec(4, 3, 2, nodes, crashes = Set(CrashFailure("C", 2)))
    assert(symmetryChecker.areEquivalentForEDB(a, b))
    assert(symmetryChecker.areEquivalentForEDB(b, c))
    assert(symmetryChecker.areEquivalentForEDB(c, a))

    val d = FailureSpec(4, 3, 2, nodes, omissions = Set(MessageLoss("A", "B", 2)))
    val e = FailureSpec(4, 3, 2, nodes, omissions = Set(MessageLoss("B", "A", 2)))
    val f = FailureSpec(4, 3, 2, nodes, omissions = Set(MessageLoss("B", "A", 1)))
    assert(symmetryChecker.areEquivalentForEDB(d, e))
    assert(!symmetryChecker.areEquivalentForEDB(a, e))
    assert(!symmetryChecker.areEquivalentForEDB(e, f))
  }

  test("nodes that appear as literals in rules are excluded from potential symmetries") {
    // This is a conservative approach that may miss out on some potential symmetries if the
    // rules themselves exhibit certain types of symmetry
    val program = Program(
      // Dummy rule so that "A" is marked as a location literal that appears in a rule body
      rules = List(
        Rule(
          Predicate("table", List(Identifier("Location")), notin = false, None),
          List(Left(Predicate("table", List(StringLiteral("A")), notin = false, None)))
        )
      ),
      facts = Nil,
      includes = Nil
    )
    val nodes = List("A", "B", "C")
    val symmetryChecker = new SymmetryChecker(program, nodes)
    val a = FailureSpec(4, 3, 2, nodes, crashes = Set(CrashFailure("A", 2)))
    val b = FailureSpec(4, 3, 2, nodes, crashes = Set(CrashFailure("B", 2)))
    val c = FailureSpec(4, 3, 2, nodes, crashes = Set(CrashFailure("C", 2)))
    assert(!symmetryChecker.areEquivalentForEDB(a, b))
    assert(symmetryChecker.areEquivalentForEDB(b, c))
    assert(!symmetryChecker.areEquivalentForEDB(c, a))
  }

  test("EDB symmetry is required for symmetry") {
    val program = Program(
      rules = Nil,
      facts = List(
        // A and B are symmetric with respect to this EDB, while A and C are not
        Predicate("foo", List(StringLiteral("A"), IntLiteral(1)), notin = false, None),
        Predicate("foo", List(StringLiteral("B"), IntLiteral(1)), notin = false, None),
        Predicate("foo", List(StringLiteral("C"), IntLiteral(2)), notin = false, None)
      ),
      includes = Nil
    )
    val nodes = List("A", "B", "C")
    val symmetryChecker = new SymmetryChecker(program, nodes)
    val a = FailureSpec(4, 3, 2, nodes, crashes = Set(CrashFailure("A", 2)))
    val b = FailureSpec(4, 3, 2, nodes, crashes = Set(CrashFailure("B", 2)))
    val c = FailureSpec(4, 3, 2, nodes, crashes = Set(CrashFailure("C", 2)))
    assert(symmetryChecker.areEquivalentForEDB(a, b))
    assert(!symmetryChecker.areEquivalentForEDB(b, c))
  }
}
