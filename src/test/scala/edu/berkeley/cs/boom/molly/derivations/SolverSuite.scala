package edu.berkeley.cs.boom.molly.derivations

import org.scalatest.{FunSuite, Matchers}

import edu.berkeley.cs.boom.molly.FailureSpec

class SolverSuite extends FunSuite with Matchers {

  test("solutionToFailureSpec should remove redundant message losses") {
    val originalFailureSpec = FailureSpec(eot = 4, eff = 2, maxCrashes = 1, nodes = List("A", "B"))
    val solution: Set[SolverVariable] = Set(
      CrashFailure("A", 2),
      MessageLoss("A", "B", 1), // Before crash, so this message should still be included
      MessageLoss("A", "B", 2), // same time as crash, so this should be removed
      MessageLoss("A", "B", 3), // after crash, so this should also be removed
      MessageLoss("B", "A", 1) // arrives at receiver while receiver crashes, so should be removed
    )
    val failureSpec = Solver.solutionToFailureSpec(originalFailureSpec, solution).get
    failureSpec.crashes should be (Set(CrashFailure("A", 2)))
    failureSpec.omissions should be (Set(MessageLoss("A", "B", 1)))
  }

  test("solutionToFailureSpec should not return failure-free specs") {
    val originalFailureSpec = FailureSpec(eot = 4, eff = 2, maxCrashes = 1, nodes = List("A", "B"))
    Solver.solutionToFailureSpec(originalFailureSpec, Set.empty) should be (None)
  }

}
