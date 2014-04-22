package edu.berkeley.cs.boom.molly

import org.scalatest.{Matchers, FunSpec}


class FailureSpecSuite extends FunSpec with Matchers {

  describe("gross estimate of runs") {

    describe("for the failure-free scenario") {
      it("should estimate only one run") {
        FailureSpec(3, 0, 0, List("a", "b", "c")).grossEstimate should be (1)
      }
    }

    describe("for fail-stop scenarios") {
      it("should treat failures independently") {
        FailureSpec(3, 0, 2, List("a", "b")).grossEstimate should be (16)
      }
      it("should allow each node to crash once or never") {
        FailureSpec(3, 0, 1, List("a")).grossEstimate should be (4)
      }
      it("should respect maxCrashes") {
        // Condition on number of crashes:
        // There are two nodes that never crash, and there are 6 ways of picking those
        // two nodes.  Those nodes only have one failure schedule each.
        // The nodes that _are_ crash prone can crash at one of four times.
        // So: 4 * 4 * 6 = 96
        FailureSpec(3, 0, 2, List("a", "b", "c", "d")).grossEstimate should be (96)
      }
    }

    describe("for omission-only scenarios") {
      it("should allow omissions until eff") {
        FailureSpec(3, 2, 0, List("a", "b")).grossEstimate should be (16)
        FailureSpec(3, 1, 0, List("a", "b")).grossEstimate should be (4)
      }
    }

    describe("for scenarios with both crashes and omissions") {
      it("should prevent crashed nodes from sending messages") {
        // With a naive estimate that treated omissions and crashes independently, each node has
        // 4 choices of times to crash and 4 possible combinations of message omissions.
        // There are two nodes, so we have (4 * 4) ** 2 = 256 possible failure scenarios.
        // However, if we condition on when the node crashes:
          // Crash @t=1  -> 1 choices of omissions
          // crash @t=2  -> 2 choices
          // crash @t=3  -> 4 choices
          // Never crash -> 4 choices
        // So 11 ** 2 = 121 possible failure scenarios.
        FailureSpec(3, 2, 2, List("a", "b")).grossEstimate should be (121)
      }
    }
  }
}
