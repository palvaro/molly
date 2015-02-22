package edu.berkeley.cs.boom.molly

import org.scalatest.{FunSuite, Matchers}

import edu.berkeley.cs.boom.molly.ast._
import edu.berkeley.cs.boom.molly.DedalusRewrites.{dc, nreserved, mreserved}

class DedalusRewritesSuite extends FunSuite with Matchers {

  test("clock rewrite with deductive rule") {
    val node = Identifier("Node")
    val pload = Identifier("Pload")
    val rules = List(
      Rule(
        Predicate("log", List(node, pload), notin = false, time = None),
        List(Left(Predicate("bcast", List(node, pload), notin = false, time = None))))
    )
    val program = Program(rules, facts = Nil, includes = Nil, tables = Set.empty)
    val rewrittenProgram = DedalusRewrites.referenceClockRules(program)
    val expectedRules = List(
      Rule(
        Predicate("log", List(node, pload, nreserved), notin = false, time = None),
        List(
          Left(Predicate("bcast", List(node, pload, nreserved), notin = false, time = None)),
          Left(Predicate("clock", List(node, node, nreserved, dc), notin = false, time = None))
        ))
    )
    rewrittenProgram.rules should be (expectedRules)
  }

  test("clock rewrite with inductive rule") {
    val node = Identifier("Node")
    val neighbor = Identifier("Neighbor")
    val rules = List(
      Rule(
        Predicate("node", List(node, neighbor), notin = false, time = Some(Next())),
        List(Left(Predicate("node", List(node, neighbor), notin = false, time = None))))
    )
    val program = Program(rules, facts = Nil, includes = Nil, tables = Set.empty)
    val rewrittenProgram = DedalusRewrites.referenceClockRules(program)
    val sendTimePlus1 = Expr(nreserved, "+", IntLiteral(1))
    val expectedRules = List(
      Rule(
        Predicate("node", List(node, neighbor, sendTimePlus1), notin = false, time = Some(Next())),
        List(
          Left(Predicate("node", List(node, neighbor, nreserved), notin = false, time = None)),
          Left(Predicate("clock", List(node, dc, nreserved, dc), notin = false, time = None))
        ))
    )
    rewrittenProgram.rules should be (expectedRules)
  }

  test("clock rewrite with async rule") {
    val node1 = Identifier("Node1")
    val node2 = Identifier("Node2")
    val pload = Identifier("Pload")
    val rules = List(
      Rule(
        Predicate("log", List(node2, pload), notin = false, time = Some(Async())),
        List(
          Left(Predicate("bcast", List(node1, pload), notin = false, time = None)),
          Left(Predicate("node", List(node1, node2), notin = false, time = None))
        ))
    )
    val program = Program(rules, facts = Nil, includes = Nil, tables = Set.empty)
    val rewrittenProgram = DedalusRewrites.referenceClockRules(program)
    val expectedRules = List(
      Rule(
        Predicate("log", List(node2, pload, mreserved), notin = false, time = Some(Async())),
        List(
          Left(Predicate("bcast", List(node1, pload, nreserved), notin = false, time = None)),
          Left(Predicate("node", List(node1, node2, nreserved), notin = false, time = None)),
          Left(Predicate("clock", List(node1, node2, nreserved, mreserved), notin = false, time = None))
        ))
    )
    rewrittenProgram.rules should be (expectedRules)
  }

}
