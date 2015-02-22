package edu.berkeley.cs.boom.molly

import org.scalatest.{FunSuite, Matchers}

import edu.berkeley.cs.boom.molly.ast._

class ASTSuite extends FunSuite with Matchers {

  test("Expr.variables") {
    Expr(IntLiteral(42), "", IntLiteral(24)).variables should be (Set.empty)
    Expr(Identifier("a"), "", IntLiteral(42)).variables.map(_.name) should be (Set("a"))
    Expr(IntLiteral(42), "", Identifier("a")).variables.map(_.name) should be (Set("a"))
    Expr(Identifier("a"), "", Identifier("b")).variables.map(_.name) should be (Set("a", "b"))
    val nestedExpression = Expr(Identifier("a"), "", Expr(Identifier("b"), "", Identifier("c")))
    nestedExpression.variables.map(_.name) should be (Set("a", "b", "c"))
  }
}
