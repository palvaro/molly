package edu.berkeley.cs.boom.molly

import org.scalatest.{FunSuite, Matchers}

import edu.berkeley.cs.boom.molly.ast._

class ASTSuite extends FunSuite with Matchers {

  test("Expr.variables") {
    Expr(IntLiteral(42), "", IntLiteral(24)).variables should be (empty)
    Expr(Identifier("a"), "", IntLiteral(42)).variables.map(_.name) should be (Set("a"))
    Expr(IntLiteral(42), "", Identifier("a")).variables.map(_.name) should be (Set("a"))
    Expr(Identifier("a"), "", Identifier("b")).variables.map(_.name) should be (Set("a", "b"))
    val nestedExpression = Expr(Identifier("a"), "", Expr(Identifier("b"), "", Identifier("c")))
    nestedExpression.variables.map(_.name) should be (Set("a", "b", "c"))
  }

  test("Predicate.*Variables") {
    val edbPredicate =
      Predicate("edb", List(IntLiteral(42), StringLiteral("foo")), notin = false, None)
    edbPredicate.topLevelVariables should be (empty)
    edbPredicate.aggregateVariables should be (empty)
    edbPredicate.expressionVariables should be (empty)
    edbPredicate.topLevelVariablesWithIndices should be (empty)

    val idbPredicate = {
      val cols = List(
        IntLiteral(42),
        StringLiteral("a"),
        Identifier("ident"),
        Aggregate("max", "aggColumn"),
        Expr(Identifier("inExprA"), "<=", Identifier("inExprB"))
      )
      Predicate("tableName", cols, notin = false, None)
    }
    idbPredicate.topLevelVariables should be (Set("ident"))
    idbPredicate.aggregateVariables should be (Set("aggColumn"))
    idbPredicate.expressionVariables should be (Set("inExprA", "inExprB"))
    idbPredicate.topLevelVariablesWithIndices should be (List(("ident", ("tableName", 2))))
  }
}
