package edu.berkeley.cs.boom.molly

import org.scalatest.{Matchers, FunSuite}
import edu.berkeley.cs.boom.molly.ast.{Aggregate, Expr}

class DedalusRewritesSuite extends FunSuite with Matchers {
  import DedalusRewrites._

  test("provenance rewrite properly handles aggregates and expressions appearing in rule head") {
    val src = "good(A+B, count<C>) :- fact(A, B, C);"
    val program = DedalusParser.parseProgram(src)
    val rewritten = addProvenanceRules(referenceClockRules(program))
    rewritten.rules.size should be (2)
    val goodRule = rewritten.rules.filter(_.head.tableName == "good").head
    val goodRulePredicates = goodRule.head :: goodRule.bodyPredicates
    val exprsOrAggs =
      goodRulePredicates.flatMap(_.cols).filter(c => c.isInstanceOf[Expr] || c.isInstanceOf[Aggregate])
    exprsOrAggs should be (empty)
  }
}
