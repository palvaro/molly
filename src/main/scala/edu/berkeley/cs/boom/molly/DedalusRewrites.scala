package edu.berkeley.cs.boom.molly

import edu.berkeley.cs.boom.molly.ast._

object DedalusRewrites {

  val dc = Identifier("_")
  val nreserved = Identifier("NRESERVED")
  val mreserved = Identifier("MRESERVED")

  /**
   * Modify a program's rules and facts to reference a clock relation.
   */
  def referenceClockRules(program: Program): Program = {
    def nextClock(loc: Atom) =
      Predicate("clock", List(loc, dc, nreserved, dc), notin = false, None)
    def localClock(loc: Atom) =
      Predicate("clock", List(loc, loc, nreserved, dc), notin = false, None)
    def asyncClock(from: Atom, to: Atom) =
      Predicate("clock", List(from, to, nreserved, mreserved), notin = false, None)

    def appendCol(col: Atom)(pred: Predicate): Predicate =
      pred.copy(cols = pred.cols :+ col)

    def rewriteHead(pred: Predicate, time: Time): Predicate = time match {
      case Next()  => appendCol(Expr(nreserved, "+", IntLiteral(1)))(pred)
      case Async() => appendCol(mreserved)(pred)
      case Tick(t) => appendCol(IntLiteral(t))(pred)
    }

    def rewriteBodyElem(elem: Either[Predicate, Expr]): Either[Predicate, Expr] =
      elem.left.map { pred =>
        pred.time match {
          case Some(Tick(t)) => appendCol(IntLiteral(t))(pred)
          case _ => appendCol(nreserved)(pred)
        }
      }

    def rewriteRule(rule: Rule): Rule = rule match {
      case Rule(head, body) =>
        val loc = rule.locationSpecifier
        rule.head.time match {
          case None =>
            // For local rules, we still need to reference the clock in order to guarantee that the
            // clock variable appears in a non-negated body predicate.  We use localClock in order
            // to reduce the number of possible derivations.
            Rule(appendCol(nreserved)(head), body.map(rewriteBodyElem) ++ List(Left(localClock(loc))))
          case Some(Next()) =>
            Rule(rewriteHead(head, Next()), body.map(rewriteBodyElem) ++ List(Left(nextClock(loc))))
          case Some(Async()) =>
            val to = head.cols(0)
            Rule(rewriteHead(head, Async()), body.map(rewriteBodyElem) ++ List(Left(asyncClock(loc, to))))
          case Some(Tick(number)) =>
            throw new IllegalStateException("Rule head can't only hold at a specific time step")
        }
    }

    program.copy(rules = program.rules.map(rewriteRule), facts = program.facts.map(f => rewriteHead(f, f.time.get)))
  }

  /**
   * Splits a rule with an aggregation in its head into two separate rules,
   * one that binds variables and another that performs the aggregation.
   *
   * For example, the rule
   *    agg(X, count<Y>) :- a(X, Z), b(Z, Y)
   * is rewritten as two rules:
   *    agg_vars(X, Y, Z) :- a(X, Z), b(Z, Y)
   *    agg(X, count<Y>) :- agg_vars(X, Y, _)
   */
  def splitAggregateRules(program: Program): Program = {
    val (rules, aggRules) = program.rules.partition(_.head.aggregateVariables.isEmpty)
    program.copy(rules = rules ++ aggRules.flatMap(splitAggregateRule))
  }

  private def splitAggregateRule(rule: Rule): Seq[Rule] = {
    assert (!rule.head.aggregateVariables.isEmpty, "Expected rule with aggregation")
    val ruleSansAgg =
      rule.copy(head = rule.head.copy(cols = rule.head.cols.filterNot(_.isInstanceOf[Aggregate])))
    val varsRule = recordAllVariableBindings(ruleSansAgg, ruleSansAgg.head.tableName + "_vars")
    val aggRuleBody = varsRule.head.cols.collect {
      case i @ Identifier(x) =>
        if (rule.head.topLevelVariables.contains(x) || rule.head.aggregateVariables.contains(x)) i
        else dc
    }
    val aggRule = rule.copy(body = List(Left(varsRule.head.copy(time = None, cols = aggRuleBody))))
    Seq(aggRule, varsRule)
  }

  /**
   * Modify a rule head to record ALL variables, even ones that are only bound in aggregates.
   */
  private def recordAllVariableBindings(rule: Rule, newTableName: String): Rule = {
    val newVariables =
      (rule.head.expressionVariables ++ rule.variables) -- rule.head.topLevelVariables
    // Produce a new head, preserving the last time column:
    val newHead = rule.head.copy(tableName = newTableName,
      cols = rule.head.cols.take(rule.head.cols.size - 1) ++
        newVariables.map(Identifier).filter(_ != dc) ++ List(rule.head.cols.last))
    rule.copy(head = newHead)
  }

  /**
   * Modify a rule head to record bound variables
   */
  private def recordBoundVariables(rule: Rule, newTableName: String): Rule = {
    val newVariables =
      (rule.head.expressionVariables ++ rule.boundVariables) -- rule.head.topLevelVariables
    // Produce a new head, preserving the last time column:
    val newHead = rule.head.copy(tableName = newTableName,
      cols = rule.head.cols.take(rule.head.cols.size - 1) ++
        newVariables.map(Identifier).filter(_ != dc) ++ List(rule.head.cols.last))
    rule.copy(head = newHead)
  }

  /**
   * Add rules and rewrite rule bodies to record provenance.
   */
  def addProvenanceRules(program: Program): Program = {
    val provenanceRules = program.rules.zipWithIndex.map { case (rule, number) =>
      recordBoundVariables(rule, rule.head.tableName + "_prov" + number)
    }
    program.copy(rules = program.rules ++ provenanceRules)
  }

}
