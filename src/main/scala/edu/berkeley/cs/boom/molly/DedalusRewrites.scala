package edu.berkeley.cs.boom.molly

import scala.Some
import edu.berkeley.cs.boom.molly.ast._

object DedalusRewrites {

  private val dc = Identifier("_")
  private val nreserved = Identifier("NRESERVED")
  private val mreserved = Identifier("MRESERVED")

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
   * Add rules and rewrite rule bodies to record provenance.
   */
  def addProvenanceRules(program: Program): Program = {
    val provenanceRules = program.rules.zipWithIndex.map { case (rule, number) =>
      // Rename the rule head and modify it to record variable bindings.
      // We also record the values of variables that appear in expressions in order to
      // avoid the need to invert those expressions to recover the variable bindings
      // inside the ProvenanceReader.
      val newVariables =
        (rule.head.variablesInExpressions ++ rule.boundVariables) -- rule.head.variables
      // Produce a new head, preserving the last time column:
      val newHead = rule.head.copy(tableName = rule.head.tableName + "_prov" + number,
        cols = rule.head.cols.take(rule.head.cols.size - 1) ++
          newVariables.map(Identifier).filter(_ != dc) ++ List(rule.head.cols.last))
      rule.copy(head = newHead)
    }
    program.copy(rules = program.rules ++ provenanceRules)
  }

}
