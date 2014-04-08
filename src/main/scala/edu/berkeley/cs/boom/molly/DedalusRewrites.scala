package edu.berkeley.cs.boom.molly

import scala.Some
import edu.berkeley.cs.boom.molly.ast._

object DedalusRewrites {

  private val dc = Identifier("_")
  private val nreserved = Identifier("NRESERVED")
  private val mreserved = Identifier("MRESERVED")

  /**
   * Modify a program's rules and facts to reference a clock relation.
   *
   * Assumes that the first column of each predicate is a location specifier.
   */
  def referenceClockRules(program: Program): Program = {
    def nextClock(loc: Atom) =
      Predicate("clock", List(loc, dc, nreserved, dc), notin = false, None)
    def localClock(loc: Atom) =
      Predicate("clock", List(loc, loc, nreserved, dc), notin = false, None)
    def asyncClock(from: Atom, to: Atom) =
      Predicate("clock", List(from, to, nreserved, mreserved), notin = false, None)

    def rewriteHead(pred: Predicate, time: Time): Predicate = time match {
      case Next() => pred.copy(cols = pred.cols ++ List(Expr(nreserved, "+", IntLiteral(1))))
      case Async() => pred.copy(cols = pred.cols ++ List(mreserved))
      case Tick(t) => pred.copy(cols = pred.cols ++ List(IntLiteral(t)))
    }

    def rewriteBodyElem(elem: Either[Predicate, Expr]): Either[Predicate, Expr] =
      elem match {
        case r @ Right(expr) => r
        case Left(pred) => pred.time match {
          case Some(Tick(t)) => Left(pred.copy(cols = pred.cols ++ List(IntLiteral(t))))
          case _ => Left(pred.copy(cols = pred.cols ++ List(nreserved)))
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
            Rule(head.copy(cols = head.cols ++ List(nreserved)), body.map(rewriteBodyElem) ++ List(Left(localClock(loc))))
          case Some(time) =>
            time match {
              case Next() =>
                Rule(rewriteHead(head, Next()), body.map(rewriteBodyElem) ++ List(Left(nextClock(loc))))
              case Async() =>
                val from = rule.bodyPredicates(0).cols(0)
                val to = head.cols(0)
                Rule(rewriteHead(head, Async()), body.map(rewriteBodyElem) ++ List(Left(asyncClock(from, to))))
              case Tick(number) =>
                throw new IllegalStateException("Rule head can't only hold at a specific time step")
            }
        }
    }

    // TODO: handle facts without times => add persistence rules:
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
