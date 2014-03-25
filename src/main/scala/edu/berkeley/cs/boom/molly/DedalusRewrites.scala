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
    def asyncClock(loc: Atom) =
      Predicate("clock", List(loc, dc, nreserved, mreserved), notin = false, None)

    def rewriteHead(pred: Predicate, time: Time): Predicate = time match {
      case Next() => pred.copy(cols = pred.cols ++ List(Expr(nreserved, "+", IntLiteral(1))))
      case Async() => pred.copy(cols = pred.cols ++ List(mreserved))
      case Tick(t) => pred.copy(cols = pred.cols ++ List(IntLiteral(t)))
    }

    def rewriteBodyElem(elem: Either[Predicate, Expr]): Either[Predicate, Expr] =
      elem match {
        case r @ Right(expr) => r
        case Left(pred) => Left(pred.copy(cols = pred.cols ++ List(nreserved)))
      }

    def rewriteRule(rule: Rule): Rule = rule match {
      case Rule(head, body) =>
        head.time match {
          case None => Rule(head.copy(cols = head.cols ++ List(nreserved)), body.map(rewriteBodyElem))
          case Some(time) =>
            time match {
              case Next() =>
                Rule(rewriteHead(head, Next()), body.map(rewriteBodyElem) ++ List(Left(nextClock(head.cols(0)))))
              case Async() =>
                Rule(rewriteHead(head, Async()), body.map(rewriteBodyElem) ++ List(Left(asyncClock(head.cols(0)))))
              //case Tick(number) =>
            }
        }
    }

    // TODO: handle facts without times => add persistence rules:
    program.copy(rules = program.rules.map(rewriteRule), facts = program.facts.map(f => rewriteHead(f, f.time.get)))
  }

  /**
   * Add additional rules to record provenance.
   */
  def addProvenanceRules(program: Program): Program = {
    val provenanceRules = program.rules.zipWithIndex.map { case (rule, number) =>
      // Rename the rule head and modify it to record all variable bindings.
      // We also record the values of variables that appear in expressions in order to
      // avoid the need to invert those expressions to recover the variable bindings
      // inside the ProvenanceReader.
      val headExprVars = rule.head.cols.filter(_.isInstanceOf[Expr]).map(_.asInstanceOf[Expr])
        .flatMap(_.variables).map(_.name)
      val allVariables = rule.variablesWithIndexes.map(_._1).toSet ++
        rule.bodyQuals.flatMap(_.variables).map(_.name) ++ headExprVars
      val newVariables = allVariables -- rule.head.variablesWithIndexes.map(_._1).toSet
      // Produce a new head, preserving the last time column:
      val newHead = rule.head.copy(tableName = rule.head.tableName + "_prov" + number,
        cols = rule.head.cols.take(rule.head.cols.size - 1) ++
          newVariables.map(Identifier).filter(_ != dc) ++ List(rule.head.cols.last))
      rule.copy(head = newHead)
    }
    program.copy(rules = program.rules ++ provenanceRules)
  }

}
