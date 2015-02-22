package edu.berkeley.cs.boom.molly.codegen

import org.kiama.output.PrettyPrinter
import edu.berkeley.cs.boom.molly.ast._
import edu.berkeley.cs.boom.molly.ast.StringLiteral
import edu.berkeley.cs.boom.molly.ast.IntLiteral
import edu.berkeley.cs.boom.molly.ast.Program
import edu.berkeley.cs.boom.molly.{DedalusType, DedalusTyper}

object C4CodeGenerator extends PrettyPrinter {
  def generate(program: Program): String = {
    val tables = program.tables.map { table =>
      "define" <> parens(table.name <> comma <+> braces(ssep(table.types.map(typeToC4Type _ andThen text), ", "))) <> semi
    }
    val facts = program.facts.map(genFact)
    val rules = program.rules.map(genRule)
    val wholeProgram = (tables.toSeq ++ facts.toSeq ++ rules.toSeq).reduce(_ <@@> _)
    super.pretty(wholeProgram)
  }

  private def typeToC4Type(t: DedalusType): String = {
    t match {
      case DedalusType.LOCATION => "string"
      case DedalusType.INT => "int"
      case DedalusType.STRING => "string"
      case DedalusType.UNKNOWN =>
        throw new IllegalArgumentException("Cannot convert unknown Dedalus type to C4 type")
    }
  }

  private def genAtom(atom: Atom): Doc = atom match {
    case StringLiteral(s) => dquotes(text(s))
    case IntLiteral(i) => text(i.toString)
    case Identifier(i) => text(i)
    case Expr(c, o, e) => genAtom(c) <+> o <+> genAtom(e)
    case Aggregate(aggName, aggCol) => aggName <> angles(aggCol)
  }

  private def genRule(rule: Rule): Doc = {
    // Place negated predicates at the end of the rule body.
    // This is a workaround for a C4 bug: https://github.com/bloom-lang/c4/issues/1
    val sortedBody = rule.body.sortBy(_.left.map(p => if (p.notin) 1 else 0).left.getOrElse(0))
    genPredicate(rule.head) <+> ":-" <+> ssep(sortedBody.map {
      case Left(p: Predicate) => genPredicate(p)
      case Right(e: Expr) => genAtom(e)
    }, ", ") <> semi
  }

  private def genFact(fact: Predicate): Doc = {
    genPredicate(fact) <> semi
  }
  
  private def genPredicate(predicate: Predicate): Doc = {
    val notin = if (predicate.notin) "notin" <> space else empty
    notin <> predicate.tableName <> parens(ssep(predicate.cols.map(genAtom), ", "))
  }
}
