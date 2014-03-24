package edu.berkeley.cs.boom.molly.codegen

import org.kiama.output.PrettyPrinter
import edu.berkeley.cs.boom.molly.ast._
import edu.berkeley.cs.boom.molly.ast.StringLiteral
import edu.berkeley.cs.boom.molly.ast.IntLiteral
import edu.berkeley.cs.boom.molly.ast.Program

object C4CodeGenerator extends PrettyPrinter {
  def generate(program: Program): String = {
    val tables = program.tables.map { table =>
      "define" <> parens(table.name <> comma <+> braces(ssep(table.types.map(text), ", "))) <> semi
    }
    val facts = program.facts.map(genFact)
    val rules = program.rules.map(genRule)
    val wholeProgram = (tables.toSeq ++ facts.toSeq ++ rules.toSeq).reduce(_ <@@> _)
    super.pretty(wholeProgram)
  }

  private def genAtom(atom: Atom): Doc = atom match {
    case StringLiteral(s) => dquotes(text(s))
    case IntLiteral(i) => text(i.toString)
    case Identifier(i) => text(i)
    case Expr(c, o, e) => genAtom(c) <+> o <+> genAtom(e)
    // TODO: case Aggregate
  }

  private def genRule(rule: Rule): Doc = {
    genPredicate(rule.head) <+> ":-" <+> ssep(rule.body.map {
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
