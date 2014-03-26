package edu.berkeley.cs.boom.molly

import org.kiama.util.PositionedParserUtilities
import edu.berkeley.cs.boom.molly.ast._


trait DedalusParser extends PositionedParserUtilities {

  lazy val ident = "[a-zA-Z0-9._?@]+".r
  lazy val semi = ";"
  lazy val number = "[0-9]+".r ^^ { s => s.toInt}
  lazy val string = "\"[^\"]*\"".r ^^ { s => s.stripPrefix("\"").stripSuffix("\"")}
  lazy val followsfrom = ":-"
  lazy val timesuffix: Parser[Time] =
    "@next" ^^ { _  => Next() } |
    "@async" ^^ { _ => Async() } |
    '@' ~> number ^^ Tick
  lazy val op = "==" | "!=" | "+" | "-" | "/" | "*" | "<" | ">" | "<=" | ">="

  lazy val constant: Parser[Constant] = string ^^ StringLiteral | number ^^ IntLiteral | ident ^^ Identifier
  lazy val expr: Parser[Expr] = constant ~ op ~ exprOrConstant ^^ { case c ~ o ~ e => Expr(c, o, e)}
  lazy val exprOrConstant: Parser[Expression] = expr | constant
  lazy val aggregate = ident ~ "<" ~ ident ~ ">" ^^ {
    case aggName ~ "<" ~ aggCol ~ ">" => Aggregate(aggName, aggCol)
  }

  lazy val program: Parser[Program] = rep(clause) ^^ { clauses =>
    Program(clauses.filter(_.isInstanceOf[Rule]).map(_.asInstanceOf[Rule]),
      clauses.filter(_.isInstanceOf[Predicate]).map(_.asInstanceOf[Predicate]),
      clauses.filter(_.isInstanceOf[Include]).map(_.asInstanceOf[Include]))
  }
  lazy val clause: Parser[Clause] = include | rule | fact
  lazy val include = "include" ~> string <~ semi ^^ Include
  lazy val fact = head <~ semi
  lazy val rule = head ~ followsfrom ~ body <~ semi ^^ {
    case head ~ followsfrom ~ body => Rule(head, body)
  }

  lazy val head = predicate
  lazy val body = repsep(bodyTerm, ",")
  lazy val bodyTerm: Parser[Either[Predicate, Expr]] =
    predicate ^^ { Left(_) } | expr ^^ { Right(_) }

  lazy val predicate = opt("notin") ~ ident ~ "(" ~ repsep(atom, ",") ~ ")" ~ opt(timesuffix) ^^ {
    case notin ~ tableName ~ "(" ~ cols ~ ")" ~ time =>
      Predicate(tableName, cols, notin.isDefined, time)
  }
  lazy val atom = aggregate | exprOrConstant | constant

  // See https://stackoverflow.com/questions/5952720
  override val whiteSpace = """(\s|//.*|(?m)/\*(\*(?!/)|[^*])*\*/)+""".r
}

object DedalusParser extends DedalusParser {
  def parseProgram(str: CharSequence): Program = {
    parseAll(program, str).get
  }
}