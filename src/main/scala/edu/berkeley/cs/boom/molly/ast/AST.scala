package edu.berkeley.cs.boom.molly.ast

import org.kiama.attribution.Attributable
import org.kiama.util.Positioned


trait Node extends Attributable with Positioned

sealed trait Atom extends Node
sealed trait Expression extends Atom
sealed trait Constant extends Expression

case class Expr(left: Constant, op: String, right: Expression) extends Expression {
  def variables: Set[Identifier] = {
    val rightVariables = right match {
      case i: Identifier => Set(i)
      case e: Expr => e.variables
      case _ => Set.empty
    }
    left match {
      case i: Identifier => Set(i) ++ rightVariables
      case _ => Set.empty
    }
  }
}

case class StringLiteral(str: String) extends Constant
case class IntLiteral(int: Int) extends Constant
case class Identifier(name: String) extends Constant

case class Aggregate(aggName: String, aggColumn: String) extends Atom

case class Program(
  rules: List[Rule],
  facts: List[Predicate],
  includes: List[Include],
  tables: Set[Table] = Set()) extends Node

case class Table(name: String, types: List[String])

sealed trait Clause extends Node
case class Include(file: String) extends Clause
case class Rule(head: Predicate, body: List[Either[Predicate, Expr]]) extends Clause {
  def bodyPredicates: List[Predicate] = body.filter(_.isLeft).map(_.left.get)
  def bodyQuals: List[Expr] = body.filter(_.isRight).map(_.right.get)
  def variablesWithIndexes: List[(String, (String, Int))] = {
    (List(head) ++ bodyPredicates).flatMap(_.variablesWithIndexes)
  }
}
case class Predicate(tableName: String,
                     cols: List[Atom],
                     notin: Boolean,
                     time: Option[Time]) extends Clause {
  def variablesWithIndexes: List[(String, (String, Int))] = {
    cols.zipWithIndex.flatMap { case (col, index) =>
      col match {
        case Identifier("_") => None
        case Identifier(i) => Some((i, (tableName, index)))
        case _ => None
      }
    }
  }
}

sealed trait Time extends Node
case class Next() extends Time
case class Async() extends Time
case class Tick(number: Int) extends Time