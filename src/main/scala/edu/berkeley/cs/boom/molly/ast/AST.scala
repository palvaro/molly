package edu.berkeley.cs.boom.molly.ast

import org.kiama.util.TreeNode

import edu.berkeley.cs.boom.molly.DedalusType

/**
 * An atom is an element that can appear in a predicate, such as a variable or aggregate.
 */
sealed trait Atom extends TreeNode
sealed trait Expression extends Atom
sealed trait Constant extends Expression

case class Expr(left: Constant, op: String, right: Expression) extends Expression {
  /**
   * Returns the complete set of variable identifiers that appear anywhere in this expression.
   */
  def variables: Set[Identifier] = {
    val rightVariables: Set[Identifier] = right match {
      case i: Identifier => Set(i)
      case e: Expr => e.variables
      case _ => Set.empty
    }
    val leftVariables: Set[Identifier] = left match {
      case i: Identifier => Set(i)
      case _ => Set.empty
    }
    leftVariables ++ rightVariables
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
  tables: Set[Table] = Set()
) extends TreeNode

case class Table(name: String, types: List[DedalusType]) {
  types.headOption.foreach { t =>
    assert(t == DedalusType.LOCATION,
      s"First column of a table must have type LOCATION, but found $t")
  }
}

sealed trait Clause extends TreeNode
case class Include(file: String) extends Clause
case class Rule(head: Predicate, body: List[Either[Predicate, Expr]]) extends Clause {
  def bodyPredicates: List[Predicate] = body.collect { case Left(pred) => pred }
  def bodyQuals: List[Expr] = body.collect { case Right(expr) => expr }
  def variablesWithIndexes: List[(String, (String, Int))] = {
    (List(head) ++ bodyPredicates).flatMap(_.topLevelVariablesWithIndices)
  }
  def variables: Set[String] = {
    variablesWithIndexes.map(_._1).toSet
  }
  /** Variables that are bound in the body (i.e. appear more than once) */
  def boundVariables: Set[String] = {
    val allVars =
      bodyPredicates.flatMap(_.topLevelVariables.toSeq) ++ bodyQuals.flatMap(_.variables).map(_.name).toSeq
    allVars.groupBy(identity).mapValues(_.size).filter(_._2 >= 2).keys.toSet
  }
  // Match the Ruby solver's convention that a predicate's location column always appears
  // as the first column of its first body predicate
  val locationSpecifier = bodyPredicates(0).cols(0)

  def isAsync: Boolean = head.time == Some(Async())
}

/**
 * Represents a Dedalus predicate.
 *
 * @param tableName the name of this predicate (e.g. the table that it helps to define)
 * @param cols      the columns of the predicate
 * @param notin     true if this predicate is negated, false otherwise
 * @param time      an optional temporal annotation
 */
case class Predicate(
  tableName: String,
  cols: List[Atom],
  notin: Boolean,
  time: Option[Time]
) extends Clause {

  /**
   * The set of variable names that appear at the top-level of this predicate (e.g. not in
   * aggregates or expressions).
   */
  def topLevelVariables: Set[String] = {
    topLevelVariablesWithIndices.map(_._1).toSet
  }

  /**
   * For each variable that appears at the top-level of this predicate (e.g. not in
   * aggregates or expressions), returns a `(variableName, (tableName, colNumber))` tuples,
   * where `tableName` is the name of this predicate.
   */
  def topLevelVariablesWithIndices: List[(String, (String, Int))] = {
    cols.zipWithIndex.collect {
      case (Identifier(i), index) if i != "_" => (i, (tableName, index))
    }
  }

  /**
   * The set of variable names that appear in aggregates in this predicate.
   */
  def aggregateVariables: Set[String] = {
    cols.collect { case Aggregate(_, aggCol) => aggCol}.toSet
  }

  /**
   * The set of variable names that appear in expressions in this predicate.
   */
  def expressionVariables: Set[String] = {
    cols.collect { case e: Expr => e }.flatMap(_.variables).map(_.name).toSet
  }
}

sealed trait Time extends TreeNode
case class Next() extends Time
case class Async() extends Time
case class Tick(number: Int) extends Time