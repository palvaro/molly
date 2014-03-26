package edu.berkeley.cs.boom.molly

import edu.berkeley.cs.boom.molly.ast._
import edu.berkeley.cs.boom.molly.ast.StringLiteral
import edu.berkeley.cs.boom.molly.ast.Expr
import edu.berkeley.cs.boom.molly.ast.Aggregate
import edu.berkeley.cs.boom.molly.ast.Program
import scala.collection.immutable

object DedalusTyper {

  private type Type = String
  private val INT: Type = "int"
  private val STRING: Type = "string"
  private val UNKNOWN: Type = "unknown"

  /**
   * Infers the types of predicate columns.
   *
   * The possible column types are 'string' and 'int'.
   *
   * @param program the program to type
   * @return a copy of the program with its `tables` field filled in.
   */
  def inferTypes(program: Program): Program = {
    require (program.tables.isEmpty, "Program is already typed!")
    // We can type string and int literals; other types need to be inferred
    // based on variable bindings.
    val allPredicates = program.facts ++ program.rules.map(_.head) ++
      program.rules.flatMap(_.bodyPredicates)
    val typeGuesses: Map[String, List[List[Type]]] =
      allPredicates.map { pred =>
        val colGuesses = pred.cols.map {
          case Identifier("MRESERVED") => INT
          case Identifier("NRESERVED") => INT
          case StringLiteral(_) => STRING
          case IntLiteral(_) => INT
          case a: Aggregate => INT
          case e: Expr => INT
          case _ => UNKNOWN
        }
        (pred.tableName, colGuesses)
      }.groupBy(_._1).mapValues(_.map(_._2))

    // As a sanity check, ensure that all occurrences of the predicate have the
    // same number of columns.
    for ((table, guesses) <- typeGuesses) {
      assert(guesses.map(_.length).toSet.size == 1,
        s"Predicate $table used with inconsistent number of columns")
    }

    def dom(a: Type, b: Type): Type = {
      if (a != UNKNOWN) a
      else b
    }
    val knownTypes: Map[String, Array[String]] = {
      val m = typeGuesses.mapValues {
        _.reduce((x, y) => x.zip(y).map(p => dom(p._1, p._2))).toArray
      }.toSeq
      immutable.Map(m: _*)    // Workaround for a Scala bug :(
    }

    // Any variable that appears in an expression in the rule body must be an int:
    for (rule <- program.rules) {
      val expressionVariables = rule.bodyQuals.flatMap(_.variables)
      rule.head.cols.zipWithIndex.foreach { case (atom, colNum) =>
        atom match {
          case ident: Identifier =>
            if (expressionVariables.contains(ident))
              knownTypes(rule.head.tableName)(colNum) = INT
          case _ =>
         }
      }
    }

    var updatedBindings = true
    // Iteratively fill in the missing types by find columns that should have the same type:
    while (updatedBindings) {
      updatedBindings = false
      for (rule <- program.rules) {
        val equivalenceClasses = rule.variablesWithIndexes.groupBy(_._1).values
        equivalenceClasses.foreach { vars =>
          val typeBindings = vars.map {
            case (_, (tableName, colNum)) => knownTypes(tableName)(colNum)
          }
          val candidateTypes = typeBindings.filter(_ != UNKNOWN).toSet
          assert(candidateTypes.size <= 1, "Could not uniquely determine type for vars " + vars)
          if (candidateTypes.size == 1 && typeBindings.contains(UNKNOWN)) {
            // Propagate the binding
            val finalType = candidateTypes.toSeq(0)
            vars.foreach {
              case (_, (tableName, colNum)) =>
                knownTypes(tableName)(colNum) = finalType
            }
            updatedBindings = true
          }
        }
      }
    }

    assert (!knownTypes.values.flatten.toSeq.contains(UNKNOWN), "Failed to determine a col type")

    val tables = knownTypes.toSeq.map(x => Table(x._1, x._2.toList)).toSet
    program.copy(tables = tables)
  }
}
