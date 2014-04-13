package edu.berkeley.cs.boom.molly.symmetry

import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.cs.boom.molly.ast.{Predicate, Program, StringLiteral}
import edu.berkeley.cs.boom.molly.{DedalusTyper, FailureSpec}


/**
 * Functions for deciding whether two failure scenarios are equivalent.
 *
 * This solves the following decision problem:
 *
 *    Given a Datalog program P and an EDB, is it the case that for all EDB' that are isomorphic
 *    to the original EDB w.r.t. some function f, the models P(EDB) and P(EDB') are isomorphic
 *    w.r.t. that same f?
 *
 * In our case, we only consider isomorphisms that change the values of location-typed attributes
 * in the EDB.
 */
object SymmetryChecker extends Logging {

  type EDB = Set[Predicate]
  type TableTypes = Map[String, List[String]]

  def locationLiteralsOnlyOccurInEDB(program: Program, nodes: List[String]): Boolean = {
    val fs = FailureSpec(1, 0, 0, nodes)  // TODO: shouldn't have to add dummy clocks to typecheck
    val typesForTable: TableTypes = {
      val tables = DedalusTyper.inferTypes(fs.addClockFacts(program)).tables
      tables.map { t => (t.name, t.types)}.toMap
    }
    val predicates = program.rules.flatMap(_.bodyPredicates)
    predicates.forall { case pred @ Predicate(table, cols, _, _)  =>
      val colTypes = typesForTable(table)
      pred.cols.zip(colTypes).forall {
        case (StringLiteral(_), DedalusTyper.LOCATION) => false
        case _ => true
      }
    }
  }

  def areEquivalentForEDB(program: Program)(a: FailureSpec, b: FailureSpec): Boolean = {
    require (a.nodes == b.nodes)
    if (a == b) return true
    val aEDB: EDB = (program.facts ++ a.generateClockFacts).toSet
    val bEDB: EDB = (program.facts ++ b.generateClockFacts).toSet
    val typesForTable: TableTypes = {
      val tables = DedalusTyper.inferTypes(program.copy(facts = aEDB.toList)).tables
      tables.map { t => (t.name, t.types)}.toMap
    }
    val remappings = a.nodes.permutations.map { p => a.nodes.zip(p).toMap }
    remappings.exists { m => mapLocations(aEDB, typesForTable, m) == bEDB }
  }

  /**
   * Apply a function to location-valued EDB columns
   */
  private def mapLocations(edb: EDB, typesForTable: TableTypes,
                           f: PartialFunction[String, String]): EDB = {
    edb.map { case fact @ Predicate(table, cols, _, _) =>
      val colTypes = typesForTable(table)
      val newCols = cols.zip(colTypes).map {
        case (StringLiteral(loc), DedalusTyper.LOCATION) => StringLiteral(f(loc))
        case (c, _) => c
      }
      fact.copy(cols = newCols)
    }
  }
}
