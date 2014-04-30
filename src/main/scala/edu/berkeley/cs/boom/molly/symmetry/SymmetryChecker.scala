package edu.berkeley.cs.boom.molly.symmetry

import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.cs.boom.molly.ast.{Predicate, Program, StringLiteral}
import edu.berkeley.cs.boom.molly.{DedalusTyper, FailureSpec}

/**
 * Decides whether two failure scenarios are equivalent.
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
class SymmetryChecker(program: Program, nodes: List[String]) extends Logging {

  type EDB = Set[Predicate]
  type TableTypes = Map[String, List[String]]

  private val typesForTable: TableTypes = {
    val fs = FailureSpec(1, 0, 0, nodes)  // TODO: shouldn't have to add dummy clocks to typecheck
    val tables = DedalusTyper.inferTypes(fs.addClockFacts(program)).tables
    tables.map { t => (t.name, t.types)}.toMap
  }

  val locationLiteralsThatAppearInRules: Set[String] = {
    val predicates = program.rules.flatMap(_.bodyPredicates).filter(_.tableName != "clock")
    predicates.collect { case pred@Predicate(table, cols, _, _) =>
      val colTypes = typesForTable(table)
      pred.cols.zip(colTypes).collect {
        case (StringLiteral(l), DedalusTyper.LOCATION) => {
          logger.debug(s"Location literal '$l' appears in rule defining '$table'")
          l
        }
      }
    }.flatten.toSet
  }

  val possiblySymmetricNodes: List[String] = {
    (nodes.toSet -- locationLiteralsThatAppearInRules).toList
  }

  if (possiblySymmetricNodes.isEmpty) {
    logger.warn("No candidates for symmetry due to location literals in rules")
  } else {
    logger.debug(s"Candidates for symmetry are {${possiblySymmetricNodes.mkString(", ")}}")
  }

  // TODO: it's necessary, but not sufficient, that the symmetries are unifiers of the EDBs
  // without the clock facts.  So, we can pre-compute a set of symmetries by looking at the
  // fixed portion of the EDB, and then we only need to worry about symmetry of the clocks.

  def areEquivalentForEDB(a: FailureSpec, b: FailureSpec): Boolean = {
    if (possiblySymmetricNodes.isEmpty) return false
    require (a.nodes == nodes && b.nodes == nodes)
    if (a == b) return true
    val aEDB: EDB = (program.facts ++ a.generateClockFacts).toSet
    val bEDB: EDB = (program.facts ++ b.generateClockFacts).toSet
    val remappings = possiblySymmetricNodes.permutations.map { p => a.nodes.zip(p).toMap }
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
        case (StringLiteral(loc), DedalusTyper.LOCATION) if f.isDefinedAt(loc) => StringLiteral(f(loc))
        case (c, _) => c
      }
      fact.copy(cols = newCols)
    }
  }
}