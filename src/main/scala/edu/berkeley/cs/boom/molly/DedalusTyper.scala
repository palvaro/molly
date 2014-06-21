package edu.berkeley.cs.boom.molly

import edu.berkeley.cs.boom.molly.ast._
import edu.berkeley.cs.boom.molly.ast.StringLiteral
import edu.berkeley.cs.boom.molly.ast.Expr
import edu.berkeley.cs.boom.molly.ast.Aggregate
import edu.berkeley.cs.boom.molly.ast.Program
import org.jgrapht.alg.util.UnionFind
import scala.collection.JavaConverters._
import org.kiama.util.Positions

object DedalusTyper {

  type Type = String
  private type ColRef = (String, Int)  // (tableName, columnNumber) pairs
  val INT: Type = "int"
  val STRING: Type = "string"
  val LOCATION: Type = "location"
  val UNKNOWN: Type = "unknown"

  private def inferTypeOfAtom(atom: Atom): Type = {
    atom match {
      case StringLiteral(_) => STRING
      case IntLiteral(_) => INT
      case a: Aggregate => INT
      case e: Expr => INT
      case _ => UNKNOWN
    }
  }

  private def dom(types: Set[(Atom, Type)]): Set[(Atom, Type)] = {
    if (types.map(_._2) == Set(STRING, LOCATION)) {
      types.filter(_._2 == LOCATION)
    } else {
      types
    }
  }

  /**
   * Infers the types of predicate columns.
   *
   * The possible column types are 'string', 'int', and 'location'.
   *
   * @param program the program to type
   * @return a copy of the program with its `tables` field filled in.
   */
  def inferTypes(program: Program): Program = {
    require (program.tables.isEmpty, "Program is already typed!")
    val allPredicates = program.facts ++ program.rules.map(_.head) ++
      program.rules.flatMap(_.bodyPredicates)
    val mostColRefs = for (
      pred <- allPredicates;
      (col, colNum) <- pred.cols.zipWithIndex
    ) yield (pred.tableName, colNum) 

    val allColRefs = mostColRefs ++ Seq(
      ("crash", 0),
      ("crash", 1),
      ("crash", 2),
      ("crash", 3),
      ("clock", 0),
      ("clock", 1),
      ("clock", 2),
      ("clock", 3)
    )

    // Determine (maximal) sets of columns that must have the same type:
    val colRefToMinColRef = new UnionFind[ColRef](allColRefs.toSet.asJava)

    // Columns that are related through variable unification must have the same type:
    for (
      rule <- program.rules;
      sameTypedCols <- rule.variablesWithIndexes.groupBy(_._1).values.map(x => x.map(_._2));
      firstColRef = sameTypedCols.head;
      colRef <- sameTypedCols
    ) {
      colRefToMinColRef.union(colRef, firstColRef)
    }

    // Columns that are related through variables appearing in quals must have the same type:
    for (
      rule <- program.rules;
      varToColRef = rule.variablesWithIndexes.groupBy(_._1).mapValues(_.head._2);
      qual <- rule.bodyQuals;
      colRefs = qual.variables.toList.map(i => varToColRef(i.name));
      firstColRef = colRefs.head;
      colRef <- colRefs
    ) {
      colRefToMinColRef.union(colRef, firstColRef)
    }

    // Accumulate all type evidence.  To provide useful error messages when we find conflicting
    // evidence, we store the provenance of this evidence (the Atom
    val typeEvidence: Map[ColRef, Set[(Atom, Type)]] = {
      // Some meta-EDB tables might be empty in certain runs (such as crash()), so we need to
      // hard-code their type evidence:
      val metaEDBTypes = Seq(
        (colRefToMinColRef.find(("crash", 0)), (null, LOCATION)),
        (colRefToMinColRef.find(("crash", 1)), (null, LOCATION)),
        (colRefToMinColRef.find(("crash", 2)), (null, INT)),
        (colRefToMinColRef.find(("crash", 3)), (null, INT)),
        (colRefToMinColRef.find(("clock", 0)), (null, LOCATION)),
        (colRefToMinColRef.find(("clock", 1)), (null, LOCATION)),
        (colRefToMinColRef.find(("clock", 2)), (null, INT)),
        (colRefToMinColRef.find(("clock", 3)), (null, INT))
      )
      val inferredFromPredicates = for (
        pred <- allPredicates;
        (col, colNum) <- pred.cols.zipWithIndex;
        inferredType = inferTypeOfAtom(col)
        if inferredType != UNKNOWN
      ) yield (colRefToMinColRef.find((pred.tableName, colNum)), (col, inferredType))
      val evidence = inferredFromPredicates ++ metaEDBTypes
      evidence.groupBy(_._1).mapValues(_.map(_._2).toSet)
    }

    // Check that all occurrences of a given predicate have the same number of columns:
    val numColsInTable = allPredicates.groupBy(_.tableName).mapValues { predicates =>
      val colCounts = predicates.map(_.cols.size).toSet
      assert(colCounts.size == 1,
        s"Predicate ${predicates.head.tableName} used with inconsistent number of columns")
      colCounts.head
    } + ("clock" -> 4) + ("crash" -> 4)

    // Assign types to each group of columns:
    val tableNames = allPredicates.map(_.tableName).toSet  ++ Set("crash", "clock")
    val tables = tableNames.map { tableName =>
      val numCols = numColsInTable(tableName)
      val colTypes = (0 to numCols - 1).map { colNum =>
        val representative = colRefToMinColRef.find((tableName, colNum))
        val types = dom(typeEvidence.getOrElse(representative,
          throw new Exception(
            s"No evidence for type of column ${representative._2} of ${representative._1}")))
        assert(types.map(_._2).size == 1, {
          val evidenceByType = types.groupBy(_._2)
          val headPosition =
            Positions.getStart(allPredicates.filter(_.tableName == tableName).head.cols(colNum))
          s"Conflicting evidence for type of column $colNum of $tableName:\n\n" +
          headPosition.longString + "\n---------------------------------------------\n" +
          evidenceByType.map { case (inferredType, evidence) =>
            val evidenceLocations = for ((atom, _) <- evidence) yield {
              if (atom != null) Positions.getStart(atom).longString
              else "unification with meta EDB column"
            }
            s"Evidence for type $inferredType:\n\n" +  evidenceLocations.mkString("\n")
          }.mkString("\n---------------------------------------------\n")
        })
        types.head._2
      }
      Table(tableName, colTypes.toList)
    }
    program.copy(tables = tables)
  }
}
