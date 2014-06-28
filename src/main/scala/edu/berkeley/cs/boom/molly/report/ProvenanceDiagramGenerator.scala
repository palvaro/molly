package edu.berkeley.cs.boom.molly.report

import edu.berkeley.cs.boom.molly.derivations.{RuleNode, GoalNode}


object ProvenanceDiagramGenerator extends GraphvizPrettyPrinter {

  def generate(goals: List[GoalNode]): String = {
    val dot = "digraph" <+> "dataflow" <+> braces(nest(
      linebreak <>
        braces("rank=\"same\";" <+> ssep(goals.map(g => text("goal" + g.id)), comma <> space)) <@@>
        // Converting to a set of strings is an ugly trick to avoid adding duplicate edges:
        goals.flatMap(dotStatements).map(d => super.pretty(d)).toSet.map(text).foldLeft(empty)(_ <@@> _)
    ) <> linebreak)
    super.pretty(dot)
  }

  private def dotStatements(goal: GoalNode): List[Doc] = {
    val id = "goal" + goal.id
    val goalNode = node(id,
      "label" -> goal.tuple.toString,
      "style" -> (if (goal.tuple.tombstone) "dashed" else "filled"),
      "fontcolor" -> (if (goal.tuple.negative && !goal.tuple.tombstone) "white" else "black"),
      "fillcolor" -> (if (goal.tuple.negative) "black" else "white"))
    val edges = goal.rules.map(rule => diEdge(id, "rule" + rule.id))
    List(goalNode) ++ edges ++ goal.rules.flatMap(dotStatements)
  }

  private def dotStatements(rule: RuleNode): List[Doc] = {
    val id = "rule" + rule.id
    val nodes = List(node(id, "label" -> rule.rule.head.tableName, "shape" -> "square"))
    val edges = rule.subgoals.map(goal => diEdge(id, "goal" + goal.id))
    nodes ++ edges ++ rule.subgoals.flatMap(dotStatements)
  }
}
