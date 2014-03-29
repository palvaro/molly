package edu.berkeley.cs.boom.molly.report

import edu.berkeley.cs.boom.molly.FailureSpec
import edu.berkeley.cs.boom.molly.derivations.Message


object SpacetimeDiagramGenerator extends GraphvizPrettyPrinter {
  def generate(failureSpec: FailureSpec, messages: Seq[Message]): String = {
    val processNodes = failureSpec.nodes.map { n =>
      node(s"proc_$n", "label" -> s"Process $n")
    }

    val nodeTimes = for (n <- failureSpec.nodes; t <- 0 to failureSpec.eot) yield {
      val crashed = !failureSpec.crashes.filter(c => c.node == n && c.time <= t).isEmpty
      val styles =
        if (crashed) Seq("color" -> "red", "fontsize" -> "10", "label" -> "CRASHED")
        else  Seq("label" -> s"$t")
      node(s"node_${n}_$t", styles: _*)
    }

    val nodeTimeEdges = failureSpec.nodes.flatMap { n =>
      val es = (0 to failureSpec.eot - 1).map { t => diEdge(s"node_${n}_$t", s"node_${n}_${t + 1}") }
      es :+ diEdge(s"proc_$n", s"node_${n}_0", "weight" -> "2")
    }

    val messageEdges = messages.map { case Message(table, from, to, sendTime, receiveTime) =>
      val wasLost = receiveTime == FailureSpec.NEVER
      diEdge(s"node_${from}_$sendTime", s"node_${to}_${sendTime + 1}",
        "label" -> (table + (if (wasLost) " (LOST)" else "")),
        "constraint" -> "false",
        "weight" -> "0",
        "style" -> (if (wasLost) "dotted" else "dashed"),
        "color" -> (if (wasLost) "red" else "black")
      )
    }

    val dot = "digraph" <+> "spacetime" <+> braces(nest(
      linebreak <>
      "rankdir=LR" <@@>
      subgraph("cluster_proc_nodes", "", processNodes) <@@>
      (nodeTimes ++ nodeTimeEdges ++ messageEdges).reduce(_ <@@> _)
    ) <> linebreak)
    super.pretty(dot)
  }
}
