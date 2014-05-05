package edu.berkeley.cs.boom.molly.report

import edu.berkeley.cs.boom.molly.FailureSpec
import edu.berkeley.cs.boom.molly.derivations.Message
import scalaz._
import Scalaz._

object SpacetimeDiagramGenerator extends GraphvizPrettyPrinter {

  private val TIMELINE_COLOR = "gray75"

  def generate(failureSpec: FailureSpec, messages: Seq[Message]): String = {
    val processNodes = failureSpec.nodes.map { n =>
      node(s"proc_$n", "label" -> s"Process $n", "group" -> n)
    }

    /**
     * The last time that this node appears on the timeline.  Usually this is EOT,
     * but it could be smaller if the node crashes at this time.
     */
    val lastTimeOfNode =
      failureSpec.crashes.map(x => x.node -> x.time).toMap.withDefaultValue(failureSpec.eot)

    val nodeTimes = for (
      n <- failureSpec.nodes;
      t <- 1 to lastTimeOfNode(n)) yield {
      val crashed = !failureSpec.crashes.filter(c => c.node == n && c.time <= t).isEmpty
      val sentOrReceivedMessage = messages.exists { m =>
        (m.from == n && m.to != n && m.sendTime == t) || (m.to == n && m.from != n && m.receiveTime == t)
      }
      val styles =
        if (crashed) Seq("color" -> "red", "fontsize" -> "10", "label" -> "CRASHED", "group" -> n, "shape" -> "box")
        else if (sentOrReceivedMessage) Seq("label" -> s"$t", "group" -> n)
        else Seq("shape" -> "point", "group" -> n, "color" -> TIMELINE_COLOR)
      node(s"node_${n}_$t", styles: _*)
    }

    val timelineLines = failureSpec.nodes.map { n =>
      val nodes = (1 to lastTimeOfNode(n)).map { t => s"node_${n}_$t" }.toSeq
      s"edge[weight=2, arrowhead=none, color=gray75, fillcolor=$TIMELINE_COLOR];" <@@> s"proc_$n -> " <> nodes.mkString(" -> ") <> semi
    }

    val messageEdges = messages.flatMap { case Message(table, from, to, sendTime, receiveTime) =>
      val wasLost = receiveTime == FailureSpec.NEVER
      val receiverEOT = lastTimeOfNode(to)
      // Note that this doesn't draw mesasges that were sent to nodes that have been
      // crashed for more than one timestep.
      val receiverCrashed = receiveTime > receiverEOT && receiverEOT < failureSpec.eot
      if (receiverCrashed) None
      else diEdge(s"node_${from}_$sendTime", s"node_${to}_${sendTime + 1}",
        "label" -> (table + (if (wasLost) " (LOST)" else "")),
        "constraint" -> "false",
        "weight" -> "0",
        "style" -> (if (wasLost) "dashed" else "solid"),
        "color" -> (if (wasLost) "red" else "black")
      ).some
    }

    val dot = "digraph" <+> "spacetime" <+> braces(nest(
      linebreak <>
      "rankdir=TD" <@@>
      "splines=line" <@@>  // Keep the message lines straight
      "outputorder=nodesfirst" <@@>
      subgraph("cluster_proc_nodes", "", processNodes) <@@>
      nodeTimes.reduce(_ <@@> _) <@@>
      messageEdges.foldLeft(empty)(_ <@@> _) <@@>
      timelineLines.reduce(_ <@@> _)
    ) <> linebreak)
    super.pretty(dot)
  }
}
