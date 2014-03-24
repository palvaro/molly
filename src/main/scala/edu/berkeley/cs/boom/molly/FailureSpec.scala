package edu.berkeley.cs.boom.molly

import org.apache.commons.math3.util.ArithmeticUtils
import edu.berkeley.cs.boom.molly.ast.{IntLiteral, StringLiteral, Predicate}

case class FailureSpec(
  eot: Int,
  eff: Int,
  crashes: Int,
  nodes: List[String]) {

  def grossEstimate: Long = {
    import ArithmeticUtils._
    val channels = (nodes.size * nodes.size) - nodes.size
    val lossScenarios = (0 to eff).map(i => binomialCoefficient(eff, i)).sum
    pow(channels, lossScenarios)
  }

  def generateClockFacts: Seq[Predicate] = {
    for (
      a <- nodes;
      b <- nodes;
      t <- 1 to eot
    ) yield Predicate("clock", List(StringLiteral(a), StringLiteral(b), IntLiteral(t), IntLiteral(t+1)), notin = false, None)
  }
}
