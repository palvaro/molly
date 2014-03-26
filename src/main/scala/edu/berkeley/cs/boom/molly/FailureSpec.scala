package edu.berkeley.cs.boom.molly

import org.apache.commons.math3.util.ArithmeticUtils
import edu.berkeley.cs.boom.molly.ast.{Program, IntLiteral, StringLiteral, Predicate}
import edu.berkeley.cs.boom.molly.derivations.SATSolver.{MessageLoss, CrashFailure}

case class FailureSpec(
  eot: Int,
  eff: Int,
  maxCrashes: Int,
  nodes: List[String],
  crashes: Set[CrashFailure] = Set.empty,
  omissions: Set[MessageLoss] = Set.empty) {

  require(maxCrashes <= nodes.size, "Can't have more crashes than nodes")
  require(crashes.size <= maxCrashes, "Can't specify more than maxCrashes crashes")
  require(omissions.forall(_.time < eff), "Can't have omissions at or after the EFF")

  import ArithmeticUtils._

  def grossEstimate: Long = {
    val channels = (nodes.size * nodes.size) - nodes.size
    val lossScenarios = (0 to eff).map(i => binomialCoefficient(eff, i)).sum
    pow(lossScenarios, channels)
  }

  def generateClockFacts: Seq[Predicate] = {
    for (
      a <- nodes;
      b <- nodes;
      t <- 1 to eot
      if a == b || !crashes.exists(c => c.node == a && c.time <= t)
      if !omissions.exists(o => o.from == a && o.to == b && o.time == t)
    ) yield Predicate("clock", List(StringLiteral(a), StringLiteral(b), IntLiteral(t), IntLiteral(t+1)), notin = false, None)
  }

  def addClockFacts(program: Program): Program = {
    program.copy(facts = program.facts ++ generateClockFacts)
  }
}
