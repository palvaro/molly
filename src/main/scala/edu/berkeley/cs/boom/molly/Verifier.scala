package edu.berkeley.cs.boom.molly

import edu.berkeley.cs.boom.molly.ast.Program
import edu.berkeley.cs.boom.molly.wrappers.C4Wrapper
import com.typesafe.scalalogging.slf4j.Logging

case class RunStatus(underlying: String) extends AnyVal
case class Run(iteration: Int, status: RunStatus, failureSpec: FailureSpec, model: UltimateModel)

class Verifier(failureSpec: FailureSpec, program: Program) extends Logging {

  /**
   * The ultimate model of a failure-free execution, used to bootstrap the verification.
   */
  val failureFreeUltimateModel: UltimateModel = {
    val failureFreeSpec = new FailureSpec(failureSpec.eot, 0, 0, failureSpec.nodes)
    val programWithClock = DedalusTyper.inferTypes(program.copy(facts = program.facts ++ failureFreeSpec.generateClockFacts))
    val wrapper = new C4Wrapper("error_free", programWithClock)
    wrapper.run
  }
  logger.info(s"Failure-free ultimate model is\n$failureFreeUltimateModel")

  def verify: List[Run] = {
    // TODO: implement
    List(Run(0, RunStatus("success"), failureSpec, failureFreeUltimateModel))
  }
}
