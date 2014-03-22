package edu.berkeley.cs.boom.molly

import edu.berkeley.cs.boom.molly.ast.Program
import edu.berkeley.cs.boom.molly.wrappers.C4Wrapper
import com.typesafe.scalalogging.slf4j.Logging

class Verifier(failureSpec: FailureSpec, program: Program) extends Logging {

  bootstrap()

  /**
   * Bootstrap the verification by computing the ultimate model
   * of a failure-free execution.
   */
  private def bootstrap() {
    val failureFreeSpec = new FailureSpec(failureSpec.eot, 0, 0, failureSpec.nodes)
    val programWithClock = DedalusTyper.inferTypes(program.copy(facts = program.facts ++ failureFreeSpec.generateClockFacts))
    val wrapper = new C4Wrapper("error_free", programWithClock)
    val ultimateModel = wrapper.run
    logger.info(s"Failure-free ultimate model is\n$ultimateModel")
  }
}
