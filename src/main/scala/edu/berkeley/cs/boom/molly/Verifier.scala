package edu.berkeley.cs.boom.molly

import edu.berkeley.cs.boom.molly.ast.Program
import edu.berkeley.cs.boom.molly.wrappers.C4Wrapper
import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.cs.boom.molly.derivations.{SATSolver, RuleGoalGraphGraphvizGenerator, ProvenanceReader}
import java.util.concurrent.atomic.AtomicInteger

case class RunStatus(underlying: String) extends AnyVal
case class Run(iteration: Int, status: RunStatus, failureSpec: FailureSpec, model: UltimateModel)

class Verifier(failureSpec: FailureSpec, program: Program) extends Logging {

  private val failureFreeSpec = failureSpec.copy(eff = 0, maxCrashes = 0)
  private val failureFreeProgram = DedalusTyper.inferTypes(failureFreeSpec.addClockFacts(program))
  private val runId = new AtomicInteger(0)

  /**
   * The ultimate model of a failure-free execution, used to bootstrap the verification.
   */
  private val failureFreeUltimateModel: UltimateModel = {
    new C4Wrapper("error_free", failureFreeProgram).run
  }

  private val failureFreeGood = failureFreeUltimateModel.tableAtTime("good", failureSpec.eot).toSet
  logger.debug(s"Failure-free 'good' is\n$failureFreeGood")


  def verify: Traversable[Run] = {
    val provenance =
      ProvenanceReader.read(failureFreeProgram, failureFreeSpec, failureFreeUltimateModel, "good")
    val satModels = SATSolver.solve(failureSpec, provenance)
    if (satModels.isEmpty) {
      List(Run(runId.getAndIncrement, RunStatus("success"), failureSpec, failureFreeUltimateModel))
    } else {
      satModels.map { case (crashes, omissions) =>
        failureSpec.copy (crashes = crashes, omissions = omissions)
      }.flatMap(doVerify)
    }
  }

  private def isVacuouslyGood(model: UltimateModel): Boolean = {
    model.tableAtTime("good", failureSpec.eot).isEmpty
  }

  private def isGood(model: UltimateModel): Boolean = {
    model.tableAtTime("good", failureSpec.eot).toSet == failureFreeGood
  }

  private def doVerify(failureSpec: FailureSpec): Traversable[Run] = {
    logger.info(s"Retesting with crashes ${failureSpec.crashes} and losses ${failureSpec.omissions}")
    val failProgram = DedalusTyper.inferTypes(failureSpec.addClockFacts(program))
    val model = new C4Wrapper("with_errors", failProgram).run
    logger.info(s"'good' is ${model.tableAtTime("good", failureSpec.eot)}")
    if (isVacuouslyGood(model)) {
      List(Run(runId.getAndIncrement, RunStatus("vacuous"), failureSpec, model))
    } else if (isGood(model)) {
      List(Run(runId.getAndIncrement, RunStatus("success"), failureSpec, model))
    } else {
      logger.info("Found counterexample: " + failureSpec)
      List(Run(runId.getAndIncrement, RunStatus("failure"), failureSpec, model))
    }

  }
}
