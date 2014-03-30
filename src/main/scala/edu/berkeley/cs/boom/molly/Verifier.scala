package edu.berkeley.cs.boom.molly

import edu.berkeley.cs.boom.molly.ast.Program
import edu.berkeley.cs.boom.molly.wrappers.C4Wrapper
import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.cs.boom.molly.derivations.{Message, SATSolver, ProvenanceReader}
import java.util.concurrent.atomic.AtomicInteger
import edu.berkeley.cs.boom.molly.derivations.SATSolver.SATVariable
import scala.collection.mutable

case class RunStatus(underlying: String) extends AnyVal
case class Run(iteration: Int, status: RunStatus, failureSpec: FailureSpec, model: UltimateModel,
               messages: List[Message])

class Verifier(failureSpec: FailureSpec, program: Program) extends Logging {

  private val failureFreeSpec = failureSpec.copy(eff = 0, maxCrashes = 0)
  private val failureFreeProgram = DedalusTyper.inferTypes(failureFreeSpec.addClockFacts(program))
  private val runId = new AtomicInteger(0)
  private val runCache = new mutable.HashMap[FailureSpec, Traversable[Run]]()

  /**
   * The ultimate model of a failure-free execution, used to bootstrap the verification.
   */
  private val failureFreeUltimateModel: UltimateModel = {
    new C4Wrapper("error_free", failureFreeProgram).run
  }

  private val failureFreeGood = failureFreeUltimateModel.tableAtTime("good", failureSpec.eot).toSet
  logger.debug(s"Failure-free 'good' is\n$failureFreeGood")


  def verify: Traversable[Run] = {
    val provenanceReader =
      new ProvenanceReader(failureFreeProgram, failureFreeSpec, failureFreeUltimateModel)
    val messages = provenanceReader.getMessages
    val provenance = provenanceReader.getDerivationTreesForTable("good")
    val satModels = SATSolver.solve(failureSpec, provenance, messages)
    if (satModels.isEmpty) {
      List(Run(runId.getAndIncrement, RunStatus("success"), failureSpec, failureFreeUltimateModel, messages))
    } else {
      satModels.flatMap(getOrComputeRuns)
    }
  }

  private def isGood(model: UltimateModel): Boolean = {
    model.tableAtTime("good", failureSpec.eot).toSet == failureFreeGood
  }

  private def getOrComputeRuns(failureSpec: FailureSpec): Traversable[Run] = {
    runCache.getOrElseUpdate(failureSpec, doVerify(failureSpec))
  }

  private def doVerify(failureSpec: FailureSpec): Traversable[Run] = {
    logger.info(s"Retesting with crashes ${failureSpec.crashes} and losses ${failureSpec.omissions}")
    val failProgram = DedalusTyper.inferTypes(failureSpec.addClockFacts(program))
    val model = new C4Wrapper("with_errors", failProgram).run
    logger.info(s"'good' is ${model.tableAtTime("good", failureSpec.eot)}")
    val provenanceReader = new ProvenanceReader(failProgram, failureSpec, model)
    val messages = provenanceReader.getMessages
    if (isGood(model)) {
      // This run may have used more channels than the original run; verify
      // that omissions on those new channels don't produce counterexamples:
      val provenance = provenanceReader.getDerivationTreesForTable("good")
      val seed: Set[SATVariable] = failureSpec.crashes ++ failureSpec.omissions
      val satModels = SATSolver.solve(failureSpec, provenance, messages, seed) -- Set(failureSpec)
      val counterexamples = satModels.flatMap(getOrComputeRuns).filter(r => r.status == RunStatus("failure"))
      if (counterexamples.isEmpty) {
        List(Run(runId.getAndIncrement, RunStatus("success"), failureSpec, model, messages))
      } else {
        counterexamples
      }
    } else {
      logger.info("Found counterexample: " + failureSpec)
      List(Run(runId.getAndIncrement, RunStatus("failure"), failureSpec, model, messages))
    }

  }
}
