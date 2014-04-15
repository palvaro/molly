package edu.berkeley.cs.boom.molly

import edu.berkeley.cs.boom.molly.ast.Program
import edu.berkeley.cs.boom.molly.wrappers.C4Wrapper
import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.cs.boom.molly.derivations.{GoalNode, Message, SATSolver, ProvenanceReader}
import java.util.concurrent.atomic.AtomicInteger
import edu.berkeley.cs.boom.molly.derivations.SATSolver.SATVariable
import com.codahale.metrics.MetricRegistry
import nl.grons.metrics.scala.InstrumentedBuilder
import scalaz._
import scala.collection.mutable

case class RunStatus(underlying: String) extends AnyVal
case class Run(iteration: Int, status: RunStatus, failureSpec: FailureSpec, model: UltimateModel,
               messages: List[Message], provenance: List[GoalNode])

class Verifier(failureSpec: FailureSpec, program: Program, useSymmetry: Boolean = false)
              (implicit val metricRegistry: MetricRegistry) extends Logging with InstrumentedBuilder {

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
  if (failureFreeGood.isEmpty) {
    throw new IllegalStateException("'good' was empty in the failure-free run")
  } else {
    logger.debug(s"Failure-free 'good' is\n$failureFreeGood")
  }

  // TODO: re-enable the symmetry analysis
  private val alreadyExplored = mutable.HashSet[FailureSpec]()

  def verify: EphemeralStream[Run] = {
    val provenanceReader =
      new ProvenanceReader(failureFreeProgram, failureFreeSpec, failureFreeUltimateModel)
    val messages = provenanceReader.getMessages
    val provenance = provenanceReader.getDerivationTreesForTable("good")
    val satModels = SATSolver.solve(failureSpec, provenance, messages)
    val failureFreeRun =
      Run(runId.getAndIncrement, RunStatus("success"), failureSpec, failureFreeUltimateModel, messages, provenance)
    failureFreeRun ##:: doVerify(satModels.iterator)
  }

  private def doVerify(queueToVerify: Iterator[FailureSpec]): EphemeralStream[Run] = {
    val unexplored = queueToVerify.dropWhile(alreadyExplored.contains)
    if (unexplored.isEmpty) {
      EphemeralStream.emptyEphemeralStream
    } else {
      val failureSpec = unexplored.next()
      assert (!alreadyExplored.contains(failureSpec))
      val (run, potentialCounterexamples) = runFailureSpec(failureSpec)
      alreadyExplored.add(failureSpec)
      run ##:: doVerify(queueToVerify ++ potentialCounterexamples)
    }
  }

  private def isGood(model: UltimateModel): Boolean = {
    model.tableAtTime("good", failureSpec.eot).toSet == failureFreeGood
  }

  /**
   * Given a failure spec, run it and return the verification result, plus any new potential
   * counterexamples that should be explored.
   */
  private def runFailureSpec(failureSpec: FailureSpec): (Run, Set[FailureSpec]) = {
    logger.info(s"Retesting with crashes ${failureSpec.crashes} and losses ${failureSpec.omissions}")
    val failProgram = DedalusTyper.inferTypes(failureSpec.addClockFacts(program))
    val model = new C4Wrapper("with_errors", failProgram).run
    logger.info(s"'good' is ${model.tableAtTime("good", failureSpec.eot)}")
    val provenanceReader = new ProvenanceReader(failProgram, failureSpec, model)
    val messages = provenanceReader.getMessages
    val provenance = provenanceReader.getDerivationTreesForTable("good")
    if (isGood(model)) {
      // This run may have used more channels than the original run; verify
      // that omissions on those new channels don't produce counterexamples:
      val seed: Set[SATVariable] = failureSpec.crashes ++ failureSpec.omissions
      val potentialCounterexamples =
        SATSolver.solve(failureSpec, provenance, messages, seed) -- Set(failureSpec)
      val run =
        Run(runId.getAndIncrement, RunStatus("success"), failureSpec, model, messages, provenance)
      (run, potentialCounterexamples)
    } else {
      logger.info("Found counterexample: " + failureSpec)
      val run =
        Run(runId.getAndIncrement, RunStatus("failure"), failureSpec, model, messages, provenance)
      (run, Set.empty)
    }

  }
}
