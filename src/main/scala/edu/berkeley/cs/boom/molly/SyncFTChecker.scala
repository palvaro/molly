package edu.berkeley.cs.boom.molly

import java.io.File
import scala.io.Source
import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.cs.boom.molly.DedalusRewrites._
import edu.berkeley.cs.boom.molly.DedalusParser._
import edu.berkeley.cs.boom.molly.report.HTMLWriter
import com.codahale.metrics.{Slf4jReporter, MetricRegistry}
import java.util.concurrent.TimeUnit
import scalaz.syntax.id._
import scalaz.EphemeralStream


case class Config(
  eot: Int = 3,
  eff: Int = 2,
  crashes: Int = 0,
  nodes: Seq[String] = Seq(),
  inputPrograms: Seq[File] = Seq(),
  useSymmetry: Boolean = false,
  generateProvenanceDiagrams: Boolean = false
)

object SyncFTChecker extends Logging {
  val parser = new scopt.OptionParser[Config]("syncftChecker") {
    head("syncftchecker", "0.1")
    opt[Int]('t', "EOT") text "end of time (default 3)" action { (x, c) => c.copy(eot = x)}
    opt[Int]('f', "EFF") text "end of finite failures (default 2)" action { (x, c) => c.copy(eff = x)}
    opt[Int]('c', "crashes") text "crash failures (default 0)" action { (x, c) => c.copy(crashes = x)}
    opt[String]('N', "nodes") text "a comma-separated list of nodes (required)" required() action { (x, c) => c.copy(nodes = x.split(','))}
    opt[Unit]("use-symmetry") text "use symmetry to skip equivalent failure scenarios" action { (x, c) => c.copy(useSymmetry = true) }
    opt[Unit]("prov-diagrams") text "generate provenance diagrams for each execution" action { (x, c) => c.copy(generateProvenanceDiagrams = true) }
    arg[File]("<file>...") unbounded() minOccurs 1 text "Dedalus files" action { (x, c) => c.copy(inputPrograms = c.inputPrograms :+ x)}
  }

  implicit val metrics: MetricRegistry = new MetricRegistry()
  val metricsReporter = Slf4jReporter.forRegistry(metrics)
    .outputTo(logger.underlying)
    .convertRatesTo(TimeUnit.SECONDS)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .build()

  def check(config: Config): EphemeralStream[Run] = {
    val combinedInput = config.inputPrograms.flatMap(Source.fromFile(_).getLines()).mkString("\n")
    val includeSearchPath = config.inputPrograms(0).getParentFile
    val program = combinedInput |> parseProgramAndIncludes(includeSearchPath) |> referenceClockRules |> splitAggregateRules |> addProvenanceRules
    val failureSpec = FailureSpec(config.eot, config.eff, config.crashes, config.nodes.toList)
    val verifier = new Verifier(failureSpec, program, useSymmetry = config.useSymmetry)
    logger.info(s"Gross estimate: ${failureSpec.grossEstimate} runs")
    verifier.verify
  }

  def main(args: Array[String]) {
    parser.parse(args, Config()) map { config =>
      val results = check(config)
      // TODO: name the output directory after the input filename and failure spec.
      HTMLWriter.write(new File("output"), Nil, results, config.generateProvenanceDiagrams)
      metricsReporter.report()  // This appears after the HTML writing due to laziness
    } getOrElse {
      // Error messages
    }
  }
}
