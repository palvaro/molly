package edu.berkeley.cs.boom.molly

import java.io.File
import scala.io.Source
import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.cs.boom.molly.ast.Program
import edu.berkeley.cs.boom.molly.DedalusRewrites._
import edu.berkeley.cs.boom.molly.report.HTMLWriter


case class Config(
  eot: Int = 3,
  eff: Int = 2,
  crashes: Int = 0,
  nodes: Seq[String] = Seq(),
  inputPrograms: Seq[File] = Seq()
)

object SyncFTChecker extends Logging {
  val parser = new scopt.OptionParser[Config]("syncftChecker") {
    head("syncftchecker", "0.1")
    opt[Int]('t', "EOT") text "end of time (default 3)" action { (x, c) => c.copy(eot = x)}
    opt[Int]('f', "EFF") text "end of finite failures (default 2)" action { (x, c) => c.copy(eff = x)}
    opt[Int]('c', "crashes") text "crash failures (default 0" action { (x, c) => c.copy(crashes = x)}
    opt[String]('N', "nodes") text "a comma-separated list of nodes (required)" required() action { (x, c) => c.copy(nodes = x.split(','))}
    arg[File]("<file>...") unbounded() minOccurs 1 text "Dedalus files" action { (x, c) => c.copy(inputPrograms = c.inputPrograms :+ x)}
  }

  private def processIncludes(program: Program, includeSearchPath: File): Program = {
    val includes = program.includes.map { include =>
      val includeFile = new File(includeSearchPath, include.file)
      val newProg = DedalusParser.parseProgram(Source.fromFile(includeFile).getLines().mkString("\n"))
      processIncludes(newProg, includeSearchPath)
    }
    Program(
      program.rules ++ includes.flatMap(_.rules),
      program.facts ++ includes.flatMap(_.facts),
      program.includes,
      program.tables
    )
  }

  def main(args: Array[String]) {
    parser.parse(args, Config()) map { config =>
      val combinedInput = config.inputPrograms.flatMap(Source.fromFile(_).getLines()).mkString("\n")
      val parseResults = DedalusParser.parseProgram(combinedInput)
      val includeSearchPath = config.inputPrograms(0).getParentFile
      val parseResultsWithIncludes = processIncludes(parseResults, includeSearchPath)
      val rewrite = c4StratificationWorkaround _ andThen referenceClockRules andThen addProvenanceRules
      val program = rewrite(parseResultsWithIncludes)
      val failureSpec = new FailureSpec(config.eot, config.eff, config.crashes, config.nodes.toList)
      val verifier = new Verifier(failureSpec, program)
      logger.info(s"Gross estimate: ${failureSpec.grossEstimate} runs")
      // TODO: name the output directory after the input filename and failure spec.
      HTMLWriter.write(new File("output"), Nil, verifier.verify)
    } getOrElse {
      // Error messages
    }
  }
}
