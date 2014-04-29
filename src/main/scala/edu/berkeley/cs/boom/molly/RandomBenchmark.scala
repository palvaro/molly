package edu.berkeley.cs.boom.molly

import com.codahale.metrics.MetricRegistry
import java.io.File
import com.github.tototoshi.csv.CSVWriter
import scala.util.control.Breaks._

/**
 * Tool for sampling "number of runs to first counterexample" from the random solver.
 * Runs until interrupted.
 */
object RandomBenchmark {
  val parser = new scopt.OptionParser[Config]("randomBenchmark") {
    head("randomBenchmark", "0.1")
    opt[Int]('t', "EOT") text "end of time (default 3)" action { (x, c) => c.copy(eot = x)}
    opt[Int]('f', "EFF") text "end of finite failures (default 2)" action { (x, c) => c.copy(eff = x)}
    opt[Int]('c', "crashes") text "crash failures (default 0)" action { (x, c) => c.copy(crashes = x)}
    opt[String]('N', "nodes") text "a comma-separated list of nodes (required)" required() action { (x, c) => c.copy(nodes = x.split(','))}
    arg[File]("<file>...") unbounded() minOccurs 1 text "Dedalus files" action { (x, c) => c.copy(inputPrograms = c.inputPrograms :+ x)}
  }
  def main(args: Array[String]) {
    val metrics = new MetricRegistry
    parser.parse(args, Config(strategy = "random")) map { config =>
      val outputFile = new File("random-benchmark.csv")
      val csvWriter = CSVWriter.open(outputFile)
      csvWriter.writeRow(Seq("numRuns", "counterexample"))
      try {
        while (true) {
          breakable {
            var numRuns = 0
            for (run <- SyncFTChecker.check(config, metrics)) {
              numRuns += 1
              if (run.status == RunStatus("failure")) {
                csvWriter.writeRow(Seq(numRuns, run.failureSpec))
                break()
              }
            }
          }
        }
      } finally {
        csvWriter.close()
      }
    }
  }
}
