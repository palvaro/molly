package edu.berkeley.cs.boom.molly

import com.codahale.metrics.MetricRegistry
import java.io.File
import com.github.tototoshi.csv.CSVWriter

/**
 * Tool for sampling "number of runs to first counterexample" from the random solver.
 */
object RandomBenchmark {
  val parser = new scopt.OptionParser[Config]("randomBenchmark") {
    head("randomBenchmark", "0.1")
    opt[Int]("max-runs") text "max runs (default unlimited)" action { (x, c) => c.copy(maxRuns = x)}
    opt[Int]('t', "EOT") text "end of time (default 3)" action { (x, c) => c.copy(eot = x)}
    opt[Int]('f', "EFF") text "end of finite failures (default 2)" action { (x, c) => c.copy(eff = x)}
    opt[Int]('c', "crashes") text "crash failures (default 0)" action { (x, c) => c.copy(crashes = x)}
    opt[String]('N', "nodes") text "a comma-separated list of nodes (required)" required() action { (x, c) => c.copy(nodes = x.split(','))}
    arg[File]("<file>...") unbounded() minOccurs 1 text "Dedalus files" action { (x, c) => c.copy(inputPrograms = c.inputPrograms :+ x)}
  }

  def findFirstCounterexample(config: Config): (Int, FailureSpec) = {
    val metrics = new MetricRegistry
    var numRuns = 0
    for (run <- SyncFTChecker.check(config, metrics)) {
      numRuns += 1
      if (run.status == RunStatus("failure")) {
        return (numRuns, run.failureSpec)
      }
    }
    throw new IllegalStateException("Random checker exited without finding a counterexample")
  }

  def main(args: Array[String]) {
    parser.parse(args, Config(strategy = "random")) map { config =>
      val csvFile = new File(s"random_${config.inputPrograms.head.getName}_t_${config.eot}_f_${config.eff}_c_${config.crashes}.csv")
      val csvWriter = CSVWriter.open(csvFile)
      csvWriter.writeRow(Seq("numRuns", "counterexample"))
      try {
        (1 to config.maxRuns).foreach { _ =>
          csvWriter.writeRow(findFirstCounterexample(config).productIterator.toSeq)
        }
      } finally {
        csvWriter.close()
      }
    }
  }
}
