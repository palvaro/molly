package edu.berkeley.cs.boom.molly.paperexperiments

import java.io.File
import com.github.tototoshi.csv.CSVWriter
import edu.berkeley.cs.boom.molly.{FailureSpec, RunStatus, SyncFTChecker, Config}
import com.codahale.metrics.MetricRegistry

/**
 * Generates the data for the table of counterexamples in the paper.
 */
object TableOfCounterexamples {

  val NUM_RANDOM_RUNS = 25

  val programs = Seq(
    // ("Input programs", "eot", "eff", "crashes", "nodes")
    (Seq("delivery/simplog.ded", "delivery/deliv_assert.ded"),    4, 2, 0, Seq("a", "b", "c")),  // AKA simple-deliv
    (Seq("delivery/rdlog.ded", "delivery/deliv_assert.ded"),      4, 2, 1, Seq("a", "b", "c")), // AKA retry-deliv
    (Seq("delivery/classic_rb.ded", "delivery/deliv_assert.ded"), 5, 3, 0, Seq("a", "b", "c")),  // AKA classic-deliv
    (Seq("commit/2pc.ded", "commit/2pc_assert.ded"),          5, 0, 1, Seq("a", "b", "C", "d")),
    (Seq("commit/2pc_ctp.ded", "commit/2pc_assert.ded"),      8, 0, 1, Seq("a", "b", "C", "d")),
    (Seq("commit/3pc.ded", "commit/2pc_assert.ded"),          9, 7, 1, Seq("a", "b", "C", "d")),
    (Seq("kafka.ded"),                          6, 4, 1, Seq("a", "b", "c", "C", "Z"))
  )

  /**
   * Run the analyzer until it finds the first counterexample.
   *
   * @param config the analyzer and program configuration
   * @return (runtime (seconds), num runs)
   */
  def runUntilFirstCounterexample(config: Config): (Double, Int) = {
    System.gc()  // Run a full GC so we don't count time spent cleaning up earlier runs.
    val metrics: MetricRegistry = new MetricRegistry()
    var runsCount = 0
    val startTime = System.currentTimeMillis()
    var runs = SyncFTChecker.check(config, metrics) // An ephemeral stream
    while (!runs.isEmpty) {
      runsCount += 1
      val result = runs.head.apply()
      if (result.status == RunStatus("failure")) {
        val duration = (System.currentTimeMillis() - startTime) / 1000.0
        return (duration, runsCount)
      } else {
        runs = runs.tail()
      }
    }
    throw new IllegalStateException("Should have found a counterexample for config " + config)
  }

  /**
   * Warm up the JVM so we get more accurate timing for the first experiment.
   */
  private def warmup() {
    val (inputPrograms, eot, eff, crashes, nodes) = programs.head
    val inputFiles = inputPrograms.map(name => new File("../examples_ft/" + name))
    val config = Config(eot, eff, crashes, nodes, inputFiles, strategy = "random",
      useSymmetry = false, disableDotRendering = true)
    val metrics: MetricRegistry = new MetricRegistry()
    val runs = SyncFTChecker.check(config, metrics) // An ephemeral stream
    runs.take(100).toArray // Force evaluation
  }

  def main(args: Array[String]) {
    warmup()
    val csvFile = new File("table_of_counterexamples.csv")
    val csvWriter = CSVWriter.open(csvFile)
    val header = Seq("program", "eot", "eff", "crashes", "bound", "mean_random_exe", "mean_random_time",
      "all_random_data", "backward_exe", "backward_time", "symm_exe", "symm_time", "causal_exe", "causal_time")
    csvWriter.writeRow(header)
    try {
      for ((inputPrograms, eot, eff, crashes, nodes) <- programs) {
        val inputFiles = inputPrograms.map(name => new File("../examples_ft/" + name))
        val randomConfig = Config(eot, eff, crashes, nodes, inputFiles, strategy = "random",
          useSymmetry = false, disableDotRendering = true)
        val backwardConfig = randomConfig.copy(strategy = "sat")
        val symmetryConfig = backwardConfig.copy(useSymmetry = true)
        val causalConfig = backwardConfig.copy(strategy = "pcausal")
        val grossEstimate = FailureSpec(eot, eff, crashes, nodes.toList).grossEstimate
        val randomResults = (1 to NUM_RANDOM_RUNS).map { _ => runUntilFirstCounterexample(randomConfig)}
        val meanRandomTime = randomResults.map(_._1).sum / (1.0 * NUM_RANDOM_RUNS)
        val meanRandomExe = randomResults.map(_._2).sum / (1.0 * NUM_RANDOM_RUNS)
        val (backwardTime, backwardExe) = runUntilFirstCounterexample(backwardConfig)
        val (symmetryTime, symmetryExe) = runUntilFirstCounterexample(symmetryConfig)
        val (causalTime, causalExe) = runUntilFirstCounterexample(causalConfig)
        csvWriter.writeRow(Seq(inputPrograms, eot, eff, crashes, grossEstimate, meanRandomExe,
          meanRandomTime, randomResults, backwardExe, backwardTime, symmetryExe, symmetryTime, causalExe, causalTime))
      }
    } finally {
      csvWriter.close()
    }
  }
}
