package edu.berkeley.cs.boom.molly.paperexperiments

import java.io.File
import com.github.tototoshi.csv.CSVWriter
import edu.berkeley.cs.boom.molly.{RunStatus, FailureSpec, SyncFTChecker, Config}
import com.codahale.metrics.MetricRegistry

/**
 * Generates the data for the table of correct programs in the paper.
 */
object TableOfCorrectPrograms {
  val programs = Seq(
    // ("Input programs", "eot", "eff", "crashes", "nodes")
    (Seq("rdlog.ded", "deliv_assert.ded"),    25, 23, 0, Seq("a", "b", "c")), // AKA retry-deliv
    (Seq("replog.ded", "deliv_assert.ded"),    8,  6, 1, Seq("a", "b", "c")),  // AKA redun-deliv
    (Seq("ack_rb.ded", "deliv_assert.ded"),    8,  6, 1, Seq("a", "b", "c")),  // AKA ack-deliv
    (Seq("paxos_synod.ded"),                   8,  3, 1, Seq("a", "b", "c"))
  )

  /**
   * Run the analyzer until it exhaustively covers the failure space.
   *
   * @param config the analyzer and program configuration
   * @return (runtime (seconds), num runs)
   */
  def runUntilExhaustion(config: Config): (Double, Int) = {
    System.gc()  // Run a full GC so we don't count time spent cleaning up earlier runs.
    val metrics: MetricRegistry = new MetricRegistry()
    var runsCount = 0
    val startTime = System.currentTimeMillis()
    var runs = SyncFTChecker.check(config, metrics) // An ephemeral stream
    while (!runs.isEmpty) {
      runsCount += 1
      assert (runs.head().status == RunStatus("success"))
      runs = runs.tail()
    }
    val duration = (System.currentTimeMillis() - startTime) / 1000.0
    (duration, runsCount)
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
    val csvFile = new File("table_of_correct_programs.csv")
    val csvWriter = CSVWriter.open(csvFile)
    val header = Seq("program", "eot", "eff", "crashes", "bound", "backward_exe", "backward_time",
      "symm_exe", "symm_time")
    csvWriter.writeRow(header)
    try {
      for ((inputPrograms, eot, eff, crashes, nodes) <- programs) {
        val inputFiles = inputPrograms.map(name => new File("../examples_ft/" + name))
        val backwardConfig = Config(eot, eff, crashes, nodes, inputFiles, strategy = "sat",
          useSymmetry = false, disableDotRendering = true)
        val symmetryConfig = backwardConfig.copy(useSymmetry = true)
        val grossEstimate = FailureSpec(eot, eff, crashes, nodes.toList).grossEstimate
        val (backwardTime, backwardExe) = runUntilExhaustion(backwardConfig)
        val (symmetryTime, symmetryExe) = runUntilExhaustion(symmetryConfig)
        csvWriter.writeRow(Seq(inputPrograms, eot, eff, crashes, grossEstimate,
          backwardExe, backwardTime, symmetryExe, symmetryTime))
      }
    } finally {
      csvWriter.close()
    }
  }
}
