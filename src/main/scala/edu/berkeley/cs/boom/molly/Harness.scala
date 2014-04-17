package edu.berkeley.cs.boom.molly

import java.io.File
import com.typesafe.scalalogging.slf4j.Logging
import org.scalatest.prop.TableDrivenPropertyChecks.{Table => ScalatestTable}
import com.codahale.metrics.MetricRegistry
import java.util.concurrent.TimeUnit
import com.fasterxml.jackson.databind.ObjectMapper
import com.codahale.metrics.json.MetricsModule
import com.github.tototoshi.csv.CSVWriter
import scala.util.control.Breaks._

object Harness extends Logging {

  val objectMapper = new ObjectMapper().registerModule(
    new MetricsModule(TimeUnit.SECONDS, TimeUnit.MILLISECONDS, false))
  val outputFile = new File("harness-results.csv")
  val csvWriter = CSVWriter.open(outputFile)

  val scenarios = ScalatestTable(
    ("Input programs",                       "eot",   "eff",                "nodes",    "crashes",    "should find counterexample"),
    (Seq("simplog.ded", "deliv_assert.ded"),      6,      3,     Seq("a", "b", "c"),            0,    true),
    (Seq("rdlog.ded", "deliv_assert.ded"),        6,      3,     Seq("a", "b", "c"),            0,    false),
    (Seq("rdlog.ded", "deliv_assert.ded"),        6,      3,     Seq("a", "b", "c"),            1,    true),
    // classic reliable broadcast fails in the omission model
    (Seq("classic_rb.ded", "deliv_assert.ded"),   6,      3,     Seq("a", "b", "c"),            0,    true),
    // but is robust in the fail-stop model.
    (Seq("classic_rb.ded", "deliv_assert.ded"),   6,      0,     Seq("a", "b", "c"),            2,    false),
    (Seq("replog.ded", "deliv_assert.ded"),       6,      3,     Seq("a", "b", "c"),            0,    false),
    (Seq("replog.ded", "deliv_assert.ded"),       6,      3,     Seq("a", "b", "c"),            1,    false),
    (Seq("ack_rb.ded", "deliv_assert.ded"),       6,      3,     Seq("a", "b", "c"),            1,    false),
    (Seq("2pc.ded", "2pc_assert.ded"),            7,      3,     Seq("a", "b", "C", "d"),       0,    false),
    (Seq("2pc.ded", "2pc_assert.ded"),            6,      3,     Seq("a", "b", "C", "d"),       1,    true),
    // naive 2pc has executions that don't decide even if the model is fail-stop.
    (Seq("2pc.ded", "2pc_assert.ded"),            6,      0,     Seq("a", "b", "C", "d"),       1,    true),
    (Seq("2pc.ded", "2pc_assert.ded"),            6,      0,     Seq("a", "b", "C", "d"),       2,    true),
    // indeed, even if we ignore executions where the coordinator fails:
    (Seq("2pc.ded", "2pc_assert_optimist.ded"),            6,      0,     Seq("a", "b", "C", "d"),       1,    true),
    (Seq("2pc.ded", "2pc_assert_optimist.ded"),            6,      0,     Seq("a", "b", "C", "d"),       2,    true),
    // with timeout+abort at the coordinator, we get termination when the coordinator doesn't fail
    (Seq("2pc_timeout.ded", "2pc_assert_optimist.ded"),            6,      0,     Seq("a", "b", "C", "d"),       1,    false),
    (Seq("2pc_timeout.ded", "2pc_assert_optimist.ded"),            6,      0,     Seq("a", "b", "C", "d"),       2,    false),
    // but honestly...
    (Seq("2pc_timeout.ded", "2pc_assert.ded"),            6,      0,     Seq("a", "b", "C", "d"),       1,    true),
    (Seq("2pc_timeout.ded", "2pc_assert.ded"),            6,      0,     Seq("a", "b", "C", "d"),       2,    true),

    // even the collaborative termination protocol has executions that don't decide.   
    (Seq("2pc_ctp.ded", "2pc_assert.ded"),        6,      0,     Seq("a", "b", "C", "d"),       1,    true),
    (Seq("2pc_ctp.ded", "2pc_assert.ded"),        6,      0,     Seq("a", "b", "C", "d"),       2,    true),

    // 3pc (yay?) is "nonblocking" in the synchronous, fail-stop model
    (Seq("3pc.ded", "2pc_assert.ded"),        8,      0,     Seq("a", "b", "C", "d"),       1,    false),
    (Seq("3pc.ded", "2pc_assert.ded"),        8,      0,     Seq("a", "b", "C", "d"),       2,    false),

    // somewhat surprised though that we can't break it's synchronicity assumptions by dropping messages...
    //(Seq("3pc.ded", "2pc_assert.ded"),        9,      7,     Seq("a", "b", "C", "d"),       1,    true),

    (Seq("tokens.ded"),                           6,      3,     Seq("a", "b", "c", "d"),       1,    true),
    (Seq("tokens.ded"),                           6,      3,     Seq("a", "b", "c", "d"),       0,    false)
  
    // simulating the kafka bug
    //(Seq("kafka.ded"),                           6,      4,     Seq("a", "b", "c", "C", "Z"),       1,    true),
    //(Seq("kafka.ded"),                           6,      4,     Seq("a", "b", "c", "C", "Z"),       0,    false)
  )

  
  def checker(config: Config, scenario: (Seq[String], Int, Int, Seq[String], Int, Boolean)) {
    val eots = List(5, 6, 7, 8, 9, 10)
    breakable {
      eots.foreach(e => 
        if (checker_run(config.copy(eot = e, eff = e-3), scenario))
          break
      )
    }
  }

  def checker_run(config: Config, scenario: (Seq[String], Int, Int, Seq[String], Int, Boolean)) : Boolean = {
    val tm = System.currentTimeMillis()
    logger.debug(s"ok eot is $config.eot and eff is $config.eff")
    val metrics: MetricRegistry = new MetricRegistry()
    val (successes, counterexamples) =
      SyncFTChecker.check(config, metrics).partition(_.status == RunStatus("success"))
    // Compute these counts here to ensure that the ephemeral stream is evaluated before
    // we retrieve the other metrics from the registry

    val duration = (System.currentTimeMillis() - tm) / 1000
    val successCount = successes.size
    val counterexampleCount = counterexamples.size
    val metricsJson = objectMapper.writeValueAsString(metrics)
    val failureSpec = FailureSpec(config.eot, config.eff, config.crashes, config.nodes.toList)


    csvWriter.writeRow(scenario.copy(_2 = config.eot, _3 = config.eff).productIterator.map(_.toString).toSeq ++
      Seq(successCount, counterexampleCount, failureSpec.grossEstimate, duration, metricsJson))

    return (counterexamples.size > 0)
  }

  def main(args: Array[String]) {
    // For now, treat the results as a mixed of structured and semi-structured data.
    // If there are specific metrics that we want to be included in the spreadsheet,
    // we can pull them out and add them as extra columns.  To capture all of the rest
    // of the metrics, we write them into a JSON-valued field.
    //val csvWriter = CSVWriter.open(outputFile)
    val header =
      scenarios.heading.productIterator.toSeq ++ Seq("successes", "counterexamples", "upper bound", "duration(secs)", "metrics")
    csvWriter.writeRow(header)
    try {
      scenarios.foreach {
        case scenario @ (inputPrograms: Seq[String], eot: Int, eff: Int, nodes: Seq[String],
          crashes: Int, shouldFindCounterexample: Boolean) =>

          
          val inputFiles = inputPrograms.map(name => new File("../examples_ft/" + name))
          val config = Config(eot, eff, crashes, nodes, inputFiles)
          checker(config, scenario)
      }
    } finally {
      csvWriter.close()
    }
  }
  
}

 


