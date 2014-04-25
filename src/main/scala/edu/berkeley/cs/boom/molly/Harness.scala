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
import scala.concurrent.duration._
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.language.postfixOps

object Harness extends Logging {

  val objectMapper = new ObjectMapper().registerModule(
    new MetricsModule(TimeUnit.SECONDS, TimeUnit.MILLISECONDS, false))
  val outputFile = new File("harness-results.csv")
  val csvWriter = CSVWriter.open(outputFile)

  val cherries = ScalatestTable(
    ("Input programs",                       "eot",   "eff",                "nodes",    "crashes",    "should find counterexample"),
    (Seq("simplog.ded", "deliv_assert.ded"),      4,      2,     Seq("a", "b", "c"),            0,    true),
    (Seq("rdlog.ded", "deliv_assert.ded"),      25,      23,     Seq("a", "b", "c"),            0,    true),
    (Seq("rdlog.ded", "deliv_assert.ded"),      4,      2,     	 Seq("a", "b", "c"),            1,    true),
    (Seq("replog.ded", "deliv_assert.ded"),      8,      6,     Seq("a", "b", "c"),            1,    true),
    (Seq("classic_rb.ded", "deliv_assert.ded"),   5,      3,     Seq("a", "b", "c"),            0,    true),
    (Seq("2pc.ded", "2pc_assert.ded"),      5,      0,     Seq("a", "b", "C", "d"),            1,    true),
    (Seq("2pc_ctp.ded", "2pc_assert.ded"),        6,      0,     Seq("a", "b", "C", "d"),       1,    true),
    (Seq("2pc_timeout.ded", "2pc_assert_optimist.ded"),            6,      0,     Seq("a", "b", "C", "d"),       2,    false),
    (Seq("3pc.ded", "2pc_assert.ded"),        8,      0,     Seq("a", "b", "C", "d"),       2,    false),
    (Seq("3pc.ded", "2pc_assert.ded"),        9,      7,     Seq("a", "b", "C", "d"),       1,    true),

    (Seq("kafka.ded"),                           6,      4,     Seq("a", "b", "c", "C", "Z"),       1,    true),
    (Seq("kafka.ded"),                           6,      4,     Seq("a", "b", "c", "C", "Z"),       0,    false),

    (Seq("ack_rb.ded", "deliv_assert.ded"),       8,      6,     Seq("a", "b", "c"),            1,    false)
  )

  val scenarios = ScalatestTable(
    ("Input programs",                                      "nodes",    "crashes",    "should find counterexample"),
    (Seq("simplog.ded", "deliv_assert.ded"),     Seq("a", "b", "c"),            0,    true),
    (Seq("rdlog.ded", "deliv_assert.ded"),       Seq("a", "b", "c"),            0,    false),
    (Seq("rdlog.ded", "deliv_assert.ded"),       Seq("a", "b", "c"),            1,    true),
    // classic reliable broadcast fails in the omission model
    (Seq("classic_rb.ded", "deliv_assert.ded"),  Seq("a", "b", "c"),            0,    true),
    // but is robust in the fail-stop model.
    (Seq("classic_rb.ded", "deliv_assert.ded"),  Seq("a", "b", "c"),            2,    false),
    (Seq("replog.ded", "deliv_assert.ded"),      Seq("a", "b", "c"),            0,    false),
    (Seq("replog.ded", "deliv_assert.ded"),      Seq("a", "b", "c"),            1,    false),
    (Seq("ack_rb.ded", "deliv_assert.ded"),      Seq("a", "b", "c"),            1,    false),
    (Seq("2pc.ded", "2pc_assert.ded"),           Seq("a", "b", "C", "d"),       0,    false),
    (Seq("2pc.ded", "2pc_assert.ded"),           Seq("a", "b", "C", "d"),       1,    true),
    // naive 2pc has executions that don't decide even if the model is fail-stop.
    (Seq("2pc.ded", "2pc_assert.ded"),           Seq("a", "b", "C", "d"),       1,    true),
    (Seq("2pc.ded", "2pc_assert.ded"),           Seq("a", "b", "C", "d"),       2,    true),
    // indeed, even if we ignore executions where the coordinator fails:
    (Seq("2pc.ded", "2pc_assert_optimist.ded"),  Seq("a", "b", "C", "d"),       1,    true),
    (Seq("2pc.ded", "2pc_assert_optimist.ded"),  Seq("a", "b", "C", "d"),       2,    true),
    // with timeout+abort at the coordinator, we get termination when the coordinator doesn't fail
    (Seq("2pc_timeout.ded", "2pc_assert_optimist.ded"),  Seq("a", "b", "C", "d"),       1,    false),
    (Seq("2pc_timeout.ded", "2pc_assert_optimist.ded"),  Seq("a", "b", "C", "d"),       2,    false),
    // but honestly...
    (Seq("2pc_timeout.ded", "2pc_assert.ded"),   Seq("a", "b", "C", "d"),       1,    true),
    (Seq("2pc_timeout.ded", "2pc_assert.ded"),   Seq("a", "b", "C", "d"),       2,    true),

    // even the collaborative termination protocol has executions that don't decide.
    (Seq("2pc_ctp.ded", "2pc_assert.ded"),       Seq("a", "b", "C", "d"),       1,    true),
    (Seq("2pc_ctp.ded", "2pc_assert.ded"),       Seq("a", "b", "C", "d"),       2,    true),

    // 3pc (yay?) is "nonblocking" in the synchronous, fail-stop model
    (Seq("3pc.ded", "2pc_assert.ded"),           Seq("a", "b", "C", "d"),       1,    false),
    (Seq("3pc.ded", "2pc_assert.ded"),           Seq("a", "b", "C", "d"),       2,    false),

    // somewhat surprised though that we can't break it's synchronicity assumptions by dropping messages...
    //(Seq("3pc.ded", "2pc_assert.ded"),        9,      7,     Seq("a", "b", "C", "d"),       1,    true),


    (Seq("tokens.ded"),                          Seq("a", "b", "c", "d"),       1,    true),
    (Seq("tokens.ded"),                          Seq("a", "b", "c", "d"),       0,    false)

    // simulating the kafka bug
    //(Seq("kafka.ded"),                         Seq("a", "b", "c", "C", "Z"),       1,    true),
    //(Seq("kafka.ded"),                         Seq("a", "b", "c", "C", "Z"),       0,    false)
  )

  private val TIMEOUT = 500  // in seconds
  private val CHERRIES = true

  private def check(crashes: Int, nodes: Seq[String], inputPrograms: Seq[File], eot: Int, eff: Int) = {
    val tm = System.currentTimeMillis()
    val config = new Config(eot, eff, crashes, nodes, inputPrograms)
    val failureSpec = FailureSpec(eot, eff, crashes, nodes.toList)
    val metrics: MetricRegistry = new MetricRegistry()

    try {
      val result = future {
        var successCount = 0
        var counterexampleCount = 0
        var runs = SyncFTChecker.check(config, metrics) // An ephemeral stream
        while (!runs.isEmpty && counterexampleCount == 0) {
          // Stop if we've found a counterexample
          val result = runs.head.apply()
          if (result.status == RunStatus("success")) {
            successCount += 1
          } else {
            counterexampleCount += 1
          }
          runs = runs.tail()
        }
        val duration = (System.currentTimeMillis() - tm) / 1000.0
        val metricsJson = objectMapper.writeValueAsString(metrics)
        (eot, eff, successCount, counterexampleCount, failureSpec.grossEstimate, duration, metricsJson)
      }
      Await.result(result, TIMEOUT seconds)
    } catch {
      case _: TimeoutException =>
        (eot, eff, -1, -1, failureSpec.grossEstimate, TIMEOUT, "")
    }

  }

  def main(args: Array[String]) {
    // For now, treat the results as a mixed of structured and semi-structured data.
    // If there are specific metrics that we want to be included in the spreadsheet,
    // we can pull them out and add them as extra columns.  To capture all of the rest
    // of the metrics, we write them into a JSON-valued field.
    val header =
      scenarios.heading.productIterator.toSeq ++
        Seq("eot", "eff", "successes", "counterexamples", "upper bound", "duration(secs)", "metrics")
    csvWriter.writeRow(header)
    try {
      if (CHERRIES) {
      	cherries.foreach {
      	  case scenario @ (inputPrograms: Seq[String], eot: Int, eff: Int, nodes: Seq[String],
            crashes: Int, shouldFindCounterexample: Boolean) =>

            val inputFiles = inputPrograms.map(name => new File("../examples_ft/" + name))
	    val outcome = check(crashes, nodes, inputFiles, eot, eff)
	    val newHeader = Seq(inputPrograms, nodes, crashes, shouldFindCounterexample)
	    csvWriter.writeRow(newHeader ++ outcome.productIterator.toSeq)
	}
      } else {
        scenarios.foreach {
          case scenario @ (inputPrograms: Seq[String], nodes: Seq[String], crashes: Int, shouldFindCounterexample: Boolean) =>
            val inputFiles = inputPrograms.map(name => new File("../examples_ft/" + name))
            // Incrementally increase the EOT and stop if we find a counterexample:
            val eots = 5 to 10
            breakable {
              eots.foreach { eot =>
                val outcome = check(crashes, nodes, inputFiles, eot, eot - 3)
                csvWriter.writeRow(scenario.productIterator.toSeq ++ outcome.productIterator.toSeq)
                if (outcome._4 > 0) // If we found a counterexample
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
