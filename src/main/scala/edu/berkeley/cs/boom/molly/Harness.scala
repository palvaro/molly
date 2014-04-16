package edu.berkeley.cs.boom.molly
import java.io.File
import scala.io.Source
import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.cs.boom.molly.DedalusRewrites._
import edu.berkeley.cs.boom.molly.DedalusParser._
import edu.berkeley.cs.boom.molly.ast._
import edu.berkeley.cs.boom.molly.report.HTMLWriter
import org.scalatest.prop.TableDrivenPropertyChecks.{Table => MyTable}
import com.codahale.metrics.{Slf4jReporter, MetricRegistry, CsvReporter, ConsoleReporter}
import java.util.concurrent.TimeUnit
import scalaz.syntax.id._
import scalaz.EphemeralStream
import java.util.{Date, Locale}

object Harness extends Logging {

  val s1 = MyTable(
    ("Input programs",                       "eot",   "eff",                "nodes",    "crashes",    "should find counterexample"),
    (Seq("ack_rb.ded", "deliv_assert.ded"),       6,      3,     Seq("a", "b", "c"),            1,    false)
  )

  val scenarios = MyTable(
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

  def main(args: Array[String]) {
    //scenarios.foreach {case (inputPrograms: Seq[String], eot: Int, eff: Int, nodes: Seq[String], crashes: Int, shouldFindCounterexample: Boolean) =>
    s1.foreach {case (inputPrograms: Seq[String], eot: Int, eff: Int, nodes: Seq[String], crashes: Int, shouldFindCounterexample: Boolean) =>


  //implicit val metrics: MetricRegistry = new MetricRegistry()
    SyncFTChecker.resetMetrics()
  val csvreporter = CsvReporter.forRegistry(SyncFTChecker.metrics)
                                        .formatFor(Locale.US)
                                        .convertRatesTo(TimeUnit.SECONDS)
                                        .convertDurationsTo(TimeUnit.MILLISECONDS)
                                        .build(new File("./metrics/"));


  val consolereporter = ConsoleReporter.forRegistry(SyncFTChecker.metrics)
                                        .convertRatesTo(TimeUnit.SECONDS)
                                        .convertDurationsTo(TimeUnit.MILLISECONDS)
                                        .build();



      //val inputFiles = inputPrograms.map(name => new File(examplesFTPath, name))

      val inputFiles = inputPrograms.map(name => new File("../examples_ft/" + name))
      val config = Config(eot, eff, crashes, nodes, inputFiles)
      SyncFTChecker.check(config)      
      csvreporter.report()
      //consolereporter.report()
      
      
      //logger.info("OK REPORTED")
    }

      //csvreporter.report()
  }
  
}

 


