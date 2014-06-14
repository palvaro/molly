package edu.berkeley.cs.boom.molly

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Matchers, PropSpec}
import java.io.File
import com.codahale.metrics.MetricRegistry


class CounterexampleSuite extends PropSpec with TableDrivenPropertyChecks with Matchers {

  val examplesFTPath = SyncFTChecker.getClass.getClassLoader.getResource("examples_ft").getPath

  val scenarios = Table(
    ("Input programs",                       "eot",   "eff",                "nodes",    "crashes",    "should find counterexample"),
    (Seq("delivery/simplog.ded", "delivery/deliv_assert.ded"),      6,      3,     Seq("a", "b", "c"),            0,    true),
    (Seq("delivery/rdlog.ded", "delivery/deliv_assert.ded"),        6,      3,     Seq("a", "b", "c"),            0,    false),
    (Seq("delivery/rdlog.ded", "delivery/deliv_assert.ded"),        6,      3,     Seq("a", "b", "c"),            1,    true),
    // classic reliable broadcast fails in the omission model
    (Seq("delivery/classic_rb.ded", "delivery/deliv_assert.ded"),   6,      3,     Seq("a", "b", "c"),            0,    true),
    // but is robust in the fail-stop model.
    (Seq("delivery/classic_rb.ded", "delivery/deliv_assert.ded"),   6,      0,     Seq("a", "b", "c"),            2,    false),
    (Seq("delivery/replog.ded", "delivery/deliv_assert.ded"),       6,      3,     Seq("a", "b", "c"),            0,    false),
    (Seq("delivery/replog.ded", "delivery/deliv_assert.ded"),       6,      3,     Seq("a", "b", "c"),            1,    false),
    (Seq("delivery/ack_rb.ded", "delivery/deliv_assert.ded"),       6,      3,     Seq("a", "b", "c"),            1,    false),
    (Seq("commit/2pc.ded", "commit/2pc_assert.ded"),            7,      3,     Seq("a", "b", "C", "d"),       0,    false),
    (Seq("commit/2pc.ded", "commit/2pc_assert.ded"),            6,      3,     Seq("a", "b", "C", "d"),       1,    true),
    // naive commit/2pc has executions that don't decide even if the model is fail-stop.
    (Seq("commit/2pc.ded", "commit/2pc_assert.ded"),            6,      0,     Seq("a", "b", "C", "d"),       1,    true),
    (Seq("commit/2pc.ded", "commit/2pc_assert.ded"),            6,      0,     Seq("a", "b", "C", "d"),       2,    true),
    // indeed, even if we ignore executions where the coordinator fails:
    (Seq("commit/2pc.ded", "commit/2pc_assert_optimist.ded"),            6,      0,     Seq("a", "b", "C", "d"),       1,    true),
    (Seq("commit/2pc.ded", "commit/2pc_assert_optimist.ded"),            6,      0,     Seq("a", "b", "C", "d"),       2,    true),
    // with timeout+abort at the coordinator, we get termination when the coordinator doesn't fail
    (Seq("commit/2pc_timeout.ded", "commit/2pc_assert_optimist.ded"),            6,      0,     Seq("a", "b", "C", "d"),       1,    false),
    (Seq("commit/2pc_timeout.ded", "commit/2pc_assert_optimist.ded"),            6,      0,     Seq("a", "b", "C", "d"),       2,    false),
    // but honestly...
    (Seq("commit/2pc_timeout.ded", "commit/2pc_assert.ded"),            6,      0,     Seq("a", "b", "C", "d"),       1,    true),
    (Seq("commit/2pc_timeout.ded", "commit/2pc_assert.ded"),            6,      0,     Seq("a", "b", "C", "d"),       2,    true),

    // even the collaborative termination protocol has executions that don't decide.   
    (Seq("commit/2pc_ctp.ded", "commit/2pc_assert.ded"),        6,      0,     Seq("a", "b", "C", "d"),       1,    true),
    (Seq("commit/2pc_ctp.ded", "commit/2pc_assert.ded"),        6,      0,     Seq("a", "b", "C", "d"),       2,    true),

    // commit/3pc (yay?) is "nonblocking" in the synchronous, fail-stop model
    (Seq("commit/3pc.ded", "commit/2pc_assert.ded"),        8,      0,     Seq("a", "b", "C", "d"),       1,    false),
    (Seq("commit/3pc.ded", "commit/2pc_assert.ded"),        8,      0,     Seq("a", "b", "C", "d"),       2,    false),

    // somewhat surprised though that we can't break it's synchronicity assumptions by dropping messages...
    //(Seq("commit/3pc.ded", "commit/2pc_assert.ded"),        9,      7,     Seq("a", "b", "C", "d"),       1,    true),

    (Seq("tokens.ded"),                           6,      3,     Seq("a", "b", "c", "d"),       1,    true),
    (Seq("tokens.ded"),                           6,      3,     Seq("a", "b", "c", "d"),       0,    false)
  
    // simulating the kafka bug
    //(Seq("kafka.ded"),                           6,      4,     Seq("a", "b", "c", "C", "Z"),       1,    true),
    //(Seq("kafka.ded"),                           6,      4,     Seq("a", "b", "c", "C", "Z"),       0,    false)
  )

  property("SAT guided search should correctly find counterexamples") {
    forAll(scenarios) { (inputPrograms: Seq[String], eot: Int, eff: Int, nodes: Seq[String], crashes: Int, shouldFindCounterexample: Boolean) =>
      val inputFiles = inputPrograms.map(name => new File(examplesFTPath, name))
      val config = Config(eot, eff, crashes, nodes, inputFiles)
      val metrics = new MetricRegistry
      val results = SyncFTChecker.check(config, metrics)
      val counterexamples = results.filter(_.status == RunStatus("failure")).map(_.failureSpec)
      if (shouldFindCounterexample) {
        counterexamples should not be empty
      } else {
        counterexamples should be (empty)
      }
    }
  }
}
