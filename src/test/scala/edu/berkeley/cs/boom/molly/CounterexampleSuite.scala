package edu.berkeley.cs.boom.molly

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Matchers, PropSpec}
import java.io.File


class CounterexampleSuite extends PropSpec with TableDrivenPropertyChecks with Matchers {

  val examplesFTPath = SyncFTChecker.getClass.getClassLoader.getResource("examples_ft").getPath

  val scenarios = Table(
    ("Input programs",                       "eot",   "eff",                "nodes",    "crashes",    "should find counterexample"),
    (Seq("simplog.ded", "deliv_assert.ded"),      6,      3,     Seq("a", "b", "c"),            0,    true),
    (Seq("rdlog.ded", "deliv_assert.ded"),        6,      3,     Seq("a", "b", "c"),            0,    false),
    (Seq("rdlog.ded", "deliv_assert.ded"),        6,      3,     Seq("a", "b", "c"),            1,    true),
    (Seq("classic_rb.ded", "deliv_assert.ded"),   6,      3,     Seq("a", "b", "c"),            0,    true),
    (Seq("replog.ded", "deliv_assert.ded"),       6,      3,     Seq("a", "b", "c"),            0,    false),
    (Seq("replog.ded", "deliv_assert.ded"),       6,      3,     Seq("a", "b", "c"),            1,    false),
    (Seq("ack_rb.ded", "deliv_assert.ded"),       6,      3,     Seq("a", "b", "c"),            1,    false),
    (Seq("2pc.ded", "2pc_assert.ded"),            7,      3,     Seq("a", "b", "C", "d"),       0,    false),
    (Seq("2pc.ded", "2pc_assert.ded"),            6,      3,     Seq("a", "b", "C", "d"),       1,    true),
    (Seq("tokens.ded"),                           6,      3,     Seq("a", "b", "C", "d"),       1,    true)
  )

  property("SAT guided search should correctly find counterexamples") {
    forAll(scenarios) { (inputPrograms: Seq[String], eot: Int, eff: Int, nodes: Seq[String], crashes: Int, shouldFindCounterexample: Boolean) =>
      val inputFiles = inputPrograms.map(name => new File(examplesFTPath, name))
      val config = Config(eot, eff, crashes, nodes, inputFiles)
      val results = SyncFTChecker.check(config)
      val counterexamples = results.filter(_.status == RunStatus("failure")).map(_.failureSpec)
      if (shouldFindCounterexample) {
        counterexamples should not be empty
      } else {
        counterexamples should be (empty)
      }
    }
  }
}
