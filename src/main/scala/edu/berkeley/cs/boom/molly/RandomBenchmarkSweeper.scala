package edu.berkeley.cs.boom.molly

import scala.sys.process._

/**
 * Benchmarks the randomized search strategy against an entire corpus of programs.
 */
object RandomBenchmarkSweeper {

  val NUM_RUNS = 100

  val corpus = Seq(
    // ("Input programs", "eot", "eff", "nodes", "crashes")
    (Seq("simplog.ded", "deliv_assert.ded"),  4,   2, Seq("a", "b", "c"), 0),
    (Seq("rdlog.ded", "deliv_assert.ded"),     4,  2, Seq("a", "b", "c"), 1),
    (Seq("replog.ded", "deliv_assert.ded"),     8, 6, Seq("a", "b", "c"), 1),
    (Seq("classic_rb.ded", "deliv_assert.ded"), 5, 3, Seq("a", "b", "c"), 0),
    (Seq("2pc.ded", "2pc_assert.ded"),          5, 0, Seq("a", "b", "C", "d"), 1),
    (Seq("2pc_ctp.ded", "2pc_assert.ded"),      6, 0, Seq("a", "b", "C", "d"), 1),
    (Seq("3pc.ded", "2pc_assert.ded"),          9, 7, Seq("a", "b", "C", "d"), 1),
    (Seq("kafka.ded"),                          6, 4, Seq("a", "b", "c", "C", "Z"), 1)
  )

  def main(args: Array[String]) {
    corpus.par.foreach { case (inputPrograms, eot, eff, nodes, crashes) =>
      val inputFiles = inputPrograms.map(name => "../examples_ft/" + name)
      val command = s"-N ${nodes.mkString(",")} -t $eot -f $eff -c $crashes --max-runs $NUM_RUNS ${inputFiles.mkString(" ")}"
      println(s"Running command '$command'")
      Seq("sbt", s"run-main edu.berkeley.cs.boom.molly.RandomBenchmark $command").!
    }
  }
}
