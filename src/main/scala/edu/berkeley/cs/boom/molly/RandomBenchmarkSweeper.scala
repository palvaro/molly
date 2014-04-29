package edu.berkeley.cs.boom.molly

import java.io.File
import com.github.tototoshi.csv.CSVWriter
import edu.berkeley.cs.boom.molly.wrappers.{C4, APR}
import jnr.ffi.{Pointer, LibraryLoader}
import jnr.ffi.byref.PointerByReference


/**
 * Benchmarks the randomized search strategy against an entire corpus of programs.
 */
object RandomBenchmarkSweeper {

  val OUTPUT_DIR = "random_benchmarks"
  val NUM_RUNS = 100

  val corpus = Seq(
    // ("Input programs", "eot", "eff", "nodes", "crashes")
    (Seq("simplog.ded", "deliv_assert.ded"),  4,   2, Seq("a", "b", "c"), 0),
    (Seq("rdlog.ded", "deliv_assert.ded"),     4,  2, Seq("a", "b", "c"), 1)
    //(Seq("replog.ded", "deliv_assert.ded"),     8, 6, Seq("a", "b", "c"), 1),
    //(Seq("classic_rb.ded", "deliv_assert.ded"), 5, 3, Seq("a", "b", "c"), 0),
    //(Seq("2pc.ded", "2pc_assert.ded"),          5, 0, Seq("a", "b", "C", "d"), 1),
    //(Seq("2pc_ctp.ded", "2pc_assert.ded"),      6, 0, Seq("a", "b", "C", "d"), 1),
    //(Seq("3pc.ded", "2pc_assert.ded"),          9, 7, Seq("a", "b", "C", "d"), 1),
    //(Seq("kafka.ded"),                          6, 4, Seq("a", "b", "c", "C", "Z"), 1)
  )

  def main(args: Array[String]) {
    val outputDirectory = new File(OUTPUT_DIR)
    outputDirectory.mkdir()

    corpus.par.foreach { case (inputPrograms, eot, eff, nodes, crashes) =>
      val inputFiles = inputPrograms.map(name => new File("../examples_ft/" + name))
      val config = Config(eot, eff, crashes, nodes, inputFiles, strategy = "random")
      val csvFile = new File(outputDirectory, s"${inputPrograms.head}_t_${eot}_f_${eff}_c_${crashes}.csv")
      val csvWriter = CSVWriter.open(csvFile)
      try {
        (1 to NUM_RUNS).foreach { _ =>
          csvWriter.writeRow(RandomBenchmark.findFirstCounterexample(config).productIterator.toSeq)
        }
      } finally {
        csvWriter.close()
      }
    }
  }
}
