package edu.berkeley.cs.boom.molly.report

import java.io.{PrintWriter, File}
import edu.berkeley.cs.boom.molly.Run
import edu.berkeley.cs.boom.molly.report.MollyCodecJsons._
import argonaut._, Argonaut._
import org.apache.commons.io.FileUtils
import scala.collection.JavaConversions._
import scala.sys.process._
import org.apache.commons.io.filefilter.{FalseFileFilter, TrueFileFilter}
import scalaz.EphemeralStream
import scalaz.syntax.id._
import java.util.concurrent.Executors


object HTMLWriter {

  private val templateDir =
    new File(HTMLWriter.getClass.getClassLoader.getResource("vis_template").getPath)

  private def copyTemplateFiles(outputDirectory: File) {
    FileUtils.iterateFilesAndDirs(templateDir, FalseFileFilter.INSTANCE, TrueFileFilter.INSTANCE).foreach {
      dir => FileUtils.copyDirectoryToDirectory(dir, outputDirectory)
    }
    FileUtils.iterateFiles(templateDir, null, false).foreach {
      file => FileUtils.copyFileToDirectory(file, outputDirectory)
    }
  }

  private def writeGraphviz(dot: String, outputDirectory: File, filename: String) = {
    val dotFile = new File(outputDirectory, s"$filename.dot")
    val svgFile = new File(outputDirectory, s"$filename.svg")
    FileUtils.write(dotFile, dot)
    new Runnable() {
      def run() {
        val dotExitCode = s"dot -Tsvg -o ${svgFile.getAbsolutePath} ${dotFile.getAbsolutePath}".!
        assert(dotExitCode == 0)
      }
    }
  }

  def write(outputDirectory: File, originalPrograms: List[File], runs: EphemeralStream[Run],
            generateProvenanceDiagrams: Boolean, disableDotRendering: Boolean = false) = {
    outputDirectory.mkdirs()
    require (outputDirectory.isDirectory)
    copyTemplateFiles(outputDirectory)
    val runsFile =
      new PrintWriter(FileUtils.openOutputStream(new File(outputDirectory, "runs.json")))
    // Unfortunately, Argonaut doesn't seem to support streaming JSON writing, hence this code:
    var first: Boolean = true
    runsFile.print("[\n")
    val executor = Executors.newFixedThreadPool(4)  // Some parallelism when writing out DOT files
    for (run <- runs) {
      if (!first) runsFile.print(",\n")
      runsFile.print(run.asJson.toString())
      first = false
      val renderSpacetime = writeGraphviz(SpacetimeDiagramGenerator.generate(run.failureSpec, run.messages),
        outputDirectory, s"run_${run.iteration}_spacetime")
      if (!disableDotRendering) executor.submit(renderSpacetime)
      if (generateProvenanceDiagrams) {
        val renderProv = writeGraphviz(ProvenanceDiagramGenerator.generate(run.provenance),
          outputDirectory, s"run_${run.iteration}_provenance")
        if (!disableDotRendering) executor.submit(renderProv)

      }
    }
    runsFile.print("\n]")
    runsFile.close()
    executor.shutdown()  // Wait for DOT rendering to finish
  }
}
