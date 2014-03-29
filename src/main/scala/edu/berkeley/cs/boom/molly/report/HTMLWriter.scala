package edu.berkeley.cs.boom.molly.report

import java.io.File
import edu.berkeley.cs.boom.molly.Run
import edu.berkeley.cs.boom.molly.report.MollyCodecJsons._
import argonaut._, Argonaut._
import org.apache.commons.io.FileUtils
import scala.collection.JavaConversions._
import scala.sys.process._


object HTMLWriter {

  private val templateDir =
    new File(HTMLWriter.getClass.getClassLoader.getResource("vis_template").getPath)

  def copyTemplateFiles(outputDirectory: File) {
    FileUtils.iterateFiles(templateDir, null, false).foreach { file =>
      FileUtils.copyFileToDirectory(file, outputDirectory)
    }
  }

  def write(outputDirectory: File, originalPrograms: List[File], runs: Traversable[Run]) = {
    outputDirectory.mkdirs()
    require (outputDirectory.isDirectory)
    copyTemplateFiles(outputDirectory)
    FileUtils.write(new File(outputDirectory, "runs.json"), runs.toList.asJson.pretty(spaces2))
    for (run <- runs) {
      val dotFile = new File(outputDirectory, s"run_${run.iteration}_spacetime.dot")
      val svgFile = new File(outputDirectory, s"run_${run.iteration}_spacetime.svg")
      FileUtils.write(dotFile, SpacetimeDiagramGenerator.generate(run.failureSpec, run.messages))
      val dotExitCode = s"dot -Tsvg -o ${svgFile.getAbsolutePath} ${dotFile.getAbsolutePath}".!
      assert (dotExitCode == 0)
    }
  }
}
