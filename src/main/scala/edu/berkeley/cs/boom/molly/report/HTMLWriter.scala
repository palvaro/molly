package edu.berkeley.cs.boom.molly.report

import java.io.File
import edu.berkeley.cs.boom.molly.Run
import edu.berkeley.cs.boom.molly.report.MollyCodecJsons._
import argonaut._, Argonaut._
import org.apache.commons.io.FileUtils
import scala.collection.JavaConversions._
import scala.sys.process._
import org.apache.commons.io.filefilter.{FalseFileFilter, TrueFileFilter}


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

  private def writeGraphviz(dot: String, outputDirectory: File, filename: String) {
    val dotFile = new File(outputDirectory, s"$filename.dot")
    val svgFile = new File(outputDirectory, s"$filename.svg")
    FileUtils.write(dotFile, dot)
    val dotExitCode = s"dot -Tsvg -o ${svgFile.getAbsolutePath} ${dotFile.getAbsolutePath}".!
    assert (dotExitCode == 0)
  }

  def write(outputDirectory: File, originalPrograms: List[File], runs: Traversable[Run]) = {
    outputDirectory.mkdirs()
    require (outputDirectory.isDirectory)
    copyTemplateFiles(outputDirectory)
    FileUtils.write(new File(outputDirectory, "runs.json"), runs.toList.asJson.pretty(spaces2))
    for (run <- runs) {
      writeGraphviz(SpacetimeDiagramGenerator.generate(run.failureSpec, run.messages),
        outputDirectory, s"run_${run.iteration}_spacetime")
      writeGraphviz(ProvenanceDiagramGenerator.generate(run.provenance),
        outputDirectory, s"run_${run.iteration}_provenance")
    }
  }
}
