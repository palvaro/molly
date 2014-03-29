package edu.berkeley.cs.boom.molly.report

import java.io.{FileWriter, File}
import edu.berkeley.cs.boom.molly.Run
import edu.berkeley.cs.boom.molly.report.MollyCodecJsons._
import argonaut._, Argonaut._
import org.apache.commons.io.FileUtils
import scala.collection.JavaConversions._


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
    val fw = new FileWriter(new File(outputDirectory, "runs.json"))
    fw.write(runs.toList.asJson.pretty(spaces2))
    fw.close()
  }
}
