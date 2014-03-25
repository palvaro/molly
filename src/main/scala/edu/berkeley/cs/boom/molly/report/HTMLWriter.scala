package edu.berkeley.cs.boom.molly.report

import java.io.{FileWriter, File}
import edu.berkeley.cs.boom.molly.Run
import edu.berkeley.cs.boom.molly.report.MollyCodecJsons._
import argonaut._, Argonaut._


object HTMLWriter {
  def write(
    outputDirectory: File,
    originalPrograms: List[File],
    runs: Traversable[Run]
  ) = {
    outputDirectory.mkdirs()
    require (outputDirectory.isDirectory)
    val fw = new FileWriter(new File(outputDirectory, "runs.json"))
    fw.write(runs.toList.asJson.pretty(spaces2))
    fw.close()
  }
}
