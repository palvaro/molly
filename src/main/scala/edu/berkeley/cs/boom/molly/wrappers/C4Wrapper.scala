package edu.berkeley.cs.boom.molly.wrappers

import edu.berkeley.cs.boom.molly.UltimateModel
import edu.berkeley.cs.boom.molly.ast.Program
import jnr.ffi.LibraryLoader
import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.cs.boom.molly.codegen.C4CodeGenerator


class C4Wrapper(name: String, program: Program) extends Logging {

  def run: UltimateModel = {
    C4Wrapper.libC4.c4_initialize()
    val c4 = C4Wrapper.libC4.c4_make(null, 0)
    try {
      val input = C4CodeGenerator.generate(program)
      logger.debug("C4 input is:\n" + input)
      assert(C4Wrapper.libC4.c4_install_str(c4, input) == 0)
      val tables = program.tables.map {
        t => (t.name, parseTableDump(C4Wrapper.libC4.c4_dump_table(c4, t.name)))
      }.toMap
      new UltimateModel(tables)
    } finally {
      C4Wrapper.libC4.c4_destroy(c4)
      C4Wrapper.libC4.c4_terminate()
    }
  }

  def parseTableDump(string: String): List[List[String]] = {
    string.lines.map(_.split(",").toList).toList
  }
}

object C4Wrapper {
  val libC4: C4 = LibraryLoader.create(classOf[C4]).load("c4")
}