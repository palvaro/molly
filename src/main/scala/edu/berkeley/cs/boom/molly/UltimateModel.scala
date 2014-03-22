package edu.berkeley.cs.boom.molly


class UltimateModel(val tables: Map[String, Array[Array[String]]]) {
  override def toString: String = {
    tables.map { case (name, values) =>
      name + ":\n" + values.map(_.mkString(",")).mkString("\n")
    }.mkString("\n\n")
  }
}
