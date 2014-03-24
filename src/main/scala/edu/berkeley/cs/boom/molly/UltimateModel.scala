package edu.berkeley.cs.boom.molly


case class UltimateModel(val tables: Map[String, List[List[String]]]) {
  override def toString: String = {
    tables.map { case (name, values) =>
      name + ":\n" + values.map(_.mkString(",")).mkString("\n")
    }.mkString("\n\n")
  }
}
