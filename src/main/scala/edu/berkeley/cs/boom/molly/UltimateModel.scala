package edu.berkeley.cs.boom.molly

// TODO: this class is misleadingly named, since it holds the contents of ALL tables
// at ALL timesteps.
case class UltimateModel(tables: Map[String, List[List[String]]]) {
  override def toString: String = {
    tables.map { case (name, values) =>
      name + ":\n" + values.map(_.mkString(",")).mkString("\n")
    }.mkString("\n\n")
  }

  def tableAtTime(table: String, time: Int) = tables(table).filter(_.last.toInt == time)
}
