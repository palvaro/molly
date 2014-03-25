package edu.berkeley.cs.boom.molly.report

import org.kiama.output.PrettyPrinter
import scala.collection.immutable

/**
 * Mixin that provides helper functions for generating GraphViz .dot markup using
 * Kiama pretty-printing.
 */
trait GraphvizPrettyPrinter extends PrettyPrinter {

  def subgraph(name: Doc, label: Doc, statements: Traversable[Doc]): Doc = {
    "subgraph" <+> name <+> braces(nest(
      linebreak <>
        "label" <> equal <> dquotes(label) <> semi <@@>
        statements.reduce(_ <@@> _)
    ) <> linebreak)
  }

  def node(id: Any, attributes: (String, String)*): Doc = {
    if (attributes.isEmpty) {
      id.toString <> semi
    } else {
      val attrs = attributes.map { case (k, v) => k <> equal <> dquotes(v) }
      id.toString <+> brackets(ssep(immutable.Seq(attrs: _*), comma)) <> semi
    }
  }

  def diEdge(from: Any, to: Any, attributes: (String, String)*): Doc = {
    if (attributes.isEmpty) {
      from.toString <+> "->" <+> to.toString
    } else {
      val attrs = attributes.map { case (k, v) => k <> equal <> dquotes(v) }
      from.toString <+> "->" <+> to.toString <+> brackets(ssep(immutable.Seq(attrs: _*), comma)) <> semi
    }
  }
}
