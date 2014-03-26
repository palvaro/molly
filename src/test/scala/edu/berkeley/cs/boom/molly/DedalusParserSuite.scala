package edu.berkeley.cs.boom.molly

import org.scalatest.{ShouldMatchers, FunSuite}

class DedalusParserSuite extends FunSuite with ShouldMatchers {
  test("comment not followed by newline") {
    val comment = """//good(C, X) :- begin(C, X)@1"""
    val lines = Seq("""omission("a", "b", 1);""", comment, """omission("a", "b", 2);""", comment)
    val program = DedalusParser.parseProgram(lines.mkString("\n"))
    program.facts.size should be (2)
    program.rules.size should be (0)
  }
}
