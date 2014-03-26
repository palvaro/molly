package edu.berkeley.cs.boom.molly

import org.scalatest.{ShouldMatchers, FunSuite}
import edu.berkeley.cs.boom.molly.codegen.C4CodeGenerator


class C4CodeGeneratorSuite extends FunSuite with ShouldMatchers {
  test("aggregation") {
    val prog =
      """
        | omission_cnt(Host, Other, count<Id>) :- omission(Host, Other, Id);
        | omission("a", "b", 1);
        | omission("a", "b", 2);
        | omission("a", "b", 3);
      """.stripMargin
    val program = DedalusTyper.inferTypes(DedalusParser.parseProgram(prog))
    val code = C4CodeGenerator.generate(program)
    code.lines.toSeq should contain ("omission_cnt(Host, Other, count<Id>) :- omission(Host, Other, Id);")
  }
}
