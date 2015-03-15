package edu.berkeley.cs.boom.molly

import org.scalatest.{ShouldMatchers, FunSuite}
import edu.berkeley.cs.boom.molly.codegen.C4CodeGenerator
import edu.berkeley.cs.boom.molly.DedalusParser._
import edu.berkeley.cs.boom.molly.DedalusTyper._
import scalaz.syntax.id._

class C4CodeGeneratorSuite extends FunSuite with ShouldMatchers {
  test("aggregation") {
    val prog =
      """
        | omission_cnt(Host, Other, count<Id>) :- omission(Host, Other, Id);
        | omission("a", "b", 1);
        | omission("a", "b", 2);
        | omission("a", "b", 3);
      """.stripMargin
    val code = prog |> parseProgram |> inferTypes |> C4CodeGenerator.generate
    code.lines.toSeq should contain ("omission_cnt(Host, Other, count<Id>) :- omission(Host, Other, Id);")
  }

  test("negated predicates should appear at end of rule body") {
    // This requirement is a workaround for https://github.com/bloom-lang/c4/issues/1
    val prog =
      """
        | foo("1");
        | bar("1");
        | baz(X) :- notin bar(X), foo(X);
      """.stripMargin
    val code = prog |> parseProgram |> inferTypes |> C4CodeGenerator.generate
    code.lines.toSeq should contain ("baz(X) :- foo(X), notin bar(X);")
  }
}
