package edu.berkeley.cs.boom.molly

import edu.berkeley.cs.boom.molly.derivations._
import org.scalatest.{FunSpec, Matchers}
import sext._


class FormulaSuite extends FunSpec with Matchers {

    val nuttin = BFLiteral[String](None)
    val foo = BFLiteral[String](Some("foo"))
    val bar = BFLiteral[String](Some("bar"))
    val baz = BFLiteral[String](Some("baz"))
    val qux = BFLiteral[String](Some("qux"))

    def trivialBooleanO(): BFNode[String] = {
      BFOrNode(foo, bar)
    }

    def trivialBooleanA(): BFNode[String] = {
      BFAndNode(baz, qux)
    }

    def nestedA1() : BFNode[String] = {
      BFAndNode(trivialBooleanO(), trivialBooleanO())
    }

    def nestedO1() : BooleanFormula[String] = {
      BooleanFormula(BFOrNode(trivialBooleanA(), trivialBooleanA()))
    }




  def simpleBooleanFormula(): BFNode[String] = {
      val oneWay = BFOrNode(BFAndNode(foo, bar), BFAndNode(baz, qux))
      val otherWay = BFOrNode(BFAndNode(qux, foo), BFAndNode(bar, baz))
      BFAndNode(BFOrNode(otherWay, BFAndNode(oneWay, otherWay)), oneWay)
    }

    def paddedBooleanFormula(n: BFNode[String]): BFNode[String] = {
      BFAndNode(nuttin, BFOrNode(nuttin, BFOrNode(BFAndNode(nuttin, BFAndNode(nuttin, n)), nuttin)))
    }

    describe("simplifying a simplified boolean formula") {
      it("should preserve the formula") {
        simpleBooleanFormula.simplify should be (simpleBooleanFormula)
      }
    }

    describe("simplifying a padded boolean formula") {
      val pad = paddedBooleanFormula(simpleBooleanFormula)
      it("should yield the pure formula") {
        pad.simplify should be (simpleBooleanFormula)
      }

      it(s"should reduce its size (${pad.clauses} vs. ${pad.simplify.clauses})") {
        pad.simplify.clauses should be < (pad.clauses)
      }

      it("should preserve its variables") {
        pad.simplify.vars should be(pad.vars)
      }

      it("should preserve its CNF form") {
        pad.simplify.convertToCNF should be(simpleBooleanFormula.convertToCNF)
      }

      it("should be idempotent") {
        pad.simplify.simplify should be (pad.simplify)
      }
    }

    describe("conversion to CNF") {
      val pad = paddedBooleanFormula(simpleBooleanFormula)
      val trivial = trivialBooleanA()
      it(s"should increase the number of clauses (${nestedO1().convertToCNFAll.root.clauses} vs. ${nestedO1.clauses})") {
        nestedO1().convertToCNFAll.root.clauses should be > (nestedO1.clauses)
        pad.simplify.convertToCNF.clauses should be > (pad.clauses)
      }

      it(s"should be idempotent") {
        //pad.simplify.convertToCNF.convertToCNF should be (pad.simplify.convertToCNF)
        //nestedA1.convertToCNF.convertToCNF.convertToCNF should be (nestedA1.convertToCNF)

        println(s"formula $nestedO1")
        println(s"cnf  ${nestedO1.convertToCNFAll.simplifyAll.treeString}")
        val cnf = nestedO1.convertToCNFAll
        cnf.convertToCNFAll should be (cnf)
        //pad.simplify.convertToCNFAll.root.convertToCNFAll should be (pad.simplify.convertToCNFAll)
      }
    }

}
