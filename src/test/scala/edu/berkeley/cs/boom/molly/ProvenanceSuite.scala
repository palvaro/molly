package edu.berkeley.cs.boom.molly

import org.scalatest.{FunSuite, Matchers}
import edu.berkeley.cs.boom.molly.DedalusRewrites._
import edu.berkeley.cs.boom.molly.DedalusParser._
import edu.berkeley.cs.boom.molly.DedalusTyper._
import edu.berkeley.cs.boom.molly.wrappers.C4Wrapper
import edu.berkeley.cs.boom.molly.derivations.{GoalNode, GoalTuple, ProvenanceReader}
import edu.berkeley.cs.boom.molly.derivations.ProvenanceReader._
import scalaz.syntax.id._
import com.codahale.metrics.MetricRegistry


class ProvenanceSuite extends FunSuite with Matchers {

  private def getFailureFreeProv(src: String,
                                 negativeSupport: Boolean = false,
                                 nodes: List[String] = List("loc")): (UltimateModel, ProvenanceReader) = {
    implicit val metricRegistry = new MetricRegistry()
    val failureFreeSpec = FailureSpec(2, 0, 0, nodes, Set.empty)
    val program =
      src |> parseProgram |> referenceClockRules |> splitAggregateRules |> addProvenanceRules |>  failureFreeSpec.addClockFacts |>  inferTypes
    val model = new C4Wrapper("prov_suite", program).run
    val provReader = new ProvenanceReader(program, failureFreeSpec, model, negativeSupport)
    (model, provReader)
  }

  test("Repeat rule firings with different contributing tuples (missing fields are part of join)") {
    // The field that does not appear in the head is part of a join key:
    val src =
      """
        | a("loc", "x", 1)@1;
        | a("loc", "y", 1)@1;
        | b("loc", "x", 1)@1;
        | b("loc", "y", 1)@1;
        | c(L, V) :- b(L, M, V), a(L, M, V);
      """.stripMargin
    val (model, provReader) = getFailureFreeProv(src, negativeSupport = false)
    model.tables("c").toSet should be(Set(List("loc", "1", "1")))
    val goalProv = provReader.getDerivationTree(GoalTuple("c", List("loc", "1", "1")))
    goalProv.rules.size should be (2)
  }

  test("Repeat rule firings with different contributing tuples (missing fields are not joined)") {
    // The field that does not appear in the head is NOT involved in a join:
    val src =
      """
        | a("loc", "x", 1)@1;
        | a("loc", "y", 1)@1;
        | proxy("loc", A, B) :- a(L, A, B);
        | b("loc", 1)@1;
        | c(L, V) :- b(L, V), proxy(L, _, V);
      """.stripMargin
    val (model, provReader) = getFailureFreeProv(src, negativeSupport = false)
    model.tables("c").toSet should be(Set(List("loc", "1", "1")))
    val goalProv = provReader.getDerivationTree(GoalTuple("c", List("loc", "1", "1")))
    println(goalProv)
    goalProv.enumerateDistinctDerivations.size should be (2)
  }

  test("Aggregate provenance is computed correctly") {
    val src =
      """
        | fact("loc", "A", 100)@1;
        | fact("loc", "A", 200)@1;
        | fact("loc", "A", 300)@1;
        | fact("loc", "B", 400)@1;
        | derived(X, Y, Z) :- fact(X, Y, Z);
        | counts(L, A, count<B>) :- derived(L, A, B);
      """.stripMargin
    val (model, provReader) = getFailureFreeProv(src, negativeSupport = true)
    model.tables("counts").toSet should be (Set(List("loc", "A", "3", "1"), List("loc", "B", "1", "1")))
    def aggContributors(goal: GoalTuple): Set[GoalNode] =
      provReader.getDerivationTree(goal).rules.head.subgoals
        .filter(t => t.tuple.table == "counts_vars" && !t.tuple.cols.contains(WILDCARD))
    aggContributors(GoalTuple("counts", List("loc", "A", "3", "1"))).size should be (3)
    aggContributors(GoalTuple("counts", List("loc", "B", "1", "1"))).size should be (1)
  }

  test("Aggregate rewrites don't affect grouping by including un-aggregated variables") {
    // Regression test
    val src =
      """
        | vote(M, V)@next :- vote(M, V);
        | member(M, V, I)@next :- member(M, V, I);
        | vote(M, V)@async :- begin(V, M);
        |
        | vote_cnt(M, count<I>) :- vote(M, V), member(M, V, I);
        | good(M, I) :- vote_cnt(M, I);
        |
        |
        | member("M", "a", 1)@1;
        | member("M", "b", 2)@1;
        | begin("a", "M")@1;
        | begin("b", "M")@1;
      """.stripMargin
    val (model, provReader) =
      getFailureFreeProv(src, negativeSupport = true, nodes = List("a", "b", "M"))
    val goal = List("M", "2", "2")
    model.tables("vote_cnt").toSet should be (Set(goal))
    def aggContributors(goal: GoalTuple): Set[GoalNode] =
      provReader.getDerivationTree(goal).rules.head.subgoals
        .filter(t => t.tuple.table == "vote_cnt_vars" && !t.tuple.cols.contains(WILDCARD))
    aggContributors(GoalTuple("vote_cnt", goal)).size should be (2)
  }
}
