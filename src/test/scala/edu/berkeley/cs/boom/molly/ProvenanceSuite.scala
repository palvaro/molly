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
  test("Aggregate provenance is computed correctly") {
    implicit val metricRegistry = new MetricRegistry()
    val src =
      """
        | fact("loc", "A", 100)@1;
        | fact("loc", "A", 200)@1;
        | fact("loc", "A", 300)@1;
        | fact("loc", "B", 400)@1;
        | derived(X, Y, Z) :- fact(X, Y, Z);
        | counts(L, A, count<B>) :- derived(L, A, B);
      """.stripMargin
    val failureFreeSpec = FailureSpec(1, 0, 0, List("loc"), Set.empty)
    val program =
      src |> parseProgram |> referenceClockRules |> splitAggregateRules |> addProvenanceRules |>  failureFreeSpec.addClockFacts |>  inferTypes
    val model = new C4Wrapper("agg_prov_test", program).run
    model.tables("counts").toSet should be (Set(List("loc", "A", "3", "1"), List("loc", "B", "1", "1")))
    val provenance =  new ProvenanceReader(program, failureFreeSpec, model, negativeSupport = true)
    def aggContributors(goal: GoalTuple): Set[GoalNode] =
      provenance.getDerivationTree(goal).rules.head.subgoals
        .filter(t => t.tuple.table == "counts_vars" && !t.tuple.cols.contains(WILDCARD))
    aggContributors(GoalTuple("counts", List("loc", "A", "3", "1"))).size should be (3)
    aggContributors(GoalTuple("counts", List("loc", "B", "1", "1"))).size should be (1)
  }

  test("Aggregate rewrites don't affect grouping by including un-aggregated variables") {
    // Regression test
    implicit val metricRegistry = new MetricRegistry()
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
    val failureFreeSpec = FailureSpec(2, 0, 0, List("a", "b", "M"), Set.empty)
    val program =
      src |> parseProgram |> referenceClockRules |> splitAggregateRules |> addProvenanceRules |>  failureFreeSpec.addClockFacts |>  inferTypes
    val model = new C4Wrapper("agg_prov_test", program).run
    val goal = List("M", "2", "2")
    model.tables("vote_cnt").toSet should be (Set(goal))
    val provenance =  new ProvenanceReader(program, failureFreeSpec, model, negativeSupport = true)
    def aggContributors(goal: GoalTuple): Set[GoalNode] =
      provenance.getDerivationTree(goal).rules.head.subgoals
        .filter(t => t.tuple.table == "vote_cnt_vars" && !t.tuple.cols.contains(WILDCARD))
    aggContributors(GoalTuple("vote_cnt", goal)).size should be (2)
  }
}
