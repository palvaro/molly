package edu.berkeley.cs.boom.molly

import org.scalatest.{FunSuite, Matchers}
import edu.berkeley.cs.boom.molly.DedalusRewrites._
import edu.berkeley.cs.boom.molly.DedalusParser._
import edu.berkeley.cs.boom.molly.DedalusTyper._
import edu.berkeley.cs.boom.molly.wrappers.C4Wrapper
import edu.berkeley.cs.boom.molly.derivations.{GoalNode, GoalTuple, ProvenanceReader}
import edu.berkeley.cs.boom.molly.derivations.ProvenanceReader._
import scalaz._, Scalaz._
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
      (parseProgram _ >>> referenceClockRules >>> addProvenanceRules >>> failureFreeSpec.addClockFacts >>> inferTypes)(src)
    val model = new C4Wrapper("agg_prov_test", program).run
    model.tables("counts").toSet should be (Set(List("loc", "A", "3", "1"), List("loc", "B", "1", "1")))
    val provenance =  new ProvenanceReader(program, failureFreeSpec, model)
    def aggregateSupport(goal: GoalTuple): Set[GoalNode] =
      provenance.getDerivationTree(goal).rules.head.subgoals
        .filter(t => t.tuple.table == "derived" && !t.tuple.cols.contains(WILDCARD))
    aggregateSupport(GoalTuple("counts", List("loc", "A", "3", "1"))).size should be (3)
    aggregateSupport(GoalTuple("counts", List("loc", "B", "1", "1"))).size should be (1)
  }
}
