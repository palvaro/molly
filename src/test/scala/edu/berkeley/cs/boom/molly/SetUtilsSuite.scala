package edu.berkeley.cs.boom.molly

import org.scalatest.{Matchers, FunSuite}

import edu.berkeley.cs.boom.molly.util.SetUtils._

class SetUtilsSuite extends FunSuite with Matchers {
  test("isSuperset") {
    isSuperset(Set.empty, Set.empty) should be (true)
    isSuperset(Set(1, 2, 3), Set(1)) should be (true)
    isSuperset(Set(1, 2), Set(1, 2, 3)) should be (false)
    isSuperset(Set(9, 10, 11), Set(0)) should be (false)
  }

  test("minimalSets") {
    minimalSets(Seq.empty) should be (empty)
    minimalSets(Seq(Set(1), Set(1))).toSet should be (Set(Set(1)))
    minimalSets(Seq(Set(1), Set(2, 3))).toSet should be (Set(Set(1), Set(2, 3)))
    minimalSets(Seq(Set(1), Set(1, 2), Set(1, 2, 3))).toSet should be (Set(Set(1)))
    minimalSets(Seq(Set(1, 2), Set(2, 3), Set(1, 3))).toSet should be (
      Set(Set(1, 2), Set(2, 3), Set(1, 3))
    )
  }
}
