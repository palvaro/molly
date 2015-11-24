package edu.berkeley.cs.boom.molly.derivations


trait BFNode[T] {
  def simplify: BFNode[T]
  def convertToCNF: BFNode[T]
  def vars: Set[T]
  def flipPolarity: BFNode[T]
  def clauses: Int

}

trait BinaryBFNode[T] extends BFNode[T] {
  def left: BFNode[T]
  def right: BFNode[T]
  def construct(l: BFNode[T], r: BFNode[T]): BFNode[T]

  override def clauses: Int = {
    1 + left.clauses + right.clauses
  }

  override def vars = {
    left.vars ++ right.vars
  }

  override def flipPolarity: BFNode[T] = {
    BFOrNode(left.flipPolarity, right.flipPolarity)
  }

  override def simplify: BFNode[T] = {
    (left, right) match {
      case (BFLiteral(None), BFLiteral(None)) => BFLiteral(None)
      case (BFLiteral(None), c) => c.simplify
      case (c, BFLiteral(None)) => c.simplify
      case (c: BFLiteral[T], d:BFLiteral[T]) => {
        if (c == d) {
          c
        } else {
          construct(c, d)
        }
      }

      case _ => construct(left.simplify, right.simplify)
    }
  }
}

//case class BFAndNode[T](left:BFNode[T], right:BFNode[T]) extends BFNode[T] {
case class BFAndNode[T](left:BFNode[T], right:BFNode[T]) extends BinaryBFNode[T] {

  override def construct(l: BFNode[T], r: BFNode[T]): BFNode[T] = {
    BFAndNode(l, r)
  }

  override def convertToCNF(): BFNode[T] = {
    // stay put, but convert your children to CNF.
    construct(left.convertToCNF, right.convertToCNF)
  }

  def findConjuncts: CNFFlat[T] = {
    val newL: Set[Disjuncts[T]] = left match {
      case a: BFAndNode[T] => a.findConjuncts.conjunctz
      case o: BFOrNode[T] => Set(o.findDisjuncts)
      case BFLiteral(Some(l)) => Set(Disjuncts(Set(l)))
    }

    val newR: Set[Disjuncts[T]] = right match {
      case a: BFAndNode[T] => a.findConjuncts.conjunctz
      case o: BFOrNode[T] => Set(o.findDisjuncts)
      case BFLiteral(Some(l)) => Set(Disjuncts(Set(l)))
      //case l: BFLiteral[T] => Set(l)
    }

    CNFFlat(newL ++ newR)
  }
}
case class BFOrNode[T](left:BFNode[T], right:BFNode[T]) extends BinaryBFNode[T] {

  override def construct(l: BFNode[T], r: BFNode[T]): BFNode[T] = {
    BFOrNode(l, r)
  }

  override def convertToCNF(): BFNode[T] = {
    val ret = (left, right) match {
      case (BFAndNode(l, r), _) => {
        val newRight = right.convertToCNF
        BFAndNode(BFOrNode(newRight, l.convertToCNF), BFOrNode(newRight, r.convertToCNF))
      }
      case (_, BFAndNode(l2, r2)) => {
        val newLeft = left.convertToCNF
        BFAndNode(BFOrNode(newLeft, l2.convertToCNF), BFOrNode(newLeft, r2.convertToCNF))
      }
      case (l, r) => BFOrNode(l.convertToCNF, r.convertToCNF)
    }
    ret
  }

  def findDisjuncts: Disjuncts[T] = {
    Disjuncts(vars)
  }
}

case class BFLiteral[T](v:Option[T]) extends BFNode[T] {
  override def simplify: BFNode[T] = {
    BFLiteral(v)
  }
  override def convertToCNF: BFNode[T] = {
    BFLiteral(v)
  }
  override def vars = {
    v match {
      case Some(c) => Set(c)
      case None => Set()
    }
  }

  override def flipPolarity = {
    BFLiteral(v)
  }

  override def clauses: Int = {
    0
  }
}



/* Formula wrapper classes */

trait AbstractBooleanFormula[T] {
  def root: BFNode[T]
  def construct(node: BFNode[T]): AbstractBooleanFormula[T]

  def simplifyAll: AbstractBooleanFormula[T] = {
    // this can't be idiomatic
    var last: BFNode[T] = null
    var current = root
    while (last != current) {
      last = current
      current = current.simplify
    }
    construct(current)
  }

  def convertToCNFAll: CNFFormula[T] = {
    var last: BFNode[T] = null
    var current = root
    var iterations = 0
    while (last != current) {
      last = current
      println(s"starting iteration $iterations.  ${current.clauses}")
      current = current.convertToCNF
      println(s"Finished iteration $iterations ${current.vars}")
      iterations = iterations + 1
    }
    CNFFormula(current)
  }

  def flipPolarity(): AbstractBooleanFormula[T] = {
    construct(root.flipPolarity)
  }

  def clauses(): Int = {
    root.clauses
  }

  def vars(): Set[T] = {
    root.vars
  }
}

case class BooleanFormula[T](root: BFNode[T]) extends AbstractBooleanFormula[T] {
  def construct(node: BFNode[T]): AbstractBooleanFormula[T] = {
    BooleanFormula(node)
  }
}

case class Disjuncts[T](disjuncts: Set[T])
case class CNFFlat[T](conjunctz: Set[Disjuncts[T]])

case class CNFFormula[T](root: BFNode[T]) extends AbstractBooleanFormula[T] {
  def construct(node: BFNode[T]) = {
    CNFFormula(node)
  }

  def conjuncts: CNFFlat[T] = {
    root match {
      case a: BFAndNode[T] => a.findConjuncts
      case o: BFOrNode[T] => CNFFlat(Set(o.findDisjuncts))
      case BFLiteral(Some(l)) => CNFFlat(Set(Disjuncts(Set(l))))
      case _ => println(s"WTF? root $root"); CNFFlat(Set(Disjuncts(Set())))
    }
  }
}



