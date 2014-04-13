package edu.berkeley.cs.boom.molly.util


/**
 * Mixin trait to cache an object's hashCode.  This is only safe to apply to immutable objects.
 *
 * Based on code from a scala-user discussion:
 * https://groups.google.com/d/msg/scala-user/drkTziXMUyE/-RWJV-fC1cYJ
 */
trait HashcodeCaching { self: Product =>
  override lazy val hashCode: Int = scala.runtime.ScalaRunTime._hashCode(this)
}
