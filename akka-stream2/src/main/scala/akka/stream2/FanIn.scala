package akka.stream2

import scala.language.higherKinds

import akka.stream2.impl._

trait FanIn[I1, I2, O] {
  def requestMore(elements: Int): Unit
  def cancel(): Unit

  def primaryOnNext(element: I1): Unit
  def primaryOnComplete(): Unit
  def primaryOnError(cause: Throwable): Unit
  def secondaryOnNext(element: I2): Unit
  def secondaryOnComplete(): Unit
  def secondaryOnError(cause: Throwable): Unit
}

object FanIn {
  trait Provider[I1, I2, O] {
    def apply(primaryUpstream: Upstream, secondaryUpstream: Upstream, downstream: Downstream): FanIn[I1, I2, O]
  }

  object Merge extends Provider[Any, Any, Any] {
    def apply[A, AA >: A](): Provider[A, AA, AA] = this.asInstanceOf[Provider[A, AA, AA]]
    def apply(primaryUpstream: Upstream, secondaryUpstream: Upstream, downstream: Downstream): Merge[Any] =
      new Merge(primaryUpstream, secondaryUpstream, downstream)
  }

  class Merge[T](primaryUpstream: Upstream, secondaryUpstream: Upstream, downstream: Downstream) extends FanIn[T, T, T] {
    def requestMore(elements: Int): Unit = ???
    def cancel(): Unit = ???

    def primaryOnNext(element: T): Unit = ???
    def primaryOnComplete(): Unit = ???
    def primaryOnError(cause: Throwable): Unit = ???

    def secondaryOnNext(element: T): Unit = ???
    def secondaryOnComplete(): Unit = ???
    def secondaryOnError(cause: Throwable): Unit = ???
  }

  object Zip extends Provider[Any, Any, (Any, Any)] {
    def apply[A, B](): Provider[A, B, (A, B)] = this.asInstanceOf[Provider[A, B, (A, B)]]
    def apply(primaryUpstream: Upstream, secondaryUpstream: Upstream, downstream: Downstream): Zip[Any, Any] =
      new Zip(primaryUpstream, secondaryUpstream, downstream)
  }

  class Zip[A, B](primaryUpstream: Upstream, secondaryUpstream: Upstream, downstream: Downstream) extends FanIn[A, B, (A, B)] {
    def requestMore(elements: Int): Unit = ???
    def cancel(): Unit = ???

    def primaryOnNext(element: A): Unit = ???
    def primaryOnComplete(): Unit = ???
    def primaryOnError(cause: Throwable): Unit = ???

    def secondaryOnNext(element: B): Unit = ???
    def secondaryOnComplete(): Unit = ???
    def secondaryOnError(cause: Throwable): Unit = ???
  }
}