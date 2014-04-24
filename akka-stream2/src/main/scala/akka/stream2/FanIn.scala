package akka.stream2

import scala.language.higherKinds

import akka.stream2.impl._

trait FanIn[I1, I2] {
  type O // output type

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
  abstract class Provider[F[_, _] <: FanIn[_, _]] {
    def apply(primaryUpstream: Upstream, secondaryUpstream: Upstream, downstream: Downstream): F[Any, Any]
  }

  object Merge extends Provider[Merge] {
    def apply(primaryUpstream: Upstream, secondaryUpstream: Upstream, downstream: Downstream): Merge[Any, Any] =
      new Merge(primaryUpstream, secondaryUpstream, downstream)
  }

  class Merge[A, B](primaryUpstream: Upstream, secondaryUpstream: Upstream, downstream: Downstream) extends FanIn[A, B] {
    type O = B // we cannot apply a B >: A type bound on this level, but we can (and do) on the API surface

    def requestMore(elements: Int): Unit = ???
    def cancel(): Unit = ???

    def primaryOnNext(element: A): Unit = ???
    def primaryOnComplete(): Unit = ???
    def primaryOnError(cause: Throwable): Unit = ???

    def secondaryOnNext(element: B): Unit = ???
    def secondaryOnComplete(): Unit = ???
    def secondaryOnError(cause: Throwable): Unit = ???
  }

  object Zip extends Provider[Zip] {
    def apply(primaryUpstream: Upstream, secondaryUpstream: Upstream, downstream: Downstream): Zip[Any, Any] =
      new Zip(primaryUpstream, secondaryUpstream, downstream)
  }

  class Zip[A, B](primaryUpstream: Upstream, secondaryUpstream: Upstream, downstream: Downstream) extends FanIn[A, B] {
    type O = (A, B)

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