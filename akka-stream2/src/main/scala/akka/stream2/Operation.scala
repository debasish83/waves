package akka.stream2

import scala.language.implicitConversions
import org.reactivestreams.api.{ Producer, Consumer, Processor }
import akka.actor.ActorRefFactory
import akka.stream2.impl.OperationProcessor
import scala.collection.immutable
import org.reactivestreams.spi.Subscription

sealed trait OperationX // untyped base trait used for dealing with untyped operations

sealed abstract class Operation[-A, +B] extends OperationX {
  import Operation._

  def ~>[C](other: B ==> C): A ==> C =
    (this, other) match {
      case (_: Identity[_], _) ⇒ other.asInstanceOf[A ==> C]
      case (_, _: Identity[_]) ⇒ this.asInstanceOf[A ==> C]
      case _                   ⇒ Operation.~>(this, other)
    }
}

object Operation {

  def apply[A]: Identity[A] = Identity[A]()

  implicit class Api1[A, B](val op: A ==> B) extends OperationApi1[B] {
    type Res[C] = A ==> C
    def ~>[C](next: B ==> C): Res[C] = op ~> next
    def toProcessor(implicit refFactory: ActorRefFactory): Processor[A, B] =
      new OperationProcessor(op) // TODO: introduce implicit settings allowing for buffer size config
    def produceTo(consumer: Consumer[B])(implicit refFactory: ActorRefFactory): Consumer[A] = {
      val processor = toProcessor
      processor.produceTo(consumer)
      processor
    }
  }

  implicit class Api2[A, B](val op: A ==> Producer[B]) extends OperationApi2[B] {
    type Res[C] = A ==> C
    def ~>[C](next: Producer[B] ==> C): Res[C] = Operation.~>(op, next)
  }

  /////////////////////////// MODEL ////////////////////////////

  final case class ~>[A, B, C](f: A ==> B, g: B ==> C) extends (A ==> C)

  final case class Buffer[A, B, S](seed: S,
                                   compress: (S, A) ⇒ S,
                                   expand: S ⇒ (S, Option[B]),
                                   canConsume: S ⇒ Boolean) extends (A ==> B)

  final case class Concat[T](producer: Producer[T]) extends (T ==> T)

  final case class Drop[T](n: Int) extends (T ==> T)

  final case class FanInBox[A, B, C](secondary: Producer[B], fanIn: FanIn[A, B, C]) extends (A ==> C)

  /**
   * The driving logic is this:
   * 1. When elements are requested from downstream `next` is called and its result dispatched to downstream.
   * 2. If no more elements are available `primaryDemand` and `secondaryDemand` (if not yet cancelled) are called and,
   *    when positive, demand is signalled to the respective upstream.
   * 3. If `primaryDemand` or `secondaryDemand` returns a negative value the respective upstream is cancelled.
   * 4. If both upstreams have been cancelled the downstream is completed and `cleanup` called.
   * 5. When an element comes in from an upstream `primaryOnNext` or `secondaryOnNext` is called, then `next` is
   *    called and the result dispatched to downstream.
   * 6. When an upstream is completed `primaryOnComplete` or `secondaryOnComplete` is called, then `next` is
   *    called and the result dispatched to downstream.
   * 7. When the last upstream has been completed the downstream is completed and `cleanup` called.
   * 8. All errors (from upstream or exceptions thrown by any method) are immediately dispatched to downstream,
   *    the remaining upstreams are cancelled (if not yet cancelled or completed) and `cleanup` is called.
   */
  trait FanIn[-A, -B, +C] {
    def next(): immutable.Seq[C]
    def primaryDemand: Int // positive: requestMore, negative: cancel, zero: no action
    def secondaryDemand: Int // positive: requestMore, negative: cancel, zero: no action
    def primaryOnNext(elem: A): Unit
    def secondaryOnNext(elem: B): Unit
    def primaryOnComplete(): Unit
    def secondaryOnComplete(): Unit
    def cleanup(): Unit = ()
  }

  final case class FanOutBox[A, B, C](fanOut: FanOut[A, B, C], secondary: Producer[C] ⇒ Unit) extends (A ==> B)

  /**
   * The driving logic is this:
   * 1. When elements are requested from the primary or secondary downstream the respective `nextPrimary` or
   *    `nextSecondary` method called and all returned elements dispatched to the respective downstream.
   * 2. If no more elements are available for one of the two downstreams `primaryIsComplete` or `secondaryIsComplete`
   *    is called and, if true, the respective downstream completed.
   * 3. If both downstreams are completed the upstream is cancelled and `cleanup` called, otherwise `canRequestMore`
   *    is called and, if true, demand is signaled to upstream.
   * 4. When an element comes in from upstream `onNext` is called followed by, if there is still unfulfilled
   *    demand for the respective downstream, steps 1 through 3.
   * 5. When the upstream is completed `onComplete` is called, followed by one last call to `nextPrimary` and
   *    `nextSecondary` (if the respective downstream is still uncancelled). After all remaining elements have
   *    been dispatched the still-open downstreams are completed and `cleanup` is called.
   * 6. All errors (from upstream or exceptions thrown by any method) are immediately dispatched to downstream,
   *    the upstream is cancelled (if the error is an exception thrown by a method) and `cleanup` is called.
   */
  trait FanOut[-A, +B, +C] {
    def nextPrimary(): immutable.Seq[B]
    def nextSecondary(): immutable.Seq[C]
    def primaryIsComplete: Boolean = false
    def secondaryIsComplete: Boolean = false
    def canRequestMore: Boolean
    def onNext(elem: A): Unit
    def onComplete(): Unit
    def cleanup(): Unit = ()
  }

  final case class Filter[T](p: T ⇒ Boolean) extends (T ==> T)

  final case class Flatten[T]() extends (Producer[T] ==> T)

  final case class Fold[A, B](seed: B, f: (B, A) ⇒ B) extends (A ==> B)

  final case class HeadAndTail[T]() extends (Producer[T] ==> (T, Producer[T]))

  sealed abstract class Identity[A] extends (A ==> A)
  object Identity extends Identity[Nothing] {
    private[this] final val unapplied = Some(this)
    def apply[T](): Identity[T] = this.asInstanceOf[Identity[T]]
    def unapply[I, O](operation: I ==> O): Option[Identity[I]] =
      if (operation eq this) unapplied.asInstanceOf[Option[Identity[I]]] else None
  }

  final case class Map[A, B](f: A ⇒ B) extends (A ==> B)

  final case class Merge[T](producer: Producer[T]) extends (T ==> T)

  final case class Multiply[T](factor: Int) extends (T ==> T)

  final case class OnTerminate[T](callback: Option[Throwable] ⇒ Any) extends (T ==> T)

  final case class Split[T](f: T ⇒ Split.Command) extends (T ==> Producer[T])
  object Split {
    sealed trait Command
    case object Drop extends Command // drop the current element
    case object Append extends Command // append to current sub-stream, if no sub-stream is currently open start a new one
    case object Last extends Command // append element (same as `Append`) and complete the current sub-stream afterwards
    case object First extends Command // complete the current sub-stream (if there is one) and start a new one with the current element
  }

  final case class Transform[A, B](transformer: Transformer[A, B]) extends (A ==> B)

  /**
   * The driving logic is this:
   * 1. Demand from downstream is directly propagated to upstream.
   * 2. When an element comes in from upstream `onNext` is called and its results dispatched to downstream.
   * 3. When all elements have been dispatched and the upstream is still uncompleted `isComplete` is being called.
   * 4. If `isComplete` is true the downstream is completed, the upstream is cancelled and `cleanup` called.
   * 5. When the upstream is completed `onComplete` is called and its result dispatched to downstream before
   *    the downstream is completed and `cleanup` called.
   * 6. All errors (from upstream or exceptions thrown by any method) are immediately dispatched to downstream,
   *    the upstream is cancelled (if the error is an exception thrown by a method) and `cleanup` is called.
   */
  trait Transformer[-A, +B] {
    def onNext(elem: A): immutable.Seq[B]
    def isComplete: Boolean = false
    def onComplete: immutable.Seq[B] = Nil
    def cleanup(): Unit = ()
  }

  final case class Take[T](n: Int) extends (T ==> T)

  final case class Tee[T](f: Producer[T] ⇒ Unit) extends (T ==> T)

  final case class Zip[A, B, C](producer: Producer[C]) extends (A ==> (B, C))
}