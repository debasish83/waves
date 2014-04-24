package akka.stream2

import scala.language.{ higherKinds, implicitConversions }
import org.reactivestreams.api.{ Producer, Consumer, Processor }
import akka.actor.ActorRefFactory
import akka.stream2.impl.OperationProcessor
import scala.collection.immutable

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

  final case class Concat[T](next: () ⇒ Producer[T]) extends (T ==> T)

  final case class ConcatAll[T]() extends (Producer[T] ==> T)

  final case class Drop[T](n: Int) extends (T ==> T)

  final case class FanInBox[A, B, F[_, _] <: FanIn[_, _]](secondary: Producer[B], fanIn: FanIn.Provider[F])
    extends (A ==> F[A, B]#O)

  final case class FanOutBox[I, F[_] <: FanOut[_]](fanOut: FanOut.Provider[F], secondary: Producer[F[I]#O2] ⇒ Unit)
    extends (I ==> F[I]#O1)

  final case class Filter[T](p: T ⇒ Boolean) extends (T ==> T)

  final case class Fold[A, B](seed: B, f: (B, A) ⇒ B) extends (A ==> B)

  final case class Head[T]() extends (Producer[T] ==> T)

  sealed abstract class Identity[A] extends (A ==> A)
  object Identity extends Identity[Nothing] {
    private[this] final val unapplied = Some(this)
    def apply[T](): Identity[T] = this.asInstanceOf[Identity[T]]
    def unapply[I, O](operation: I ==> O): Option[Identity[I]] =
      if (operation eq this) unapplied.asInstanceOf[Option[Identity[I]]] else None
  }

  final case class Map[A, B](f: A ⇒ B) extends (A ==> B)

  final case class Multiply[T](factor: Int) extends (T ==> T)

  final case class OnTerminate[T](callback: Option[Throwable] ⇒ Any) extends (T ==> T)

  final case class OuterMap[A, B](f: Producer[A] ⇒ Producer[B]) extends (A ==> B)

  final case class Recover[A, B <: A](f: Throwable ⇒ immutable.Seq[B]) extends (A ==> B)

  final case class Split[T](f: T ⇒ Split.Command) extends (T ==> Producer[T])
  object Split {
    sealed trait Command
    case object Drop extends Command // drop the current element
    case object Append extends Command // append to current sub-stream, if no sub-stream is currently open start a new one
    case object Last extends Command // append element (same as `Append`) and complete the current sub-stream afterwards
    case object First extends Command // complete the current sub-stream (if there is one) and start a new one with the current element
  }

  final case class Take[T](n: Int) extends (T ==> T)

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
}