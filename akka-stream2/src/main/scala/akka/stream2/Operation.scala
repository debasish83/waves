package akka.stream2

import scala.language.implicitConversions
import org.reactivestreams.api.{ Consumer, Processor }
import akka.actor.ActorRefFactory
import akka.stream2.impl.OperationProcessor

sealed trait OperationX // untyped base trait used for dealing with untyped operations

sealed abstract class Operation[-A, +B] extends OperationX {
  import Operation._

  def ~>[C](other: B ==> C): A ==> C =
    (this, other) match {
      case (_: Identity[_], _) ⇒ other.asInstanceOf[A ==> C]
      case (_, _: Identity[_]) ⇒ this.asInstanceOf[A ==> C]
      case _                   ⇒ Operation.~>(this, other)
    }

  def toProcessor[AA <: A, BB >: B](implicit refFactory: ActorRefFactory): Processor[AA, BB] =
    new OperationProcessor(this).asInstanceOf[Processor[AA, BB]] // TODO: introduce implicit settings allowing for buffer size config

  def produceTo[AA <: A, BB >: B](consumer: Consumer[BB])(implicit refFactory: ActorRefFactory): Consumer[AA] = {
    val processor = toProcessor[A, BB]
    processor.produceTo(consumer)
    processor.asInstanceOf[Consumer[AA]]
  }
}

object Operation {

  implicit class Api1[A, B](val op: A ==> B) extends OperationApi1[B] {
    type Res[C] = A ==> C
    def ~>[C](next: B ==> C): Res[C] = op ~> next
  }

  implicit class Api2[A, B](val op: A ==> Source[B]) extends OperationApi2[B] {
    type Res[C] = A ==> C
    def ~>[C](next: Source[B] ==> C): Res[C] = Operation.~>(op, next)
  }

  /////////////////////////// MODEL ////////////////////////////

  final case class ~>[A, B, C](f: A ==> B, g: B ==> C) extends (A ==> C)

  final case class Append[T](source: Source[T]) extends (T ==> T)

  final case class Buffer[A, B, S](seed: S,
                                   compress: (S, A) ⇒ S,
                                   expand: S ⇒ (S, Option[B]),
                                   canConsume: S ⇒ Boolean) extends (A ==> B)

  final case class Drop[T](n: Int) extends (T ==> T)

  final case class Filter[T](p: T ⇒ Boolean) extends (T ==> T)

  final case class Flatten[T]() extends (Source[T] ==> T)

  final case class Fold[A, B](seed: B, f: (B, A) ⇒ B) extends (A ==> B)

  final case class Split[T](f: T ⇒ Split.Command) extends (T ==> Source[T])
  object Split {
    sealed trait Command
    case object Drop extends Command // drop the current element
    case object Append extends Command // append to current sub-stream, if no sub-stream is currently open start a new one
    case object Last extends Command // append element (same as `Append`) and complete the current sub-stream afterwards
    case object First extends Command // complete the current sub-stream (if there is one) and start a new one with the current element
  }

  final case class HeadAndTail[T]() extends (Source[T] ==> (T, Source[T]))

  sealed abstract class Identity[A] extends (A ==> A)
  object Identity extends Identity[Nothing] {
    private[this] final val unapplied = Some(this)
    def apply[T](): Identity[T] = this.asInstanceOf[Identity[T]]
    def unapply[I, O](operation: I ==> O): Option[Identity[I]] =
      if (operation eq this) unapplied.asInstanceOf[Option[Identity[I]]] else None
  }

  final case class Map[A, B](f: A ⇒ B) extends (A ==> B)

  final case class Merge[T](source: Source[T]) extends (T ==> T)

  final case class Multiply[T](factor: Int) extends (T ==> T)

  //FIXME add a "name" String and fix toString so it is easy to delegate to Process and still retain proper toStrings
  final case class Process[A, B, S](seed: S,
                                    onNext: (S, A) ⇒ Process.Command[B, S],
                                    onComplete: S ⇒ Process.Command[B, S]) extends (A ==> B)
  object Process {
    sealed trait Command[+T, +S]
    final case class Emit[T, S](value: T, andThen: Command[T, S]) extends Command[T, S]
    final case class Continue[T, S](nextState: S) extends Command[T, S]
    case object Stop extends Command[Nothing, Nothing]
  }

  final case class Take[T](n: Int) extends (T ==> T)

  final case class Zip[A, B, C](source: Source[C]) extends (A ==> (B, C))
}