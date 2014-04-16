package akka.stream2

import scala.language.implicitConversions

import scala.concurrent.ExecutionContext
import org.reactivestreams.api.Consumer

sealed trait Sink[-T]

object Sink {
  def apply[T](f: T ⇒ Unit)(implicit executor: ExecutionContext): Unmapped[T] = apply(new FunctionSinkSubscriber(f))
  def apply[T](consumer: Consumer[T]): Unmapped[T] = Unmapped(consumer)

  implicit def fromForeach[T](f: T ⇒ Unit)(implicit executor: ExecutionContext): Sink[T] = apply(f)
  implicit def fromConsumer[T](consumer: Consumer[T]): Sink[T] = apply(consumer)

  /////////////////////// MODEL //////////////////////

  final case class Mapped[A, B](op: Operation[A, B], consumer: Consumer[B]) extends Sink[A]
  final case class Unmapped[T](consumer: Consumer[T]) extends Sink[T]
}
