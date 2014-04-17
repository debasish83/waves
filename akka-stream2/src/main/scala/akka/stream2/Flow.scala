package akka.stream2

import scala.language.implicitConversions

import scala.concurrent.{ ExecutionContext, Future }
import org.reactivestreams.api.{ Consumer, Producer }
import akka.actor.ActorRefFactory
import akka.stream2.impl.OperationProcessor

sealed trait Flow[+A] {
  def ~>[B](other: A ==> B): Flow[B]
}

object Flow {
  val Empty: Flow[Nothing] = apply(StreamProducer.empty[Nothing])

  def apply[T](producer: Producer[T]): Flow[T] = Unmapped(producer)
  def apply[T](future: Future[T])(implicit ec: ExecutionContext): Flow[T] = Unmapped(new FutureProducer(future))
  def apply[T](iterable: Iterable[T])(implicit ec: ExecutionContext): Flow[T] = apply(iterable.iterator)
  def apply[T](iterator: Iterator[T])(implicit ec: ExecutionContext): Flow[T] = Unmapped(new IteratorProducer(iterator))

  implicit def fromProducer[T](producer: Producer[T])(implicit ec: ExecutionContext): Flow[T] = apply(producer)
  implicit def fromFuture[T](future: Future[T])(implicit ec: ExecutionContext): Flow[T] = apply(future)
  implicit def fromIterable[T](iterable: Iterable[T])(implicit ec: ExecutionContext): Flow[T] = apply(iterable)

  implicit class Api1[A](val flow: Flow[A]) extends OperationApi1[A] {
    type Res[B] = Flow[B]

    def ~>[B](next: A ==> B): Flow[B] = flow ~> next

    def toProducer(implicit refFactory: ActorRefFactory): Producer[A] =
      flow match {
        case Mapped(producer, op) ⇒
          val processor = new OperationProcessor(op) // TODO: introduce implicit settings allowing for buffer size config
          producer.produceTo(processor)
          processor.asInstanceOf[Producer[A]]
        case Unmapped(producer) ⇒ producer
      }

    def produceTo(consumer: Consumer[A])(implicit refFactory: ActorRefFactory): Unit =
      toProducer.produceTo(consumer)
  }

  implicit class Api2[A](val flow: Flow[Producer[A]]) extends OperationApi2[A] {
    type Res[B] = Flow[B]
    def ~>[B](next: Producer[A] ==> B): Flow[B] = flow ~> next
  }

  /////////////////////// MODEL //////////////////////

  final case class Mapped[A, B](producer: Producer[A], op: Operation[A, B]) extends Flow[B] {
    def ~>[C](op2: B ==> C): Flow[C] = Mapped(producer, op ~> op2)
  }

  final case class Unmapped[A](producer: Producer[A]) extends Flow[A] {
    def ~>[B](op: A ==> B): Flow[B] = Mapped(producer, op)
  }
}