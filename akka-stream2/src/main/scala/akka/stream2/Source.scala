package akka.stream2

import scala.language.implicitConversions

import scala.concurrent.{ ExecutionContext, Future }
import org.reactivestreams.api.{ Consumer, Producer }
import akka.actor.ActorRefFactory
import akka.stream2.impl.OperationProcessor

sealed trait Source[+A] {
  def ~>[B](other: A ==> B): Source[B]
  def toProducer[AA >: A](implicit refFactory: ActorRefFactory): Producer[AA]
  def produceTo[AA >: A](consumer: Consumer[AA])(implicit refFactory: ActorRefFactory): Unit =
    toProducer[AA].produceTo(consumer)
}

object Source {
  val Empty: Source[Nothing] = apply(StreamProducer.empty[Nothing])

  def apply[T](producer: Producer[T]): Source[T] = Unmapped(producer)
  def apply[T](future: Future[T])(implicit ec: ExecutionContext): Source[T] = Unmapped(new FutureProducer(future))
  def apply[T](iterable: Iterable[T])(implicit ec: ExecutionContext): Source[T] = apply(iterable.iterator)
  def apply[T](iterator: Iterator[T])(implicit ec: ExecutionContext): Source[T] = Unmapped(new IteratorProducer(iterator))

  implicit def fromProducer[T](producer: Producer[T])(implicit ec: ExecutionContext): Source[T] = apply(producer)
  implicit def fromFuture[T](future: Future[T])(implicit ec: ExecutionContext): Source[T] = apply(future)
  implicit def fromIterable[T](iterable: Iterable[T])(implicit ec: ExecutionContext): Source[T] = apply(iterable)

  implicit class Api1[A](val source: Source[A]) extends OperationApi1[A] {
    type Res[B] = Source[B]
    def ~>[B](next: A ==> B): Source[B] = source ~> next
  }

  implicit class Api2[A](val source: Source[Source[A]]) extends OperationApi2[A] {
    type Res[B] = Source[B]
    def ~>[B](next: Source[A] ==> B): Source[B] = source ~> next
  }

  /////////////////////// MODEL //////////////////////

  final case class Mapped[A, B](producer: Producer[A], op: Operation[A, B]) extends Source[B] {
    def ~>[C](op2: B ==> C): Source[C] = Mapped(producer, op ~> op2)
    def toProducer[BB >: B](implicit refFactory: ActorRefFactory): Producer[BB] = {
      val processor = new OperationProcessor(op) // TODO: introduce implicit settings allowing for buffer size config
      producer.getPublisher.subscribe(processor.getSubscriber)
      processor.asInstanceOf[Producer[BB]]
    }
  }

  final case class Unmapped[A](producer: Producer[A]) extends Source[A] {
    def ~>[B](op: A ==> B): Source[B] = Mapped(producer, op)
    def toProducer[AA >: A](implicit refFactory: ActorRefFactory): Producer[AA] = producer.asInstanceOf[Producer[AA]]
  }
}