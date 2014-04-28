/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.stream2

import org.reactivestreams.api.Producer
import scala.concurrent.{ ExecutionContext, Future }

/**
 * Type-class witnessing that an instance of type P can be implicitly converted to a Producer[T].
 */
trait Producable[P, T] extends (P ⇒ Producer[T])

object Producable {
  def unapply[A, B](element: A)(implicit ev: Producable[A, B]): Option[Producer[B]] = Some(ev(element))

  // TODO: where possible: reduce allocation by casting a static instance

  implicit def forProducer[T] = new Producable[Producer[T], T] {
    def apply(producer: Producer[T]) = producer
  }
  implicit def forFuture[T](implicit executor: ExecutionContext) = new Producable[Future[T], T] {
    def apply(future: Future[T]) = StreamProducer(future)
  }
  implicit def forIterable[T] = new Producable[Iterable[T], T] {
    def apply(iterable: Iterable[T]) = StreamProducer(iterable)
  }
  implicit def forOption[T] = new Producable[Option[T], T] {
    def apply(option: Option[T]) = StreamProducer(option)
  }
  implicit def forIterator[T] = new Producable[Iterator[T], T] {
    def apply(iterator: Iterator[T]) = StreamProducer(iterator)
  }
}