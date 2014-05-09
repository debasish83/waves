/*
 * Copyright (C) 2014 Mathias Doenitz
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package waves

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
  implicit def forFuture[P, T](implicit ev: Producable[P, T], ec: ExecutionContext) = new Producable[Future[P], T] {
    def apply(future: Future[P]) = StreamProducer(future)
  }
  implicit def forIterable[P, T](implicit ev: P <:< Iterable[T]) = new Producable[P, T] {
    def apply(iterable: P) = StreamProducer(ev(iterable))
  }
  implicit def forOption[T] = new Producable[Option[T], T] {
    def apply(option: Option[T]) = StreamProducer(option)
  }
  implicit def forIterator[T] = new Producable[Iterator[T], T] {
    def apply(iterator: Iterator[T]) = StreamProducer(iterator)
  }
}