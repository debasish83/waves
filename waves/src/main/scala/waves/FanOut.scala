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

import scala.language.higherKinds

import org.reactivestreams.api.Producer
import org.reactivestreams.spi.Subscriber
import scala.concurrent.{ ExecutionContext, Promise }
import waves.impl._

object FanOut {

  /**
   * An unbuffered fanout that never drops elements and only cancels upstream when both downstreams have cancelled.
   */
  object Tee {
    def unapply[T](upstream: Producer[T])(implicit ec: ExecutionContext): Option[(Producer[T], Producer[T])] = {
      val promise = Promise[Producer[T]]()
      val op = Operation.Tee[T](promise.success)
      val processor = new OperationProcessor(op)
      upstream.produceTo(processor)
      val secondaryProducer = new AbstractProducer[T] {
        def subscribe(subscriber: Subscriber[T]) = promise.future.foreach(_.getPublisher.subscribe(subscriber))
      }
      Some(processor.asInstanceOf[Producer[T]] -> secondaryProducer)
    }
  }
}