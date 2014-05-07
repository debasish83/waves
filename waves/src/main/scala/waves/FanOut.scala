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

trait FanOut[T] {
  type O1 // primary output type
  type O2 // secondary output type
  def primaryRequestMore(elements: Int): Unit
  def primaryCancel(): Unit
  def secondaryRequestMore(elements: Int): Unit
  def secondaryCancel(): Unit

  def onNext(element: T): Unit
  def onComplete(): Unit
  def onError(cause: Throwable): Unit
}

object FanOut {
  abstract class Provider[F[_] <: FanOut[_]] {
    def apply(upstream: Upstream, primaryDownstream: Downstream, secondaryDownstream: Downstream): F[Any]

    def unapply[I](upstream: Producer[I])(implicit ec: ExecutionContext): Option[(Producer[F[I]#O1], Producer[F[I]#O2])] = {
      val promise = Promise[Producer[F[I]#O2]]()
      val op = Operation.FanOutBox[I, F](this, promise.success)
      val processor = new OperationProcessor(op)
      upstream.produceTo(processor)
      val secondaryProducer = new AbstractProducer[F[I]#O2] {
        def subscribe(subscriber: Subscriber[F[I]#O2]) = promise.future.foreach(_.getPublisher.subscribe(subscriber))
      }
      Some(processor.asInstanceOf[Producer[F[I]#O1]] -> secondaryProducer)
    }
  }

  /**
   * An unbuffered fanout that never drops elements and only cancels upstream when both downstreams have cancelled.
   */
  object Tee extends Provider[Tee] {
    def apply(upstream: Upstream, primaryDownstream: Downstream, secondaryDownstream: Downstream): Tee[Any] =
      new Tee(upstream, primaryDownstream, secondaryDownstream)
  }

  class Tee[T](upstream: Upstream, primaryDownstream: Downstream, secondaryDownstream: Downstream) extends FanOut[T] {
    type O2 = T
    type O1 = T

    var requested1 = 0
    var requested2 = 0
    var cancelled1 = false
    var cancelled2 = false

    def onNext(element: T): Unit = {
      if (!cancelled1) primaryDownstream.onNext(element)
      if (!cancelled2) secondaryDownstream.onNext(element)
    }

    def onComplete(): Unit = {
      if (!cancelled1) primaryDownstream.onComplete()
      if (!cancelled2) secondaryDownstream.onComplete()
    }

    def onError(cause: Throwable): Unit = {
      if (!cancelled1) primaryDownstream.onError(cause)
      if (!cancelled2) secondaryDownstream.onError(cause)
    }

    override def primaryRequestMore(elements: Int): Unit = {
      requested1 += elements
      requestMoreIfPossible()
    }

    override def primaryCancel(): Unit = {
      cancelled1 = true
      if (cancelled2) upstream.cancel()
      else requestMoreIfPossible()
    }

    def secondaryRequestMore(elements: Int): Unit = {
      requested2 += elements
      requestMoreIfPossible()
    }

    def secondaryCancel(): Unit = {
      cancelled2 = true
      if (cancelled1) upstream.cancel()
      else requestMoreIfPossible()
    }

    private def requestMoreIfPossible(): Unit =
      if (cancelled1) {
        if (requested2 > 0) {
          val r = requested2
          requested2 = 0
          upstream.requestMore(r)
        }
      } else if (cancelled2) {
        if (requested1 > 0) {
          val r = requested1
          requested1 = 0
          upstream.requestMore(r)
        }
      } else {
        math.min(requested1, requested2) match {
          case 0 ⇒ // nothing to do
          case r ⇒
            requested1 -= r
            requested2 -= r
            upstream.requestMore(r)
        }
      }
  }
}