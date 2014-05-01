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

import org.reactivestreams.api.Consumer
import org.reactivestreams.spi.{ Subscription, Subscriber }
import scala.concurrent.{ Promise, ExecutionContext }
import java.util.concurrent.atomic.{ AtomicInteger, AtomicReference }

object StreamConsumer {

  /**
   * A consumer that simply drains any stream it is subscribed to.
   */
  def blackHole[T](implicit ec: ExecutionContext): Consumer[T] =
    new SimpleConsumer[T] {
      private[this] val requested = new AtomicInteger(Int.MaxValue)
      def onSubscribe() = requestIntMaxValue()
      def onNext(element: T): Unit =
        if (requested.decrementAndGet() == 0) {
          requested.set(Int.MaxValue)
          requestIntMaxValue()
        }
      private def requestIntMaxValue() = schedule { subscription.get.requestMore(Int.MaxValue) }
    }

  /**
   * A consumer that competes the given promise with either the first stream element or the stream error.
   */
  def headFuture[T](promise: Promise[T])(implicit ec: ExecutionContext): Consumer[T] =
    new SimpleConsumer[T] {
      def onSubscribe() = schedule { subscription.get.requestMore(1) }
      def onNext(element: T): Unit =
        if (!promise.isCompleted) {
          subscription.get.cancel()
          promise.success(element)
        }
      override def onComplete(): Unit = if (!promise.isCompleted) onError(new NoSuchElementException)
      override def onError(cause: Throwable): Unit = if (!promise.isCompleted) promise.failure(cause)
    }

  private abstract class SimpleConsumer[T](implicit ec: ExecutionContext) extends Consumer[T] with Subscriber[T] {
    protected val subscription = new AtomicReference[Subscription]
    def getSubscriber: Subscriber[T] = this
    def onSubscribe(sub: Subscription): Unit =
      if (subscription.compareAndSet(null, sub)) onSubscribe()
      else throw new IllegalStateException("Cannot be subscribed twice")
    def onComplete(): Unit = ()
    def onError(cause: Throwable): Unit = ()
    protected def onSubscribe(): Unit
    protected def schedule(block: ⇒ Unit): Unit = ec.execute {
      new Runnable {
        def run(): Unit = block
      }
    }
  }
}