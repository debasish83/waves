/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.stream2

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