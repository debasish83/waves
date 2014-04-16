/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.stream2

import scala.annotation.tailrec
import scala.util.control.NonFatal
import org.reactivestreams.api.{ Consumer, Producer }
import org.reactivestreams.spi.{ Subscription, Subscriber, Publisher }
import java.util.concurrent.atomic.AtomicReference

object StreamProducer {

  // since there is nothing implementation-specific to the empty producer
  // shouldn't we make it available as a static method right on `org.reactivestreams.api.Producer`?
  def empty[T]: Producer[T] = EmptyProducer.asInstanceOf[Producer[T]]

  private object EmptyProducer extends Producer[Nothing] with Publisher[Nothing] {
    def getPublisher: Publisher[Nothing] = this
    def produceTo(consumer: Consumer[Nothing]): Unit = subscribe(consumer.getSubscriber)
    def subscribe(subscriber: Subscriber[Nothing]): Unit = subscriber.onComplete()
  }

  // since there is nothing implementation-specific to the error producer
  // shouldn't we make it available as a static method right on `org.reactivestreams.api.Producer`?
  def apply[T](error: Throwable): Producer[T] = new ErrorProducer(error).asInstanceOf[Producer[T]]

  private class ErrorProducer(error: Throwable) extends Producer[Nothing] with Publisher[Nothing] {
    def getPublisher: Publisher[Nothing] = this
    def produceTo(consumer: Consumer[Nothing]): Unit = subscribe(consumer.getSubscriber)
    def subscribe(subscriber: Subscriber[Nothing]): Unit = subscriber.onError(error)
  }

  /**
   * Constructs a Producer which efficiently produces the elements from the given Iterable
   * synchronously in `subscription.requestMore`.
   *
   * CAUTION: This is a convenience wrapper designed for iterables which are static collections.
   * Do *NOT* use it for iterables on lazy collections or other implementations that do more
   * than merely retrieve an element in their `next()` method!
   */
  def apply[T](iterable: Iterable[T]): Producer[T] = apply(iterable.iterator)

  /**
   * Constructs a Producer which efficiently produces the elements from the given iterator
   * synchronously in `subscription.requestMore`.
   *
   * CAUTION: This is a convenience wrapper designed for iterators over static collections.
   * Do *NOT* use it for iterators on lazy collections or other implementations that do more
   * than merely retrieve an element in their `next()` method!
   */
  def apply[T](iterator: Iterator[T]): Producer[T] =
    new IteratorProducer(iterator).asInstanceOf[Producer[T]]

  private class IteratorProducer(iterator: Iterator[Any]) extends Producer[Any] with Publisher[Any] {
    private trait State extends Publisher[Any] with Subscription
    private[this] val state = new AtomicReference[State](Fresh)

    def getPublisher: Publisher[Any] = this
    def produceTo(consumer: Consumer[Any]): Unit = subscribe(consumer.getSubscriber)
    def subscribe(subscriber: Subscriber[Any]): Unit = state.get.subscribe(subscriber)

    private object Fresh extends State {
      def subscribe(subscriber: Subscriber[Any]): Unit = {
        val running = new Running(subscriber)
        if (state.compareAndSet(Fresh, running)) {
          subscriber.onSubscribe {
            new Subscription {
              def requestMore(elements: Int): Unit = state.get.requestMore(elements)
              def cancel(): Unit = state.get.cancel()
            }
          }
        } else running.subscribe(subscriber)
      }
      def requestMore(elements: Int) = throw new IllegalStateException
      def cancel() = throw new IllegalStateException
    }
    private class Running(subscriber: Subscriber[Any]) extends State {
      def subscribe(subscriber: Subscriber[Any]): Unit = Closed.subscribe(subscriber)
      @tailrec final def requestMore(elements: Int) =
        if (elements > 0) {
          val recurse =
            try {
              if (iterator.hasNext) {
                subscriber.onNext(iterator.next())
                true
              } else {
                subscriber.onComplete()
                false
              }
            } catch {
              case NonFatal(e) ⇒
                subscriber.onError(e)
                false
            }
          if (recurse) requestMore(elements - 1)
        }
      def cancel() = state.set(Closed)
    }
    private object Closed extends State {
      def subscribe(subscriber: Subscriber[Any]): Unit =
        subscriber.onError(new RuntimeException("Cannot subscribe more than one subscriber"))
      def requestMore(elements: Int) = () // we have to ignore here
      def cancel() = () // we have to ignore here
    }
  }
}
