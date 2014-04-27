/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.stream2

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.util.control.NonFatal
import scala.concurrent.{ ExecutionContext, Future }
import org.reactivestreams.spi.{ Subscription, Subscriber }
import org.reactivestreams.api.Producer
import scala.util.{ Failure, Success, Try }

object StreamProducer {

  // a producer that always completes the subscriber directly in `subscribe`
  def empty[T]: Producer[T] = EmptyProducer.asInstanceOf[Producer[T]]

  // case object so we get value equality
  case object EmptyProducer extends AbstractProducer[Any] {
    def subscribe(subscriber: Subscriber[Any]) = subscriber.onComplete()
  }

  // a producer that always calls `subscriber.onError` directly in `subscribe`
  def error[T](error: Throwable): Producer[T] = new ErrorProducer(error).asInstanceOf[Producer[T]]

  // case class so we get value equality
  case class ErrorProducer(error: Throwable) extends AbstractProducer[Any] {
    def subscribe(subscriber: Subscriber[Any]) = subscriber.onError(error)
  }

  /**
   * Shortcut for constructing an `ForIterable`.
   */
  def of[T](elements: T*): Producer[T] = apply(elements)

  /**
   * Shortcut for constructing an `ForIterable`.
   */
  def apply[T](iterable: Iterable[T]): Producer[T] = ForIterable(iterable)

  /**
   * A producer supporting unlimited subscribers which all receive independent subscriptions which
   * efficiently produce the elements of the given Iterable synchronously in `subscription.requestMore`.
   * Provides value equality.
   */
  case class ForIterable[T](iterable: Iterable[T]) extends AbstractProducer[T] {
    def subscribe(subscriber: Subscriber[T]) =
      subscriber.onSubscribe(new IteratorSubscription(iterable.iterator, subscriber))
  }

  /**
   * Constructs a Producer which efficiently produces the elements from the given iterator
   * synchronously in `subscription.requestMore`.
   *
   * CAUTION: This is a convenience wrapper designed for iterators over static collections.
   * Do *NOT* use it for iterators on lazy collections or other implementations that do more
   * than merely retrieve an element in their `next()` method!
   */
  def apply[T](iterator: Iterator[T]): Producer[T] =
    new AtomicReference[Subscriber[T]] with AbstractProducer[T] {
      def subscribe(subscriber: Subscriber[T]) =
        if (compareAndSet(null, subscriber)) {
          subscriber.onSubscribe(new IteratorSubscription(iterator, subscriber))
        } else subscriber.onError(new RuntimeException("Cannot subscribe more than one subscriber"))
    }

  private class IteratorSubscription[T](iterator: Iterator[T], subscriber: Subscriber[T]) extends Subscription {
    @volatile var cancelled = false
    @tailrec final def requestMore(elements: Int) =
      if (!cancelled && elements > 0) {
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
    def cancel() = cancelled = true
  }

  /**
   * A producer supporting unlimited subscribers which all receive independent subscriptions to
   * a single element stream producing the value of the given future.
   * If the future is already completed at the time of the first `subscription.requestMore` the
   * value is produced synchronously in `subscription.requestMore`.
   */
  def apply[T](future: Future[T])(implicit executor: ExecutionContext): Producer[T] =
    new AbstractProducer[T] {
      def subscribe(subscriber: Subscriber[T]) =
        subscriber.onSubscribe {
          new Subscription {
            @volatile var cancelled = false
            def requestMore(elements: Int) =
              if (!cancelled)
                if (future.isCompleted) dispatch(future.value.get)
                else future.onComplete(dispatch)
            def cancel() = cancelled = true
            def dispatch(value: Try[T]): Unit =
              if (!cancelled)
                value match {
                  case Success(x) ⇒
                    subscriber.onNext(x)
                    subscriber.onComplete()
                  case Failure(error) ⇒ subscriber.onError(error)
                }
          }
        }
    }
}
