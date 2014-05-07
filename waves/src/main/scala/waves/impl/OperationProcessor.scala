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
package impl

import scala.concurrent.ExecutionContext
import java.util.concurrent.atomic.AtomicReference
import org.reactivestreams.api.{ Consumer, Producer, Processor }
import org.reactivestreams.spi
import spi.{ Publisher, Subscription, Subscriber }

class OperationProcessor[I, O](op: Operation[I, O])(implicit ec: ExecutionContext)
    extends Processor[I, O] {
  import OperationProcessor._

  private val actor: SimpleActor = {
    // TODO: make buffer setup configurable
    val operationWithInputAndOutputBuffers = Operation.Buffer[I](4) ~> op ~> Operation.Buffer[O](4)
    new OperationProcessor.Actor(operationWithInputAndOutputBuffers)
  }

  val getSubscriber: Subscriber[I] =
    new Subscriber[I] {
      def onSubscribe(subscription: Subscription): Unit = actor ! OnSubscribe(subscription)
      def onNext(element: I): Unit = actor ! OnNext(element)
      def onComplete(): Unit = actor ! OnComplete
      def onError(cause: Throwable): Unit = actor ! OnError(cause)
      override def toString = s"ProcessorSubscriber($op)"
    }

  val getPublisher: Publisher[O] =
    new Publisher[O] {
      def subscribe(subscriber: Subscriber[O]) = actor ! Subscribe(subscriber.asInstanceOf[Subscriber[Any]])
    }

  def produceTo(consumer: Consumer[O]): Unit = getPublisher.subscribe(consumer.getSubscriber)

  override def toString = s"OperationProcessor($op)"
}

object OperationProcessor {
  // Subscriber-side messages
  private case class OnSubscribe(subscription: Subscription)
  private case class OnNext(element: Any) // TODO: remove this model and make it the default match
  private case object OnComplete
  private case class OnError(cause: Throwable)

  // Publisher-side messages
  private case class Subscribe(subscriber: Subscriber[Any])
  private case class RequestMore(elements: Int)
  private case object Cancel

  // other messages
  private case class Job(thunk: () ⇒ Unit)

  trait Context {
    def requestSubUpstream[T <: Any](producer: Producer[T], subDownstream: ⇒ SubDownstreamHandling): Unit
    def requestSubDownstream(subUpstream: ⇒ SubUpstreamHandling): Producer[Any] with Downstream
  }

  trait SubDownstreamHandling {
    def subOnSubscribe(subUpstream: Upstream): Unit
    def subOnNext(element: Any): Unit
    def subOnComplete(): Unit
    def subOnError(cause: Throwable): Unit
  }

  trait SubUpstreamHandling {
    def subRequestMore(elements: Int): Unit
    def subCancel(): Unit
  }

  // Actor providing execution logic around an `OperationChain`
  private class Actor(op: OperationX)(implicit ec: ExecutionContext) extends SimpleActor with Subscription with Context {
    val chain = new OperationChain(op, this)

    val receive: Receive = {
      case OnNext(element)           ⇒ chain.leftDownstream.onNext(element)
      case OnComplete                ⇒ chain.leftDownstream.onComplete()
      case OnError(e)                ⇒ chain.leftDownstream.onError(e)

      case RequestMore(elements)     ⇒ chain.rightUpstream.requestMore(elements)
      case Cancel                    ⇒ chain.rightUpstream.cancel()

      case Job(thunk)                ⇒ thunk()

      case OnSubscribe(subscription) ⇒ chain.connectUpstream(subscription)
      case Subscribe(subscriber)     ⇒ connectDownstream(subscriber)
    }

    def connectDownstream(subscriber: Subscriber[Any]): Unit = {
      chain.connectDownstream {
        new Downstream {
          def onNext(element: Any) = subscriber.onNext(element)
          def onComplete() = subscriber.onComplete()
          def onError(cause: Throwable) = subscriber.onError(cause)
          override def toString = s"Downstream($subscriber)"
        }
      }
      subscriber.onSubscribe(this)
    }

    // outside Subscription interface facing downstream, called from another thread
    def requestMore(elements: Int) = this ! RequestMore(elements)
    def cancel() = this ! Cancel

    // Context interface
    def requestSubUpstream[T <: Any](producer: Producer[T], subDownstream: ⇒ SubDownstreamHandling): Unit =
      producer.getPublisher.subscribe {
        new Subscriber[T] {
          def onSubscribe(subscription: Subscription) = schedule(subDownstream.subOnSubscribe(subscription))
          def onNext(element: T) = schedule(subDownstream.subOnNext(element))
          def onComplete() = schedule(subDownstream.subOnComplete())
          def onError(cause: Throwable) = schedule(subDownstream.subOnError(cause))
          override def toString = s"SubUpstream($producer)"
        }
      }
    def requestSubDownstream(subUpstream: ⇒ SubUpstreamHandling): Producer[Any] with Downstream =
      new AtomicReference[Subscriber[Any]] with AbstractProducer[Any] with Subscription with Downstream {
        def subscribe(subscriber: Subscriber[Any]) =
          if (compareAndSet(null, subscriber)) subscriber.onSubscribe(this)
          else subscriber.onError(new RuntimeException("Cannot subscribe more than one subscriber"))

        def onNext(element: Any) = get.onNext(element)
        def onComplete() = get.onComplete()
        def onError(cause: Throwable) = get.onError(cause)

        // outside upstream interface facing downstream, called from another thread
        def requestMore(elements: Int) = schedule(subUpstream.subRequestMore(elements))
        def cancel() = schedule(subUpstream.subCancel())
        override def toString = s"SubDownstream(${Option(get) getOrElse "<unsubscribed>"}"
      }

    // helpers
    def schedule(body: ⇒ Unit): Unit = this ! Job(body _)
  }
}