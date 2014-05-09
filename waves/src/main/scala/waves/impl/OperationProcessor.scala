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
import java.util.concurrent.atomic.AtomicBoolean
import org.reactivestreams.api.{ Consumer, Producer, Processor }
import org.reactivestreams.spi
import spi.{ Publisher, Subscription, Subscriber }

class OperationProcessor[I, O](op: Operation[I, O])(implicit ec: ExecutionContext) extends Processor[I, O] {
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

  trait Context {
    def requestSecondaryUpstream[T <: Any](producer: Producer[T], impl: OperationImpl.WithSecondaryDownstreamBehavior): Unit
    def requestSecondaryDownstream(impl: OperationImpl.WithSecondaryUpstreamBehavior): Producer[Any] with Downstream
  }

  // Actor providing execution logic around an `OperationChain`
  private class Actor(op: OperationX)(implicit ec: ExecutionContext) extends SimpleActor with Subscription with Context {
    private[this] val upstreamConnector = new UpstreamConnector
    private[this] val downstreamConnector = new DownstreamConnector

    materialize(op, upstreamConnector, downstreamConnector, this)

    startMessageProcessing()

    def apply(msg: AnyRef): Unit = msg match {
      case OnNext(element)           ⇒ upstreamConnector.onNext(element)
      case OnComplete                ⇒ upstreamConnector.onComplete()
      case OnError(e)                ⇒ upstreamConnector.onError(e)

      case RequestMore(elements)     ⇒ downstreamConnector.requestMore(elements)
      case Cancel                    ⇒ downstreamConnector.cancel()

      case job: Function0[_]         ⇒ job()

      case OnSubscribe(subscription) ⇒ upstreamConnector.connectUpstream(subscription)
      case Subscribe(subscriber)     ⇒ connectDownstream(subscriber)
    }

    def connectDownstream(subscriber: Subscriber[Any]): Unit = {
      downstreamConnector.connectDownstream(Downstream(subscriber))
      subscriber.onSubscribe(this)
    }

    // outside Subscription interface facing downstream, called from another thread
    def requestMore(elements: Int) = this ! RequestMore(elements)
    def cancel() = this ! Cancel

    // Context interface
    def requestSecondaryUpstream[T <: Any](producer: Producer[T], impl: OperationImpl.WithSecondaryDownstreamBehavior): Unit = {
      val connector = new UpstreamConnector
      connector.connectDownstream {
        new Downstream {
          def onNext(element: Any) = impl.behavior.secondaryOnNext(element)
          def onComplete() = impl.behavior.secondaryOnComplete()
          def onError(cause: Throwable) = impl.behavior.secondaryOnError(cause)
        }
      }
      producer.getPublisher.subscribe {
        new Subscriber[T] {
          def onSubscribe(subscription: Subscription) = schedule {
            connector.connectUpstream(subscription)
            impl.behavior.secondaryOnSubscribe(connector)
          }
          def onNext(element: T) = schedule(connector.onNext(element))
          def onComplete() = schedule(connector.onComplete())
          def onError(cause: Throwable) = schedule(connector.onError(cause))
          override def toString = s"SecondaryUpstream($producer)"
        }
      }
    }

    def requestSecondaryDownstream(impl: OperationImpl.WithSecondaryUpstreamBehavior): Producer[Any] with Downstream =
      new AtomicBoolean with AbstractProducer[Any] with Subscription with Downstream {
        val connector = new DownstreamConnector
        connector.connectUpstream {
          new Upstream {
            def requestMore(elements: Int) = impl.behavior.secondaryRequestMore(elements)
            def cancel() = impl.behavior.secondaryCancel()
          }
        }
        def subscribe(subscriber: Subscriber[Any]) =
          if (compareAndSet(false, true)) {
            schedule(connector.connectDownstream(Downstream(subscriber)))
            subscriber.onSubscribe(this)
          } else subscriber.onError(new RuntimeException("Cannot subscribe more than one subscriber"))

        def onNext(element: Any) = connector.onNext(element)
        def onComplete() = connector.onComplete()
        def onError(cause: Throwable) = connector.onError(cause)

        // outside upstream interface facing downstream, called from another thread
        def requestMore(elements: Int) = schedule(connector.requestMore(elements))
        def cancel() = schedule(connector.cancel())
        override def toString = s"SecondaryDownstream(${Option(get) getOrElse "<unsubscribed>"}"
      }

    // helpers
    def schedule(body: ⇒ Unit): Unit = this ! body _
  }
}