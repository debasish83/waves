package akka.stream2
package impl

import java.util.concurrent.atomic.AtomicReference
import akka.actor.{ PoisonPill, ActorRefFactory, Props }
import org.reactivestreams.api.{ Consumer, Producer, Processor }
import org.reactivestreams.spi
import spi.{ Publisher, Subscription, Subscriber }

// Processor interface around an `OperationProcessor.Actor`
class OperationProcessor[I, O](op: Operation[I, O])(implicit refFactory: ActorRefFactory)
  extends Processor[I, O] {
  import OperationProcessor._

  private val actor =
    refFactory.actorOf(Props(new OperationProcessor.Actor(op)))

  val getSubscriber: Subscriber[I] =
    new Subscriber[I] {
      def onSubscribe(subscription: Subscription): Unit = actor ! OnSubscribe(subscription)
      def onNext(element: I): Unit = actor ! OnNext(element)
      def onComplete(): Unit = actor ! OnComplete
      def onError(cause: Throwable): Unit = actor ! OnError(cause)
    }

  val getPublisher: Publisher[O] =
    new Publisher[O] {
      def subscribe(subscriber: Subscriber[O]) = actor ! Subscribe(subscriber.asInstanceOf[Subscriber[Any]])
    }

  def produceTo(consumer: Consumer[O]): Unit = getPublisher.subscribe(consumer.getSubscriber)
}

object OperationProcessor {
  // Subscriber-side messages
  case class OnSubscribe(subscription: Subscription)
  case class OnNext(element: Any) // TODO: remove this model and make it the default match
  case object OnComplete
  case class OnError(cause: Throwable)

  // Publisher-side messages
  case class Subscribe(subscriber: Subscriber[Any])
  case class RequestMore(elements: Int)
  case object Cancel

  // other messages
  case class Job(thunk: () ⇒ Unit)

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
  class Actor(op: OperationX) extends akka.actor.Actor with Subscription with Context {

    // if both the outer upstream and outer downstream 
    var upstreamCompleted = false
    var downstreamCompleted = false

    def receive: Receive = {
      case OnSubscribe(subscription) ⇒ context.become(waitingForDownstreamConnection(subscription))
      case Subscribe(subscriber)     ⇒ context.become(waitingForUpstreamConnection(subscriber))
    }

    def waitingForDownstreamConnection(subscription: Subscription): Receive = {
      case Subscribe(subscriber) ⇒ context.become(running(subscription, subscriber))
    }

    def waitingForUpstreamConnection(subscriber: Subscriber[Any]): Receive = {
      case OnSubscribe(subscription)  ⇒ context.become(running(subscription, subscriber))
      case Subscribe(otherSubscriber) ⇒ reject(otherSubscriber)
    }

    def running(subscription: Subscription, subscriber: Subscriber[Any]): Receive = {
      subscriber.onSubscribe(this)
      val chain = new OperationChain(op, leftUpstream(subscription), rightDownstream(subscriber), this);
      {
        case OnNext(element)            ⇒ chain.leftDownstream.onNext(element)
        case OnComplete                 ⇒ markUpstreamCompleted(chain.leftDownstream.onComplete())
        case OnError(e)                 ⇒ markUpstreamCompleted(chain.leftDownstream.onError(e))

        case RequestMore(elements)      ⇒ chain.rightUpstream.requestMore(elements)
        case Cancel                     ⇒ markDownstreamCompleted(chain.rightUpstream.cancel())
        case Subscribe(otherSubscriber) ⇒ reject(otherSubscriber)

        case Job(thunk)                 ⇒ thunk()
      }
    }

    def leftUpstream(subscription: Subscription) =
      new Upstream {
        def requestMore(elements: Int) = subscription.requestMore(elements)
        def cancel() = markUpstreamCompleted(subscription.cancel())
      }

    def rightDownstream(subscriber: Subscriber[Any]) =
      new Downstream {
        def onNext(element: Any) = subscriber.onNext(element)
        def onError(cause: Throwable) = markDownstreamCompleted(subscriber.onError(cause))
        def onComplete() = markDownstreamCompleted(subscriber.onComplete())
      }

    def markUpstreamCompleted(unit: Unit) =
      if (downstreamCompleted) self ! PoisonPill
      else upstreamCompleted = true

    def markDownstreamCompleted(unit: Unit) =
      if (upstreamCompleted) self ! PoisonPill
      else downstreamCompleted = true

    def reject(subscriber: Subscriber[Any]): Unit =
      subscriber.onError(new RuntimeException("Cannot subscribe more than one subscriber"))

    // outside Subscription interface facing downstream, called from another thread
    def requestMore(elements: Int) = self ! RequestMore(elements)
    def cancel() = self ! Cancel

    // Context interface
    def requestSubUpstream[T <: Any](producer: Producer[T], subDownstream: ⇒ SubDownstreamHandling): Unit =
      producer.getPublisher.subscribe {
        new Subscriber[T] {
          def onSubscribe(subscription: Subscription) = schedule(subDownstream.subOnSubscribe(subscription))
          def onNext(element: T) = schedule(subDownstream.subOnNext(element))
          def onComplete() = schedule(subDownstream.subOnComplete())
          def onError(cause: Throwable) = schedule(subDownstream.subOnError(cause))
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
      }

    // helpers
    def schedule(body: ⇒ Unit): Unit = self ! Job(body _)
  }
}