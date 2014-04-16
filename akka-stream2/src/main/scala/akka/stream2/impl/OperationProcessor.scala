package akka.stream2
package impl

import akka.actor.{ ActorRefFactory, PoisonPill, Props }
import org.reactivestreams.api.{ Consumer, Producer, Processor }
import org.reactivestreams.spi
import spi.{ Publisher, Subscription, Subscriber }

// Processor interface around an `OperationProcessor.Actor`
class OperationProcessor[I, O](op: Operation[I, O],
                               initialFanOutBufferSize: Int = 1,
                               maxFanOutBufferSize: Int = 16)(implicit refFactory: ActorRefFactory)
  extends Processor[I, O] {
  import OperationProcessor._

  private val actor =
    refFactory.actorOf(Props(new OperationProcessor.Actor(op, initialFanOutBufferSize, maxFanOutBufferSize)))

  val getSubscriber: Subscriber[I] =
    new Subscriber[I] {
      def onSubscribe(subscription: Subscription): Unit = actor ! OnSubscribed(subscription)
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
  case class OnSubscribed(subscription: Subscription)
  case class OnNext(element: Any) // TODO: remove this model and make it the default match
  case object OnComplete
  case class OnError(cause: Throwable)

  // Publisher-side messages
  case class Subscribe(subscriber: Subscriber[Any])
  case class MoreRequested(subscription: Subscription, elements: Int)
  case class UnregisterSubscription(subscription: Subscription)

  // other messages
  case class Job(thunk: () ⇒ Unit)

  trait Context {
    def requestSubUpstream(flow: Flow[Any], subDownstream: ⇒ SubDownstreamHandling): Unit
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
  class Actor(op: OperationX, initialFanOutBufferSize: Int, maxFanOutBufferSize: Int)
    extends AbstractProducer[Any](initialFanOutBufferSize, maxFanOutBufferSize)
    with akka.actor.Actor with Downstream with Context {

    @volatile var isShutdown = false
    var _chain: Option[OperationChain] = None
    def chain: OperationChain = _chain.getOrElse(sys.error("Upstream not yet connected"))

    def receive: Receive = {
      // Subscriber side
      case OnNext(element)                       ⇒ chain.onNext(element)
      case OnComplete                            ⇒ chain.onComplete()
      case OnError(e)                            ⇒ chain.onError(e)
      case OnSubscribed(subscription)            ⇒ _chain = Some(new OperationChain(op, Upstream(subscription), this, this))

      // Publisher side
      case Subscribe(subscriber)                 ⇒ subscribe(subscriber)
      case MoreRequested(subscription, elements) ⇒ super.moreRequested(subscription.asInstanceOf[Subscription], elements)
      case UnregisterSubscription(subscription)  ⇒ super.unregisterSubscription(subscription.asInstanceOf[Subscription])

      // other
      case Job(thunk)                            ⇒ thunk()
    }

    def requestFromUpstream(elements: Int): Unit = chain.requestMore(elements)
    def cancelUpstream(): Unit = chain.cancel()
    def shutdown(): Unit = {
      isShutdown = true
      self ! PoisonPill // we don't `context.stop(self)` here in order to prevent dead letters
    }

    // `Downstream` interface
    def onNext(element: Any): Unit = pushToDownstream(element)
    def onComplete(): Unit = completeDownstream()
    def onError(cause: Throwable): Unit = abortDownstream(cause)

    // these two methods are directly called from a Subscriber thread, therefore we need to "move" them into the actor
    override def moreRequested(subscription: Subscription, elements: Int) =
      if (!isShutdown) self ! MoreRequested(subscription, elements)
    override def unregisterSubscription(subscription: Subscription) =
      if (!isShutdown) self ! UnregisterSubscription(subscription)

    // Context interface
    def requestSubUpstream(flow: Flow[Any], subDownstream: ⇒ SubDownstreamHandling): Unit =
      flow.toProducer.getPublisher.subscribe {
        new Subscriber[Any] {
          def onSubscribe(subscription: spi.Subscription): Unit = schedule(subDownstream.subOnSubscribe(Upstream(subscription)))
          def onNext(element: Any): Unit = schedule(subDownstream.subOnNext(element))
          def onComplete(): Unit = schedule(subDownstream.subOnComplete())
          def onError(cause: Throwable): Unit = schedule(subDownstream.subOnError(cause))
        }
      }
    def requestSubDownstream(subUpstream: ⇒ SubUpstreamHandling): Producer[Any] with Downstream =
      new AbstractProducer[Any](initialFanOutBufferSize, maxFanOutBufferSize) with Downstream { // TODO: add buffer config facility
        def requestFromUpstream(elements: Int) = subUpstream.subRequestMore(elements)
        def cancelUpstream() = subUpstream.subCancel()
        def shutdown() = () // nothing to do

        // these two methods are directly called from a Subscriber thread, therefore we need to "move" them into the actor
        override def moreRequested(subscription: Subscription, elements: Int): Unit =
          schedule(super.moreRequested(subscription, elements))
        override def unregisterSubscription(subscription: Subscription) =
          schedule(super.unregisterSubscription(subscription))

        // `Downstream` interface
        def onNext(element: Any): Unit = pushToDownstream(element)
        def onComplete(): Unit = completeDownstream()
        def onError(cause: Throwable): Unit = abortDownstream(cause)
      }

    // helpers
    def schedule(body: ⇒ Unit): Unit = self ! Job(body _)
  }
}