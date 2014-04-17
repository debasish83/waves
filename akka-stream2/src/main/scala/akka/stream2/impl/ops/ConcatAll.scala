package akka.stream2.impl
package ops

import akka.stream2.impl.OperationImpl.Terminated
import OperationProcessor.SubDownstreamHandling
import org.reactivestreams.api.Producer

class ConcatAll(implicit val upstream: Upstream, val downstream: Downstream, ctx: OperationProcessor.Context)
  extends OperationImpl.Stateful {

  var requested = 0
  var upstreamCompleted = false
  val startBehavior = behavior

  def initialBehavior: BehaviorWithSubDownstreamHandling =
    new BehaviorWithSubDownstreamHandling {
      override def requestMore(elements: Int): Unit = {
        requested += elements
        if (requested == elements) upstream.requestMore(1)
      }
      override def onNext(element: Any) =
        element match {
          case producer: Producer[_] ⇒
            become(new WaitingForSubstreamSubscription)
            ctx.requestSubUpstream(producer.asInstanceOf[Producer[Any]], behavior.asInstanceOf[SubDownstreamHandling])
          case _ ⇒ sys.error("Illegal operation setup, expected `Producer[_]` but got " + element)
        }
    }

  class WaitingForSubstreamSubscription extends BehaviorWithSubDownstreamHandling {
    var cancelled = false
    var error: Option[Throwable] = None
    override def requestMore(elements: Int): Unit = requested += elements
    override def cancel(): Unit = {
      upstream.cancel()
      cancelled = true
    }
    override def onComplete(): Unit = upstreamCompleted = true
    override def onError(cause: Throwable): Unit = error = Some(cause)

    override def subOnSubscribe(subUpstream: Upstream): Unit =
      if (cancelled) {
        become(Terminated)
        subUpstream.cancel()
      } else {
        become(new Draining(subUpstream, error))
        subUpstream.requestMore(requested)
      }
    override def subOnComplete(): Unit =
      if (!cancelled) finishSubstream(error)
    // else if we were cancelled while waiting for the subscription and the sub stream is empty we are done
    override def subOnError(cause: Throwable) = {
      become(Terminated)
      if (!cancelled) {
        downstream.onError(cause)
        upstream.cancel()
      }
    }
  }

  // when we enter this state we have already requested `requested` elements from the subUpstream
  class Draining(subUpstream: Upstream, var error: Option[Throwable]) extends BehaviorWithSubDownstreamHandling {
    override def requestMore(elements: Int): Unit = {
      requested += elements
      subUpstream.requestMore(elements)
    }
    override def cancel(): Unit = {
      subUpstream.cancel()
      upstream.cancel()
    }
    override def onComplete(): Unit = upstreamCompleted = true
    override def onError(cause: Throwable): Unit = error = Some(cause)

    override def subOnNext(element: Any): Unit = {
      requested -= 1
      downstream.onNext(element)
    }
    override def subOnComplete(): Unit = finishSubstream(error)
    override def subOnError(cause: Throwable): Unit = {
      become(Terminated)
      upstream.cancel()
      downstream.onError(cause)
    }
  }

  def finishSubstream(error: Option[Throwable]): Unit = {
    if (upstreamCompleted) {
      become(Terminated)
      downstream.onComplete()
    } else if (error.isDefined) {
      become(Terminated)
      downstream.onError(error.get)
    } else {
      become(startBehavior)
      if (requested > 0) upstream.requestMore(1)
    }
  }
}