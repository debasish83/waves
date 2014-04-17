package akka.stream2.impl
package ops

import akka.stream2.impl.OperationImpl.Terminated
import org.reactivestreams.api.Producer
import OperationProcessor.SubDownstreamHandling

class Concat(producer: Producer[Any])(implicit val upstream: Upstream, val downstream: Downstream, ctx: OperationProcessor.Context)
  extends OperationImpl.Stateful {

  def initialBehavior: Behavior =
    new Behavior {
      var requested = 0
      override def requestMore(elements: Int): Unit = {
        requested += elements
        upstream.requestMore(elements)
      }
      override def onNext(element: Any) = {
        requested -= 1
        downstream.onNext(element)
      }
      override def onComplete(): Unit = {
        become(new WaitingForSecondFlowSubscription(requested))
        ctx.requestSubUpstream(producer, behavior.asInstanceOf[SubDownstreamHandling])
      }
    }

  class WaitingForSecondFlowSubscription(var requested: Int) extends BehaviorWithSubDownstreamHandling {
    var cancelled = false
    override def requestMore(elements: Int): Unit = requested += elements
    override def cancel(): Unit = cancelled = true
    override def subOnSubscribe(upstream2: Upstream): Unit =
      if (cancelled) {
        become(Terminated)
        upstream2.cancel()
      } else {
        become(new Draining(upstream2))
        upstream2.requestMore(requested)
      }
    override def subOnComplete(): Unit = {
      become(Terminated)
      if (!cancelled) downstream.onComplete()
    }
    override def subOnError(cause: Throwable) = {
      become(Terminated)
      if (!cancelled) downstream.onError(cause)
    }
  }

  // when we enter this state we have already requested all so far requested elements from upstream2
  class Draining(upstream2: Upstream) extends BehaviorWithSubDownstreamHandling {
    override def requestMore(elements: Int): Unit = upstream2.requestMore(elements)
    override def cancel(): Unit = upstream2.cancel()
    override def subOnNext(element: Any): Unit = downstream.onNext(element)
    override def subOnComplete(): Unit = {
      become(Terminated)
      downstream.onComplete()
    }
    override def subOnError(cause: Throwable): Unit = {
      become(Terminated)
      downstream.onError(cause)
    }
  }
}