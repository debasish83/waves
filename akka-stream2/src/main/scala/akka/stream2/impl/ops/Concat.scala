package akka.stream2.impl
package ops

import org.reactivestreams.api.Producer
import OperationProcessor.SubDownstreamHandling

class Concat(next: () ⇒ Producer[Any])(implicit val upstream: Upstream, val downstream: Downstream,
                                       ctx: OperationProcessor.Context)
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
        ctx.requestSubUpstream(next(), behavior.asInstanceOf[SubDownstreamHandling])
      }
    }

  class WaitingForSecondFlowSubscription(var requested: Int) extends BehaviorWithSubDownstreamHandling {
    var cancelled = false
    override def requestMore(elements: Int) = requested += elements
    override def cancel() = cancelled = true
    override def subOnSubscribe(upstream2: Upstream) =
      if (cancelled) {
        upstream2.cancel()
      } else {
        become(new Draining(upstream2))
        upstream2.requestMore(requested)
      }
    override def subOnComplete() = downstream.onComplete()
    override def subOnError(cause: Throwable) = downstream.onError(cause)
  }

  // when we enter this state we have already requested all so far requested elements from upstream2
  class Draining(upstream2: Upstream) extends BehaviorWithSubDownstreamHandling {
    override def requestMore(elements: Int) = upstream2.requestMore(elements)
    override def cancel() = upstream2.cancel()
    override def subOnNext(element: Any) = downstream.onNext(element)
    override def subOnComplete() = downstream.onComplete()
    override def subOnError(cause: Throwable) = downstream.onError(cause)
  }
}