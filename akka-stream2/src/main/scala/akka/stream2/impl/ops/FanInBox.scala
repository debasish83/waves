package akka.stream2.impl
package ops

import org.reactivestreams.api.Producer
import akka.stream2.FanIn
import OperationProcessor.SubDownstreamHandling

class FanInBox(secondary: Producer[Any], fanInProvider: FanIn.Provider[FanIn])(implicit val upstream: Upstream,
                                                                               val downstream: Downstream, ctx: OperationProcessor.Context)
  extends OperationImpl.Stateful {

  ctx.requestSubUpstream(secondary, behavior.asInstanceOf[SubDownstreamHandling])

  def initialBehavior: BehaviorWithSubDownstreamHandling =
    new BehaviorWithSubDownstreamHandling {
      var requested = 0
      var error: Option[Throwable] = _
      override def requestMore(elements: Int) = requested += elements
      override def cancel() = requested = Int.MinValue
      override def onComplete() = error = None
      override def onError(cause: Throwable) = error = Some(cause)
      override def subOnSubscribe(subUpstream: Upstream): Unit = {
        val fanIn = fanInProvider(upstream, subUpstream, downstream)
        become(wired(fanIn))
        error match {
          case null ⇒
            if (requested > 0) fanIn.requestMore(requested)
            else if (requested < 0) fanIn.cancel()
          case None        ⇒ fanIn.primaryOnComplete()
          case Some(cause) ⇒ fanIn.primaryOnError(cause)
        }
      }
    }

  def wired(fanIn: FanIn[Any, Any]): BehaviorWithSubDownstreamHandling =
    new BehaviorWithSubDownstreamHandling {
      override def requestMore(elements: Int): Unit = fanIn.requestMore(elements)
      override def cancel(): Unit = fanIn.cancel()

      override def onNext(element: Any): Unit = fanIn.primaryOnNext(element)
      override def onComplete(): Unit = fanIn.primaryOnComplete()
      override def onError(cause: Throwable): Unit = fanIn.primaryOnError(cause)

      override def subOnError(cause: Throwable): Unit = fanIn.secondaryOnError(cause)
      override def subOnNext(element: Any): Unit = fanIn.secondaryOnNext(element)
      override def subOnComplete(): Unit = fanIn.secondaryOnComplete()
    }
}