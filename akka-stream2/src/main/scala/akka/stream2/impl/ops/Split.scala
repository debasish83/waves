package akka.stream2.impl
package ops

import akka.stream2.Operation.Split._
import akka.stream2.impl.OperationProcessor.SubUpstreamHandling
import akka.stream2.Flow
import akka.stream2.impl.OperationImpl.Terminated

class Split(f: Any ⇒ Command)(implicit val upstream: Upstream, val downstream: Downstream,
                              ctx: OperationProcessor.Context)
  extends OperationImpl.Stateful {

  var mainRequested = 0
  var upstreamCompleted = false
  var upstreamError: Option[Throwable] = None

  abstract class BehaviorWithOnNext extends BehaviorWithSubUpstreamHandling {
    override def onNext(element: Any) =
      f(element) match {
        case Drop   ⇒ upstream.requestMore(1)
        case Append ⇒ onNextAppend(element)
        case Last   ⇒ onNextLast(element)
        case First  ⇒ onNextFirst(element)
      }
    def onNextAppend(element: Any): Unit
    def onNextLast(element: Any): Unit
    def onNextFirst(element: Any): Unit
  }

  class WaitingForRequestMore(firstElementOfNextSubstream: Any) extends BehaviorWithSubUpstreamHandling {
    override def requestMore(elements: Int): Unit = {
      mainRequested = elements
      startBehavior.onNextFirst(firstElementOfNextSubstream)
    }
    override def onComplete(): Unit = upstreamCompleted = true
    override def onError(cause: Throwable): Unit = upstreamError = Some(cause)
  }

  val startBehavior = behavior.asInstanceOf[BehaviorWithOnNext]
  def initialBehavior: BehaviorWithOnNext =
    new BehaviorWithOnNext {
      override def requestMore(elements: Int): Unit = {
        mainRequested += elements
        if (mainRequested == elements) upstream.requestMore(1)
      }
      def onNextAppend(element: Any): Unit = onNextFirst(element)
      def onNextLast(element: Any): Unit = startNewSubstream(element, completeAfterFirstElement = true)
      def onNextFirst(element: Any): Unit = startNewSubstream(element, completeAfterFirstElement = false)
      def startNewSubstream(firstElement: Any, completeAfterFirstElement: Boolean): Unit = {
        val substream = ctx.requestSubDownstream(behavior.asInstanceOf[SubUpstreamHandling])
        become(new WaitingForSubstreamRequestMore(substream, firstElement, completeAfterFirstElement))
        mainRequested -= 1
        downstream.onNext(Flow(substream))
        if (upstreamCompleted) downstream.onComplete()
        else if (upstreamError.isDefined) downstream.onError(upstreamError.get)
      }
    }

  class WaitingForSubstreamRequestMore(substream: Downstream, firstElement: Any,
                                       completeAfterFirstElement: Boolean) extends BehaviorWithSubUpstreamHandling {
    override def requestMore(elements: Int): Unit = mainRequested += elements
    override def cancel(): Unit =
      if (!upstreamCompleted && upstreamError.isEmpty) {
        upstreamCompleted = true
        upstream.cancel()
      }
    override def onComplete(): Unit = {
      upstreamCompleted = true
      downstream.onComplete()
    }
    override def onError(cause: Throwable): Unit = {
      upstreamError = Some(cause)
      downstream.onError(cause)
    }
    override def subRequestMore(elements: Int): Unit = {
      val behavior = new InSubstream(substream, elements)
      become(behavior)
      if (completeAfterFirstElement) behavior.onNextLast(firstElement)
      else behavior.onNextAppend(firstElement)
    }
    override def subCancel(): Unit =
      if (!upstreamCompleted && upstreamError.isEmpty) {
        // the sub-downstream was cancelled before we were able to push the first element,
        // in this case we follow the principle of not dropping elements if possible, so
        // we treat the element as the first one of the next sub-stream
        if (mainRequested > 0) {
          become(startBehavior)
          upstream.requestMore(1)
        } else become(new WaitingForRequestMore(firstElement))
      }
  }

  class InSubstream(substream: Downstream, var subRequested: Int) extends BehaviorWithOnNext {
    var downstreamCancelled = false
    override def requestMore(elements: Int): Unit = mainRequested += elements
    override def cancel(): Unit = downstreamCancelled = true
    def onNextAppend(element: Any): Unit = {
      substream.onNext(element)
      subRequested -= 1
      if (upstreamCompleted) {
        become(Terminated)
        substream.onComplete()
      } else if (upstreamError.isDefined) {
        become(Terminated)
        substream.onError(upstreamError.get)
      } else if (subRequested > 0) upstream.requestMore(1)
    }
    def onNextLast(element: Any): Unit = {
      // subRequested is > 0, we don't decrease it here because
      // 1. we don't need the counter anymore
      // 2. to prevent a potential sync requestMore from the sub-downstream to propagate to upstream
      substream.onNext(element)
      substream.onComplete()
      unbecomeInSubstream()
    }
    def onNextFirst(element: Any): Unit = {
      substream.onComplete()
      if (downstreamCancelled) {
        become(Terminated)
        upstream.cancel()
      } else if (mainRequested > 0) startBehavior.onNextFirst(element)
      else become(new WaitingForRequestMore(element))
    }
    override def onComplete(): Unit = {
      become(Terminated)
      substream.onComplete()
      downstream.onComplete()
    }
    override def onError(cause: Throwable): Unit = {
      become(Terminated)
      substream.onError(cause)
      downstream.onError(cause)
    }
    override def subRequestMore(elements: Int): Unit = {
      subRequested += elements
      if (subRequested == elements) upstream.requestMore(1)
    }
    override def subCancel(): Unit = unbecomeInSubstream()

    private def unbecomeInSubstream(): Unit =
      if (downstreamCancelled) {
        become(Terminated)
        upstream.cancel()
      } else {
        become(startBehavior)
        if (mainRequested > 0) upstream.requestMore(1)
      }
  }
}
