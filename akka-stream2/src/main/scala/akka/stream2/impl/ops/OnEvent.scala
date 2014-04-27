package akka.stream2.impl
package ops

import scala.util.control.NonFatal
import akka.stream2.Operation.StreamEvent

class OnEvent(callback: StreamEvent[Any] ⇒ Unit)(implicit val upstream: Upstream, val downstream: Downstream)
  extends OperationImpl.Abstract {

  override def requestMore(elements: Int) = {
    dispatch(StreamEvent.RequestMore(elements))
    upstream.requestMore(elements)
  }

  override def cancel() = {
    dispatch(StreamEvent.Cancel)
    upstream.cancel()
  }

  override def onNext(element: Any) = {
    dispatch(StreamEvent.OnNext(element))
    downstream.onNext(element)
  }

  override def onComplete() = {
    dispatch(StreamEvent.OnComplete)
    downstream.onComplete()
  }

  override def onError(cause: Throwable) = {
    dispatch(StreamEvent.OnError(cause))
    downstream.onError(cause)
  }

  private def dispatch(ev: StreamEvent[Any]): Unit =
    try callback(ev)
    catch {
      case NonFatal(e) ⇒ downstream.onError(e)
    }
}