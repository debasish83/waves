package akka.stream2.impl
package ops

import scala.annotation.tailrec

class CustomBuffer(seed: Any,
                   compress: (Any, Any) ⇒ Any,
                   expand: Any ⇒ (Any, Option[Any]),
                   canConsume: Any ⇒ Boolean)(implicit val upstream: Upstream, val downstream: Downstream)
  extends OperationImpl.Abstract {

  var requested = 0
  var state = seed
  var elementPendingFromUpstream = false

  override def requestMore(elements: Int): Unit = {
    requested += elements
    if (requested == elements) tryExpanding()
  }

  override def onNext(element: Any): Unit = {
    elementPendingFromUpstream = false
    state =
      try compress(state, element)
      catch {
        case t: Throwable ⇒
          downstream.onError(t)
          upstream.cancel()
          return
      }
    if (requested > 0) tryExpanding()
    else requestOneFromUpstreamIfPossibleAndRequired()
  }

  @tailrec private def tryExpanding(): Unit =
    if (requested > 0) {
      val expandResult =
        try expand(state)
        catch {
          case t: Throwable ⇒
            downstream.onError(t)
            upstream.cancel()
            return
        }
      expandResult match {
        case (nextState, None) ⇒
          state = nextState
          requestOneFromUpstreamIfPossibleAndRequired()
        case (nextState, Some(element)) ⇒
          downstream.onNext(element)
          requested -= 1
          state = nextState
          requestOneFromUpstreamIfPossibleAndRequired()
          tryExpanding()
      }
    }

  def requestOneFromUpstreamIfPossibleAndRequired(): Unit =
    if (!elementPendingFromUpstream) {
      val requestFromUpstream =
        try canConsume(state)
        catch {
          case t: Throwable ⇒
            downstream.onError(t)
            upstream.cancel()
            return
        }
      if (requestFromUpstream) {
        elementPendingFromUpstream = true
        upstream.requestMore(1)
      }
    }
}
