package akka.stream2.impl
package ops

import scala.util.control.NonFatal

class OnElement(callback: Any ⇒ Unit)(implicit val upstream: Upstream, val downstream: Downstream)
  extends OperationImpl.Abstract {

  override def onNext(element: Any): Unit =
    try {
      callback(element)
      downstream.onNext(element)
    } catch {
      case NonFatal(e) ⇒ downstream.onError(e)
    }
}