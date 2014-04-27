package akka.stream2.impl
package ops

import scala.util.control.NonFatal

class OnTerminate(callback: Option[Throwable] ⇒ Unit)(implicit val upstream: Upstream, val downstream: Downstream)
  extends OperationImpl.Abstract {

  override def onComplete(): Unit =
    try {
      callback(None)
      downstream.onComplete()
    } catch {
      case NonFatal(e) ⇒ downstream.onError(e)
    }

  override def onError(cause: Throwable): Unit =
    try {
      callback(Some(cause))
      downstream.onError(cause)
    } catch {
      case NonFatal(e) ⇒ downstream.onError(e)
    }
}