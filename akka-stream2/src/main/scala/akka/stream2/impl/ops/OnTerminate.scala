package akka.stream2
package impl
package ops

class OnTerminate(callback: Option[Throwable] ⇒ Any)(implicit val upstream: Upstream, val downstream: Downstream)
  extends OperationImpl.Abstract {

  override def onComplete(): Unit = {
    callback(None)
    downstream.onComplete()
  }

  override def onError(cause: Throwable): Unit = {
    callback(Some(cause))
    downstream.onError(cause)
  }
}