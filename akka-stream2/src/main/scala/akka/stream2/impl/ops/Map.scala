package akka.stream2
package impl
package ops

class Map(f: Any ⇒ Any)(implicit val upstream: Upstream, val downstream: Downstream)
  extends OperationImpl.Abstract {

  override def onNext(element: Any): Unit = {
    val mappedElement =
      try f(element)
      catch {
        case t: Throwable ⇒
          downstream.onError(t)
          upstream.cancel()
          return
      }
    downstream.onNext(mappedElement)
  }
}
