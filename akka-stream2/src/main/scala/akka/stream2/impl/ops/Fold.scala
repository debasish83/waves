package akka.stream2
package impl
package ops

class Fold(seed: Any, f: (Any, Any) ⇒ Any)(implicit val upstream: Upstream, val downstream: Downstream)
  extends OperationImpl.Abstract {

  var acc = seed

  override def onNext(element: Any): Unit = {
    acc =
      try f(acc, element)
      catch {
        case t: Throwable ⇒
          downstream.onError(t)
          upstream.cancel()
          return
      }
    upstream.requestMore(1)
  }

  override def onComplete() = {
    downstream.onNext(acc)
    downstream.onComplete()
  }
}
