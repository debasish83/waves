package akka.stream2
package impl
package ops

class Filter(f: Any ⇒ Boolean)(implicit val upstream: Upstream, val downstream: Downstream)
  extends OperationImpl.Abstract {

  override def onNext(element: Any): Unit = {
    val pass =
      try f(element)
      catch {
        case t: Throwable ⇒
          downstream.onError(t)
          upstream.cancel()
          return
      }
    if (pass) downstream.onNext(element)
    else upstream.requestMore(1)
  }
}