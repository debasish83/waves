package akka.stream2
package impl
package ops

class Take(count: Int)(implicit val upstream: Upstream, val downstream: Downstream) extends OperationImpl.Abstract {
  require(count > 0)

  var stillToBeRequested = count
  var stillToBeProduced = count

  override def requestMore(elements: Int) =
    if (stillToBeRequested > 0 && stillToBeProduced > 0) {
      val requestNow = math.min(stillToBeRequested, elements)
      stillToBeRequested -= requestNow
      upstream.requestMore(requestNow)
    }

  override def onNext(element: Any) = {
    stillToBeProduced -= 1
    if (stillToBeProduced > 0) {
      downstream.onNext(element)
    } else {
      downstream.onNext(element) // this must not re-enter our `onNext` here, we prevent that in `requestMore`
      downstream.onComplete()
      upstream.cancel()
    }
  }
}