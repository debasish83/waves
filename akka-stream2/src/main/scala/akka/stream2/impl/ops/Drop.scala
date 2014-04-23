package akka.stream2
package impl
package ops

class Drop(count: Int)(implicit val upstream: Upstream, val downstream: Downstream) extends OperationImpl.Abstract {

  require(count >= 0)

  var stillToBeDropped = count
  var firstRequest = true

  override def requestMore(elements: Int): Unit =
    upstream.requestMore {
      if (firstRequest) {
        firstRequest = false
        elements + stillToBeDropped
      } else elements
    }

  override def onNext(element: Any) =
    if (stillToBeDropped > 0) stillToBeDropped -= 1
    else downstream.onNext(element)
}