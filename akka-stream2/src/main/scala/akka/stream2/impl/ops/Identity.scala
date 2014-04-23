package akka.stream2
package impl
package ops

class Identity(val upstream: Upstream, val downstream: Downstream) extends OperationImpl.Abstract {

  override def onNext(element: Any) = downstream.onNext(element)

}
