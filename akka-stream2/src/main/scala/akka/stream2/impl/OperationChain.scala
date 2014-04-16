package akka.stream2
package impl

import Operation._

// provides outside interfaces around a chain of OperationImpls
class OperationChain(op: OperationX,
                     outerUpstream: Upstream, outerDownstream: Downstream, ctx: OperationProcessor.Context) {

  private[this] var finished = false

  // the Upstream side of the right-most OperationImpl
  private[this] var rightUpstream: Upstream = _

  // wrapper around the `outerDownstream`, required because we need to get notified of completion
  private[this] val rightDownstream: Downstream =
    new Downstream {
      def onNext(element: Any): Unit = outerDownstream.onNext(element)
      def onComplete(): Unit = {
        finished = true
        outerDownstream.onComplete()
      }
      def onError(cause: Throwable): Unit = {
        finished = true
        outerDownstream.onError(cause)
      }
    }

  // our Upstream connection to the outside
  private[this] def leftUpstream = outerUpstream

  // the Downstream side of the left-most OperationImpl
  private[this] var leftDownstream: Downstream = _

  // here we convert the model Operation into a double-linked chain of OperationImpls,
  // the left-to-right direction (going downstream) is directly wired up,
  // the right-to-left direction (going upstream) has `UpstreamJoint` instances inserted between the ops
  convertToImplAndWireUp(op, leftUpstream, rightDownstream)
  private def convertToImplAndWireUp(op: OperationX, us: Upstream, ds: Downstream): Unit =
    op match {
      case head ~> tail ⇒
        val joint = new UpstreamJoint
        convertToImplAndWireUp(tail, joint, ds)
        val tailRightUpstream = rightUpstream
        convertToImplAndWireUp(head, us, leftDownstream)
        joint.upstream = rightUpstream
        rightUpstream = tailRightUpstream
      case _ ⇒
        val opImpl = OperationImpl(op)(us, ds, ctx)
        leftDownstream = opImpl
        rightUpstream = opImpl
    }

  // the upstream interface we present to the outside
  def cancel(): Unit = rightUpstream.cancel()
  def requestMore(elements: Int): Unit =
    if (!finished && elements > 0) rightUpstream.requestMore(elements)

  // the downstream interface we present to the outside
  def onNext(element: Any): Unit = leftDownstream.onNext(element)
  def onComplete(): Unit = leftDownstream.onComplete()
  def onError(cause: Throwable): Unit = leftDownstream.onError(cause)

  class UpstreamJoint extends Upstream {
    var upstream: Upstream = _
    def requestMore(elements: Int): Unit = upstream.requestMore(elements)
    def cancel(): Unit = upstream.cancel()
  }
}