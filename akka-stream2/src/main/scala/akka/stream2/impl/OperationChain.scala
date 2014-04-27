package akka.stream2
package impl

import Operation._

// provides outside interfaces around a chain of OperationImpls
class OperationChain(op: OperationX,
                     outerUpstream: Upstream, outerDownstream: Downstream, ctx: OperationProcessor.Context) {
  import OperationChain._

  // the Upstream interface of the right-most OperationImpl
  private[this] val _rightUpstream = new UpstreamJoint

  // our Downstream connection to the outside
  private[this] val _rightDownstream = new DownstreamJoint
  _rightDownstream.arm(outerDownstream)

  // our Upstream connection to the outside
  private[this] val _leftUpstream = new UpstreamJoint
  _leftUpstream.arm(outerUpstream)

  // the Downstream interface of the left-most OperationImpl
  private[this] val _leftDownstream = new DownstreamJoint

  _rightUpstream.link(_rightDownstream)
  _rightDownstream.link(_rightUpstream)
  _leftUpstream.link(_leftDownstream)
  _leftDownstream.link(_leftUpstream)

  // here we convert the model Operation into a double-linked chain of OperationImpls,
  // in the left-to-right direction (going downstream) the ops are separated by DownstreamJoints
  // in the right-to-left direction (going upstream) the ops are separated by UpstreamJoints
  convertToImplAndWireUp(op, _leftUpstream, _leftDownstream, _rightDownstream, _rightUpstream)
  private def convertToImplAndWireUp(op: OperationX, us: Upstream, leftDownstreamJoint: DownstreamJoint,
                                     ds: Downstream, rightUpstreamJoint: UpstreamJoint): Unit =
    op match {
      case head ~> tail ⇒
        val usj = new UpstreamJoint
        val dsj = new DownstreamJoint
        usj.link(dsj)
        dsj.link(usj)
        convertToImplAndWireUp(head, us, leftDownstreamJoint, dsj, usj)
        convertToImplAndWireUp(tail, usj, dsj, ds, rightUpstreamJoint)
      case _ ⇒
        val opImpl = OperationImpl(op)(us, ds, ctx)
        leftDownstreamJoint.arm(opImpl)
        rightUpstreamJoint.arm(opImpl)
    }

  def leftDownstream: Downstream = _leftDownstream
  def rightUpstream: Upstream = _rightUpstream
}

object OperationChain {
  private val BlackHoleUpstream = new Upstream {
    def requestMore(elements: Int) = ()
    def cancel() = ()
  }
  class UpstreamJoint extends Upstream {
    private[this] var upstream: Upstream = _
    private[this] var dsj: DownstreamJoint = _
    def link(dsj: DownstreamJoint) = this.dsj = dsj
    def arm(up: Upstream) = upstream = up
    def disarm() = upstream = BlackHoleUpstream
    def isDisarmed = upstream eq BlackHoleUpstream
    def requestMore(elements: Int) = upstream.requestMore(elements)
    def cancel() = {
      val up = upstream
      disarm()
      dsj.disarm()
      up.cancel()
    }
  }

  private val BlackHoleDownstream = new Downstream {
    def onNext(element: Any) = ()
    def onComplete() = ()
    def onError(cause: Throwable) = ()
  }
  class DownstreamJoint extends Downstream {
    private[this] var downstream: Downstream = _
    private[this] var usj: UpstreamJoint = _
    def link(usj: UpstreamJoint) = this.usj = usj
    def arm(down: Downstream) = downstream = down
    def disarm() = downstream = BlackHoleDownstream
    def isDisarmed = downstream eq BlackHoleDownstream
    def onNext(element: Any) = downstream.onNext(element)
    def onComplete() = {
      val down = downstream
      disarm()
      usj.disarm()
      down.onComplete()
    }
    def onError(cause: Throwable) = {
      val down = downstream
      disarm()
      usj.disarm()
      down.onError(cause)
    }
  }
}