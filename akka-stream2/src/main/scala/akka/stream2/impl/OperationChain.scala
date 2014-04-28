package akka.stream2
package impl

import Operation._

// provides outside interfaces around a chain of OperationImpls
private[impl] class OperationChain(op: OperationX, ctx: OperationProcessor.Context) {
  import OperationChain._

  // our Upstream connection to the outside
  private[this] val _leftUpstream = new UpstreamJoint

  // the Downstream interface of the left-most OperationImpl
  private[this] val _leftDownstream = new DownstreamJoint

  // the Upstream interface of the right-most OperationImpl
  private[this] val _rightUpstream = new UpstreamJoint

  // our Downstream connection to the outside
  private[this] val _rightDownstream = new DownstreamJoint

  // before an upstream is connected we need collect potentially outgoing events
  private[this] var leftUpstreamCollector = new CollectingUpstream
  _leftUpstream.arm(leftUpstreamCollector)

  // before a downstream is connected we need collect potentially incoming events
  private[this] var rightDownstreamCollector = new CollectingDownstream
  _rightDownstream.arm(rightDownstreamCollector)

  _leftUpstream.link(_leftDownstream)
  _leftDownstream.link(_leftUpstream)
  _rightUpstream.link(_rightDownstream)
  _rightDownstream.link(_rightUpstream)

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

  def rightUpstream: Upstream = _rightUpstream
  def leftDownstream: Downstream = _leftDownstream

  def isUpstreamCompleted = _leftUpstream.isDisarmed
  def isDownstreamCompleted = _rightDownstream.isDisarmed

  def connectUpstream(upstream: Upstream): Unit =
    if (_leftUpstream.upstream == leftUpstreamCollector) {
      _leftUpstream.arm(upstream)
      leftUpstreamCollector.flushTo(upstream)
      leftUpstreamCollector = null // free memory and signal that we have flushed
    } else if (isUpstreamCompleted && (leftUpstreamCollector ne null)) {
      // the upstream has been cancelled *before* we were even connected, so cancel by flushing
      leftUpstreamCollector.flushTo(upstream)
    } else throw new IllegalStateException(s"Cannot subscribe to second upstream $upstream, already connected to ${_leftUpstream.upstream}")

  def connectDownstream(downstream: Downstream): Unit =
    if (_rightDownstream.downstream == rightDownstreamCollector) {
      _rightDownstream.arm(downstream)
      rightDownstreamCollector.flushTo(downstream)
      rightDownstreamCollector = null // free memory and signal that we have flushed
    } else if (isDownstreamCompleted && (rightDownstreamCollector ne null)) {
      // the upstream has been cancelled *before* we were even connected, so cancel by flushing
      rightDownstreamCollector.flushTo(downstream)
    } else throw new IllegalStateException(s"Cannot attach 2nd downstream $downstream, downstream ${_rightDownstream.downstream} is already attached")
}

private object OperationChain {
  private case object BlackHoleUpstream extends Upstream {
    def requestMore(elements: Int) = ()
    def cancel() = ()
  }
  private class UpstreamJoint extends Upstream {
    private[this] var dsj: DownstreamJoint = _
    var upstream: Upstream = _
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

  private case object BlackHoleDownstream extends Downstream {
    def onNext(element: Any) = ()
    def onComplete() = ()
    def onError(cause: Throwable) = ()
  }
  private class DownstreamJoint extends Downstream {
    private[this] var usj: UpstreamJoint = _
    var downstream: Downstream = _
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

  private class CollectingUpstream extends Upstream {
    private[this] var requested = 0
    private[this] var cancelled = false
    def requestMore(elements: Int) = requested += elements
    def cancel() = cancelled = true
    def flushTo(upstream: Upstream): Unit = {
      if (cancelled) upstream.cancel()
      else if (requested > 0) upstream.requestMore(requested)
    }
  }

  private class CollectingDownstream extends Downstream {
    private[this] var termination: Option[Throwable] = _
    def onNext(element: Any) = throw new IllegalStateException // unrequested onNext
    def onComplete() = termination = None
    def onError(cause: Throwable) = termination = Some(cause)
    def flushTo(downstream: Downstream): Unit = {
      termination match {
        case null        ⇒ // nothing to do
        case None        ⇒ downstream.onComplete()
        case Some(error) ⇒ downstream.onError(error)
      }
    }
  }
}