package akka.stream2
package impl

import org.reactivestreams.api.Producer
import OperationProcessor.{ SubUpstreamHandling, SubDownstreamHandling }

trait OperationImpl extends Downstream with Upstream

object OperationImpl {
  // base class with default implementations that simply forward through the chain,
  // is allowed to synchronously call its upstream and/or downstream peers with one exception:
  // in a call to `downstream.onNext` the `requestMore` method MUST NOT re-enter into `downstream.onNext`
  // (but somehow collect the request element count for later evaluation)
  abstract class Abstract extends OperationImpl {
    def upstream: Upstream
    def downstream: Downstream
    def onNext(element: Any): Unit = throw new IllegalStateException(s"Unrequested `onNext($element)` in $this")
    def onComplete() = downstream.onComplete()
    def onError(cause: Throwable) = downstream.onError(cause)
    def requestMore(elements: Int) = upstream.requestMore(elements)
    def cancel() = upstream.cancel()
  }

  // OperationImpl base class sporting `become`
  abstract class Stateful extends OperationImpl { outer ⇒
    def upstream: Upstream
    def downstream: Downstream
    def initialBehavior: OperationImpl // cannot be implemented with a val due to initialization order!
    var behavior: OperationImpl = initialBehavior
    def become(next: OperationImpl): Unit = behavior = next

    def onNext(element: Any) = behavior.onNext(element)
    def onComplete() = behavior.onComplete()
    def onError(cause: Throwable) = behavior.onError(cause)
    def requestMore(elements: Int) = behavior.requestMore(elements)
    def cancel() = behavior.cancel()

    // helper classes for easier stateful behavior definition
    class Behavior extends Abstract {
      def upstream = outer.upstream
      def downstream = outer.downstream
    }

    class BehaviorWithSubDownstreamHandling extends Behavior with SubDownstreamHandling {
      def subOnSubscribe(subUpstream: Upstream): Unit = throw new IllegalStateException(s"Unexpected `subOnSubscribe($subUpstream)` in $this")
      def subOnNext(element: Any): Unit = throw new IllegalStateException(s"Unexpected `subOnNext($element)` in $this")
      def subOnComplete(): Unit = throw new IllegalStateException("Unexpected `subOnComplete` in $this")
      def subOnError(cause: Throwable): Unit = throw new IllegalStateException(s"Unexpected `subOnError($cause)` in $this")
    }

    class BehaviorWithSubUpstreamHandling extends Behavior with SubUpstreamHandling {
      def subRequestMore(elements: Int): Unit = throw new IllegalStateException(s"Unexpected `subRequestMore($elements)` in $this")
      def subCancel(): Unit = throw new IllegalStateException(s"Unexpected `subCancel()` in $this")
    }
  }

  import Operation._
  def apply(op: OperationX)(implicit upstream: Upstream, downstream: Downstream,
                            ctx: OperationProcessor.Context): OperationImpl =
    op.asInstanceOf[Operation[Any, Any]] match {
      case Buffer(size)                ⇒ new ops.Buffer(size)
      case Concat(next)                ⇒ new ops.Concat(next)
      case ConcatAll()                 ⇒ new ops.ConcatAll()
      case CustomBuffer(seed, f, g, h) ⇒ new ops.CustomBuffer(seed, f, g, h)
      case Drop(n)                     ⇒ new ops.Drop(n)
      case FanInBox(sec, fanIn)        ⇒ new ops.FanInBox(sec, fanIn)
      case FanOutBox(fanOut, sec)      ⇒ new ops.FanOutBox(fanOut.asInstanceOf[FanOut.Provider[FanOut]], sec.asInstanceOf[Producer[Any] ⇒ Unit])
      case Filter(f)                   ⇒ new ops.Filter(f)
      case Fold(seed, f)               ⇒ new ops.Fold(seed, f)
      case Map(f)                      ⇒ new ops.Map(f)
      case Multiply(factor)            ⇒ new ops.Multiply(factor)
      case OnEvent(callback)           ⇒ new ops.OnEvent(callback)
      case Recover(f)                  ⇒ new ops.Recover(f)
      case Transform(transformer)      ⇒ new ops.Transform(transformer)
      case Split(f)                    ⇒ new ops.Split(f)
      case Take(n)                     ⇒ new ops.Take(n)
    }
}