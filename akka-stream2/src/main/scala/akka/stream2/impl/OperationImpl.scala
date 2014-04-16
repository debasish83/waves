package akka.stream2
package impl

import org.reactivestreams.spi.Subscription
import OperationProcessor.{ SubUpstreamHandling, SubDownstreamHandling }

// same as `Subscriber[T]`, but untyped and without `onSubscribe` and the "must be async" semantics
trait Downstream {
  def onNext(element: Any): Unit
  def onComplete(): Unit
  def onError(cause: Throwable): Unit
}

// same as `Subscription` TODO: consider switching to a type alias
trait Upstream {
  def requestMore(elements: Int): Unit
  def cancel(): Unit
}

object Upstream {
  def apply(subscription: Subscription): Upstream =
    new Upstream {
      def requestMore(elements: Int): Unit = subscription.requestMore(elements)
      def cancel(): Unit = subscription.cancel()
    }

}

trait OperationImpl extends Downstream with Upstream

object OperationImpl {
  // base class with default implementations that simply forward through the chain
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
    def become(next: Terminated.type): Unit = behavior = next.asInstanceOf[OperationImpl]

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
      def subOnSubscribe(subUpstream: Upstream): Unit = throw new IllegalStateException(s"Unexpected `subOnSubscribe($subUpstream)`")
      def subOnNext(element: Any): Unit = throw new IllegalStateException(s"Unexpected `subOnNext($element)`")
      def subOnComplete(): Unit = throw new IllegalStateException("Unexpected `subOnComplete`")
      def subOnError(cause: Throwable): Unit = throw new IllegalStateException(s"Unexpected `subOnError($cause)`")
    }

    class BehaviorWithSubUpstreamHandling extends Behavior with SubUpstreamHandling {
      def subRequestMore(elements: Int): Unit = throw new IllegalStateException(s"Unexpected `subRequestMore($elements)`")
      def subCancel(): Unit = throw new IllegalStateException(s"Unexpected `subCancel()`")
    }
  }

  // terminal behavior making it easier to detect bugs in other operation impls that do not correctly adhere to
  // the communication protocol, could potentially be removed after operation impls have stabilized
  object Terminated extends OperationImpl with SubUpstreamHandling with SubDownstreamHandling {
    def onNext(element: Any) = throw new IllegalStateException(s"`onNext($element)` called after termination")
    def onComplete() = throw new IllegalStateException("`onComplete()` called after termination")
    def onError(cause: Throwable) = throw new IllegalStateException(s"`onError($cause)` called after termination")
    def requestMore(elements: Int) = throw new IllegalStateException(s"`requestMore($elements)` called after termination")
    def cancel() = throw new IllegalStateException("`cancel()` called after termination")
    def subOnSubscribe(subUpstream: Upstream): Unit = throw new IllegalStateException(s"`subOnSubscribe($subUpstream)` called after termination")
    def subOnNext(element: Any) = throw new IllegalStateException(s"`subOnNext($element)` called after termination")
    def subOnComplete() = throw new IllegalStateException("`subOnComplete()` called after termination")
    def subOnError(cause: Throwable) = throw new IllegalStateException(s"`subOnError($cause)` called after termination")
    def subRequestMore(elements: Int) = throw new IllegalStateException(s"`subRequestMore($elements)` called after termination")
    def subCancel() = throw new IllegalStateException("`subCancel()` called after termination")
  }

  import Operation._
  def apply(op: OperationX)(implicit upstream: Upstream, downstream: Downstream,
                            ctx: OperationProcessor.Context): OperationImpl =
    op.asInstanceOf[Operation[Any, Any]] match {
      case Concat(source)         ⇒ new ops.Concat(source)
      case Buffer(seed, f, g, h)  ⇒ new ops.Buffer(seed, f, g, h)
      case Drop(n)                ⇒ new ops.Drop(n)
      case Filter(f)              ⇒ new ops.Filter(f)
      case Fold(seed, f)          ⇒ new ops.Fold(seed, f)
      case Map(f)                 ⇒ new ops.Map(f)
      case Multiply(factor)       ⇒ new ops.Multiply(factor)
      case Transform(transformer) ⇒ new ops.Transform(transformer)
      case Split(f)               ⇒ new ops.Split(f)
      case Take(n)                ⇒ new ops.Take(n)
      case _ ⇒ op match {
        // unfortunately, due to type inference issues, we don't seem to be able to add these to the main match directly
        case Flatten() ⇒ new ops.Flatten
      }
    }
}