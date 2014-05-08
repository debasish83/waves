/*
 * Copyright (C) 2014 Mathias Doenitz
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package waves
package impl

import org.reactivestreams.api.Producer

private[waves] trait OperationImpl extends Downstream with Upstream

private[waves] object OperationImpl {
  val Placeholder = new AnyRef

  trait SecondaryDownstream {
    def secondaryOnSubscribe(upstream2: Upstream): Unit = throw new IllegalStateException(s"Unexpected `subOnSubscribe($upstream2)` in $this")
    def secondaryOnNext(element: Any): Unit = throw new IllegalStateException(s"Unexpected `subOnNext($element)` in $this")
    def secondaryOnComplete(): Unit = throw new IllegalStateException("Unexpected `subOnComplete` in $this")
    def secondaryOnError(cause: Throwable): Unit = throw new IllegalStateException(s"Unexpected `subOnError($cause)` in $this")
  }

  trait SecondaryUpstream {
    def secondaryRequestMore(elements: Int): Unit = throw new IllegalStateException(s"Unexpected `subRequestMore($elements)` in $this")
    def secondaryCancel(): Unit = throw new IllegalStateException(s"Unexpected `subCancel()` in $this")
  }

  trait WithSecondaryUpstreamBehavior {
    def behavior: SecondaryUpstream
    def requestSecondaryDownstream()(implicit ctx: OperationProcessor.Context): Producer[Any] with Downstream =
      ctx.requestSecondaryDownstream(this)
  }

  trait WithSecondaryDownstreamBehavior {
    def behavior: SecondaryDownstream
    def requestSecondaryUpstream(producer: Producer[Any])(implicit ctx: OperationProcessor.Context): Unit =
      ctx.requestSecondaryUpstream(producer, this)
  }

  // base class with default implementations that simply forward through the chain,
  // is allowed to synchronously call its upstream and/or downstream peers with one exception:
  // in a call to `downstream.onNext` the `requestMore` method MUST NOT re-enter into `downstream.onNext`
  // (but somehow collect the request element count for later evaluation)
  abstract class Default extends OperationImpl {
    def upstream: Upstream
    def downstream: Downstream
    def onNext(element: Any): Unit = throw new IllegalStateException(s"Unrequested `onNext($element)` in $this")
    def onComplete() = downstream.onComplete()
    def onError(cause: Throwable) = downstream.onError(cause)
    def requestMore(elements: Int) = upstream.requestMore(elements)
    def cancel() = upstream.cancel()
  }

  abstract class DefaultWithSecondaryDownstream extends Default with WithSecondaryUpstreamBehavior with SecondaryUpstream {
    def behavior: SecondaryUpstream = this
  }

  abstract class AbstractStateful extends OperationImpl { outer ⇒
    def upstream: Upstream
    def downstream: Downstream
    type Behavior <: AbstractBehavior

    def initialBehavior: Behavior // cannot be implemented with a val due to initialization order!
    private[this] var _behavior: Behavior = initialBehavior
    def behavior: Behavior = _behavior
    def become(next: Behavior): Unit = _behavior = next

    def onNext(element: Any) = behavior.onNext(element)
    def onComplete() = behavior.onComplete()
    def onError(cause: Throwable) = behavior.onError(cause)
    def requestMore(elements: Int) = behavior.requestMore(elements)
    def cancel() = behavior.cancel()

    abstract class AbstractBehavior extends Default {
      def upstream = outer.upstream
      def downstream = outer.downstream
    }
  }

  abstract class Stateful extends AbstractStateful {
    type Behavior = AbstractBehavior
  }

  abstract class StatefulWithSecondaryUpstream extends AbstractStateful with WithSecondaryDownstreamBehavior { outer ⇒
    type Behavior = BehaviorBase
    abstract class BehaviorBase extends AbstractBehavior with SecondaryDownstream
  }

  abstract class StatefulWithSecondaryDownstream extends AbstractStateful with WithSecondaryUpstreamBehavior { outer ⇒
    type Behavior = BehaviorBase
    abstract class BehaviorBase extends AbstractBehavior with SecondaryUpstream
  }

  import Operation._
  def apply(op: OperationX)(implicit upstream: Upstream, downstream: Downstream,
                            ctx: OperationProcessor.Context): OperationImpl =
    op match {
      case Buffer(size)                ⇒ new ops.Buffer(size)
      case Concat(next)                ⇒ new ops.Concat(next)
      case ConcatAll()                 ⇒ new ops.ConcatAll()
      case CustomBuffer(seed, f, g, h) ⇒ new ops.CustomBuffer(seed, f.asInstanceOf[(Any, Any) ⇒ Any], g, h)
      case Drop(n)                     ⇒ new ops.Drop(n)
      case Filter(f)                   ⇒ new ops.Filter(f.asInstanceOf[Any ⇒ Boolean])
      case Fold(seed, f)               ⇒ new ops.Fold(seed, f.asInstanceOf[(Any, Any) ⇒ Any])
      case Map(f)                      ⇒ new ops.Map(f.asInstanceOf[Any ⇒ Any])
      case Merge(secondary)            ⇒ new ops.Merge(secondary.asInstanceOf[Producer[Any]])
      case MergeToEither(secondary)    ⇒ new ops.MergeToEither(secondary.asInstanceOf[Producer[Any]])
      case Multiply(factor)            ⇒ new ops.Multiply(factor)
      case OnEvent(callback)           ⇒ new ops.OnEvent(callback.asInstanceOf[Operation.StreamEvent[Any] ⇒ Unit])
      case Recover(f)                  ⇒ new ops.Recover(f)
      case Split(f)                    ⇒ new ops.Split(f.asInstanceOf[Any ⇒ Operation.Split.Command])
      case Take(n)                     ⇒ new ops.Take(n)
      case Tee(secondary)              ⇒ new ops.Tee(secondary)
      case Transform(transformer)      ⇒ new ops.Transform(transformer.asInstanceOf[Operation.Transformer[Any, Any]])
      case Unzip(secondary)            ⇒ new ops.Unzip(secondary)
      case Zip(secondary)              ⇒ new ops.Zip(secondary)
      case x                           ⇒ sys.error(s"Operation $x is not yet implemented")
    }
}