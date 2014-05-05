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

package waves.impl
package ops

import org.reactivestreams.api.Producer
import OperationProcessor.SubDownstreamHandling

class ConcatAll(implicit val upstream: Upstream, val downstream: Downstream, ctx: OperationProcessor.Context)
    extends OperationImpl.Stateful {

  var requested = 0
  var upstreamCompleted = false
  val startBehavior = behavior

  def initialBehavior: BehaviorWithSubDownstreamHandling =
    new BehaviorWithSubDownstreamHandling {
      override def requestMore(elements: Int): Unit = {
        requested += elements
        if (requested == elements) upstream.requestMore(1)
      }
      override def onNext(element: Any) =
        element match {
          case producer: Producer[_] ⇒
            become(new WaitingForSubstreamSubscription)
            ctx.requestSubUpstream(producer.asInstanceOf[Producer[Any]], behavior.asInstanceOf[SubDownstreamHandling])
          case _ ⇒ sys.error("Illegal operation setup, expected `Producer[_]` but got " + element)
        }
    }

  class WaitingForSubstreamSubscription extends BehaviorWithSubDownstreamHandling {
    var cancelled = false
    var error: Option[Throwable] = None
    override def requestMore(elements: Int) = requested += elements
    override def cancel() = {
      upstream.cancel()
      cancelled = true
    }
    override def onComplete() = upstreamCompleted = true
    override def onError(cause: Throwable) = error = Some(cause)

    override def subOnSubscribe(subUpstream: Upstream) =
      if (cancelled) {
        subUpstream.cancel()
      } else {
        become(new Draining(subUpstream, error))
        subUpstream.requestMore(requested)
      }
    override def subOnComplete() = finishSubstream(error)
    // else if we were cancelled while waiting for the subscription and the sub stream is empty we are done
    override def subOnError(cause: Throwable) = {
      downstream.onError(cause)
      upstream.cancel()
    }
  }

  // when we enter this state we have already requested `requested` elements from the subUpstream
  class Draining(subUpstream: Upstream, var error: Option[Throwable]) extends BehaviorWithSubDownstreamHandling {
    override def requestMore(elements: Int): Unit = {
      requested += elements
      subUpstream.requestMore(elements)
    }
    override def cancel(): Unit = {
      subUpstream.cancel()
      upstream.cancel()
    }
    override def onComplete(): Unit = upstreamCompleted = true
    override def onError(cause: Throwable): Unit = error = Some(cause)

    override def subOnNext(element: Any): Unit = {
      requested -= 1
      downstream.onNext(element)
    }
    override def subOnComplete(): Unit = finishSubstream(error)
    override def subOnError(cause: Throwable): Unit = {
      upstream.cancel()
      downstream.onError(cause)
    }
  }

  def finishSubstream(error: Option[Throwable]): Unit = {
    if (upstreamCompleted) {
      downstream.onComplete()
    } else if (error.isDefined) {
      downstream.onError(error.get)
    } else {
      become(startBehavior)
      if (requested > 0) upstream.requestMore(1)
    }
  }
}