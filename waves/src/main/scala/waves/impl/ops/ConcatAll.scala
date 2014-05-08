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

class ConcatAll(implicit val upstream: Upstream, val downstream: Downstream, ctx: OperationProcessor.Context)
    extends OperationImpl.StatefulWithSecondaryUpstream {

  var requested = 0
  var upstreamCompleted = false
  val startBehavior = behavior

  def initialBehavior: Behavior =
    new Behavior {
      override def requestMore(elements: Int): Unit = {
        requested += elements
        if (requested == elements) upstream.requestMore(1)
      }
      override def onNext(element: Any) =
        element match {
          case producer: Producer[_] ⇒
            become(waitingForSubstreamSubscription)
            requestSecondaryUpstream(producer.asInstanceOf[Producer[Any]])
          case _ ⇒ sys.error("Illegal operation setup, expected `Producer[_]` but got " + element)
        }
    }

  def waitingForSubstreamSubscription: Behavior =
    new Behavior {
      var cancelled = false
      var error: Option[Throwable] = None
      override def requestMore(elements: Int) = requested += elements
      override def cancel() = {
        upstream.cancel()
        cancelled = true
      }
      override def onComplete() = upstreamCompleted = true
      override def onError(cause: Throwable) = error = Some(cause)

      override def secondaryOnSubscribe(upstream2: Upstream) =
        if (cancelled) {
          upstream2.cancel()
        } else {
          become(new Draining(upstream2, error))
          upstream2.requestMore(requested)
        }
      override def secondaryOnComplete() = finishSubstream(error)
      // else if we were cancelled while waiting for the subscription and the sub stream is empty we are done
      override def secondaryOnError(cause: Throwable) = {
        downstream.onError(cause)
        upstream.cancel()
      }
    }

  // when we enter this state we have already requested `requested` elements from the subUpstream
  class Draining(upstream2: Upstream, var error: Option[Throwable]) extends Behavior {
    override def requestMore(elements: Int): Unit = {
      requested += elements
      upstream2.requestMore(elements)
    }
    override def cancel(): Unit = {
      upstream2.cancel()
      upstream.cancel()
    }
    override def onComplete(): Unit = upstreamCompleted = true
    override def onError(cause: Throwable): Unit = error = Some(cause)

    override def secondaryOnNext(element: Any): Unit = {
      requested -= 1
      downstream.onNext(element)
    }
    override def secondaryOnComplete(): Unit = finishSubstream(error)
    override def secondaryOnError(cause: Throwable): Unit = {
      upstream.cancel()
      downstream.onError(cause)
    }
  }

  def finishSubstream(error: Option[Throwable]): Unit =
    if (upstreamCompleted) {
      downstream.onComplete()
    } else if (error.isDefined) {
      downstream.onError(error.get)
    } else {
      become(startBehavior)
      if (requested > 0) upstream.requestMore(1)
    }
}