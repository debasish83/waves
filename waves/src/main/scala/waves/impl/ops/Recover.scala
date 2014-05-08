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

import scala.util.control.NonFatal
import org.reactivestreams.api.Producer

class Recover(pf: PartialFunction[Throwable, Producer[Any]])(implicit val upstream: Upstream,
                                                             val downstream: Downstream,
                                                             ctx: OperationProcessor.Context)
    extends OperationImpl.StatefulWithSecondaryUpstream with (Throwable ⇒ Nothing) {

  def apply(error: Throwable): Nothing = throw error

  def initialBehavior: Behavior =
    new Behavior {
      var requested = 0
      override def requestMore(elements: Int): Unit = {
        requested += elements
        upstream.requestMore(elements)
      }
      override def onNext(element: Any) = {
        requested -= 1
        downstream.onNext(element)
      }
      override def onError(cause: Throwable): Unit = {
        become(new WaitingForSecondFlowSubscription(requested))
        try {
          val producer = pf.applyOrElse(cause, Recover.this)
          requestSecondaryUpstream(producer)
        } catch {
          case NonFatal(e) ⇒ downstream.onError(e)
        }
      }
    }

  class WaitingForSecondFlowSubscription(var requested: Int) extends Behavior {
    var cancelled = false
    override def requestMore(elements: Int) = requested += elements
    override def cancel() = cancelled = true
    override def secondaryOnSubscribe(upstream2: Upstream) =
      if (cancelled) {
        upstream2.cancel()
      } else {
        become(draining(upstream2))
        upstream2.requestMore(requested)
      }
    override def secondaryOnComplete() = downstream.onComplete()
    override def secondaryOnError(cause: Throwable) = downstream.onError(cause)
  }

  // when we enter this state we have already requested all so far requested elements from upstream2
  def draining(upstream2: Upstream): Behavior =
    new Behavior {
      override def requestMore(elements: Int) = upstream2.requestMore(elements)
      override def cancel() = upstream2.cancel()
      override def secondaryOnNext(element: Any) = downstream.onNext(element)
      override def secondaryOnComplete() = downstream.onComplete()
      override def secondaryOnError(cause: Throwable) = downstream.onError(cause)
    }
}