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

import scala.annotation.tailrec

class CustomBuffer(seed: Any,
                   compress: (Any, Any) ⇒ Any,
                   expand: Any ⇒ (Any, Option[Any]),
                   canConsume: Any ⇒ Boolean)(implicit val upstream: Upstream, val downstream: Downstream)
    extends OperationImpl.Default {

  var requested = 0
  var state = seed
  var elementPendingFromUpstream = false

  override def requestMore(elements: Int): Unit = {
    requested += elements
    if (requested == elements) tryExpanding()
  }

  override def onNext(element: Any): Unit = {
    elementPendingFromUpstream = false
    state =
      try compress(state, element)
      catch {
        case t: Throwable ⇒
          downstream.onError(t)
          upstream.cancel()
          return
      }
    if (requested > 0) tryExpanding()
    else requestOneFromUpstreamIfPossibleAndRequired()
  }

  @tailrec private def tryExpanding(): Unit =
    if (requested > 0) {
      val expandResult =
        try expand(state)
        catch {
          case t: Throwable ⇒
            downstream.onError(t)
            upstream.cancel()
            return
        }
      expandResult match {
        case (nextState, None) ⇒
          state = nextState
          requestOneFromUpstreamIfPossibleAndRequired()
        case (nextState, Some(element)) ⇒
          downstream.onNext(element)
          requested -= 1
          state = nextState
          requestOneFromUpstreamIfPossibleAndRequired()
          tryExpanding()
      }
    }

  def requestOneFromUpstreamIfPossibleAndRequired(): Unit =
    if (!elementPendingFromUpstream) {
      val requestFromUpstream =
        try canConsume(state)
        catch {
          case t: Throwable ⇒
            downstream.onError(t)
            upstream.cancel()
            return
        }
      if (requestFromUpstream) {
        elementPendingFromUpstream = true
        upstream.requestMore(1)
      }
    }
}
