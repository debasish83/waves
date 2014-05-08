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
import waves.impl.OperationProcessor.SubUpstreamHandling

class Tee(secondary: Producer[Any] ⇒ Unit)(implicit val upstream: Upstream, val downstream: Downstream,
                                           ctx: OperationProcessor.Context)
    extends OperationImpl.Abstract with SubUpstreamHandling {

  val downstream2 = ctx.requestSubDownstream(this)
  secondary(downstream2)

  var requested1 = 0
  var requested2 = 0
  var cancelled1 = false
  var cancelled2 = false

  override def onNext(element: Any): Unit = {
    if (!cancelled1) downstream.onNext(element)
    if (!cancelled2) downstream2.onNext(element)
  }

  override def onComplete(): Unit = {
    if (!cancelled1) downstream.onComplete()
    if (!cancelled2) downstream2.onComplete()
  }

  override def onError(cause: Throwable): Unit = {
    if (!cancelled1) downstream.onError(cause)
    if (!cancelled2) downstream2.onError(cause)
  }

  override def requestMore(elements: Int): Unit = {
    requested1 += elements
    requestMoreIfPossible()
  }

  override def cancel(): Unit = {
    cancelled1 = true
    if (cancelled2) upstream.cancel()
    else requestMoreIfPossible()
  }

  def subRequestMore(elements: Int): Unit = {
    requested2 += elements
    requestMoreIfPossible()
  }

  def subCancel(): Unit = {
    cancelled2 = true
    if (cancelled1) upstream.cancel()
    else requestMoreIfPossible()
  }

  private def requestMoreIfPossible(): Unit =
    if (cancelled1) {
      if (requested2 > 0) {
        val r = requested2
        requested2 = 0
        upstream.requestMore(r)
      }
    } else if (cancelled2) {
      if (requested1 > 0) {
        val r = requested1
        requested1 = 0
        upstream.requestMore(r)
      }
    } else {
      math.min(requested1, requested2) match {
        case 0 ⇒ // nothing to do
        case r ⇒
          requested1 -= r
          requested2 -= r
          upstream.requestMore(r)
      }
    }
}
