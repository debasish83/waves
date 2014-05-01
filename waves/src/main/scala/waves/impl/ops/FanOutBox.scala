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
import OperationProcessor.SubUpstreamHandling
import waves.FanOut

class FanOutBox(fanOutProvider: FanOut.Provider[FanOut],
                secondary: Producer[Any] ⇒ Unit)(implicit val upstream: Upstream,
                                                 val downstream: Downstream, ctx: OperationProcessor.Context)
    extends OperationImpl.Abstract with SubUpstreamHandling {

  val secondaryDownstream = ctx.requestSubDownstream(this)
  secondary(secondaryDownstream)

  val fanOut = fanOutProvider(upstream, downstream, secondaryDownstream)

  override def onNext(element: Any): Unit = fanOut.onNext(element)
  override def onComplete(): Unit = fanOut.onComplete()
  override def onError(cause: Throwable): Unit = fanOut.onError(cause)

  override def requestMore(elements: Int): Unit = fanOut.primaryRequestMore(elements)
  override def cancel(): Unit = fanOut.primaryCancel()

  def subRequestMore(elements: Int): Unit = fanOut.secondaryRequestMore(elements)
  def subCancel(): Unit = fanOut.secondaryCancel()
}