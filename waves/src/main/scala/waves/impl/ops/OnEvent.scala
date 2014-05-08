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
import waves.Operation.StreamEvent

class OnEvent(callback: StreamEvent[Any] ⇒ Unit)(implicit val upstream: Upstream, val downstream: Downstream)
    extends OperationImpl.Default {

  override def requestMore(elements: Int) = {
    dispatch(StreamEvent.RequestMore(elements))
    upstream.requestMore(elements)
  }

  override def cancel() = {
    dispatch(StreamEvent.Cancel)
    upstream.cancel()
  }

  override def onNext(element: Any) = {
    dispatch(StreamEvent.OnNext(element))
    downstream.onNext(element)
  }

  override def onComplete() = {
    dispatch(StreamEvent.OnComplete)
    downstream.onComplete()
  }

  override def onError(cause: Throwable) = {
    dispatch(StreamEvent.OnError(cause))
    downstream.onError(cause)
  }

  private def dispatch(ev: StreamEvent[Any]): Unit =
    try callback(ev)
    catch {
      case NonFatal(e) ⇒ downstream.onError(e)
    }
}