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

class Unzip(secondary: Producer[Any] ⇒ Unit)(implicit val upstream: Upstream, val downstream: Downstream,
                                             ctx: OperationProcessor.Context)
    extends OperationImpl.DefaultWithSecondaryDownstream {

  val downstream2 = requestSecondaryDownstream()
  secondary(downstream2)

  var requested1 = 0
  var requested2 = 0

  override def requestMore(elements: Int): Unit = {
    requested1 += elements
    requestMoreIfPossible()
  }

  override def onNext(element: Any): Unit = {
    val tuple = element.asInstanceOf[(Any, Any)]
    downstream.onNext(tuple._1)
    downstream2.onNext(tuple._2)
  }

  override def onComplete(): Unit = {
    downstream.onComplete()
    downstream2.onComplete()
  }

  override def onError(cause: Throwable): Unit = {
    downstream.onError(cause)
    downstream2.onError(cause)
  }

  override def secondaryRequestMore(elements: Int): Unit = {
    requested2 += elements
    requestMoreIfPossible()
  }

  override def secondaryCancel(): Unit = upstream.cancel()

  private def requestMoreIfPossible(): Unit =
    math.min(requested1, requested2) match {
      case 0 ⇒ // nothing to do
      case r ⇒
        requested1 -= r
        requested2 -= r
        upstream.requestMore(r)
    }
}
