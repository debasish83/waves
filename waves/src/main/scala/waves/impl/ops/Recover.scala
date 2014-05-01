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
import scala.util.control.NonFatal

class Recover(f: Throwable ⇒ Seq[Any])(implicit val upstream: Upstream, val downstream: Downstream)
    extends OperationImpl.Abstract {

  var requested = 0
  var errorOutput: List[Any] = _

  override def requestMore(elements: Int): Unit = {
    requested += elements
    if (errorOutput eq null) upstream.requestMore(requested)
    else if (requested == elements) drainErrorOutput()
  }

  override def onNext(element: Any): Unit = {
    downstream.onNext(element)
    requested -= 1
  }

  override def onError(cause: Throwable): Unit =
    try {
      errorOutput = f(cause).toList
      drainErrorOutput()
    } catch {
      case NonFatal(e) ⇒ downstream.onError(e)
    }

  @tailrec private def drainErrorOutput(): Unit =
    if (errorOutput.isEmpty) downstream.onComplete()
    else if (requested > 0) {
      downstream.onNext(errorOutput.head)
      requested -= 1
      errorOutput = errorOutput.tail
      drainErrorOutput()
    } // else wait for requestMore
}