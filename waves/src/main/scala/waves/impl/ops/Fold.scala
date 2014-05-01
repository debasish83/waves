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

class Fold(seed: Any, f: (Any, Any) ⇒ Any)(implicit val upstream: Upstream, val downstream: Downstream)
    extends OperationImpl.Abstract {

  var acc = seed

  override def requestMore(elements: Int) = upstream.requestMore(1)

  override def onNext(element: Any): Unit = {
    acc =
      try f(acc, element)
      catch {
        case t: Throwable ⇒
          downstream.onError(t)
          upstream.cancel()
          return
      }
    upstream.requestMore(1)
  }

  override def onComplete() = {
    downstream.onNext(acc)
    downstream.onComplete()
  }
}
