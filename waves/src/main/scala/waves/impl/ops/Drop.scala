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

class Drop(count: Int)(implicit val upstream: Upstream, val downstream: Downstream) extends OperationImpl.Abstract {

  require(count >= 0)

  var stillToBeDropped = count
  var firstRequest = true

  override def requestMore(elements: Int): Unit =
    upstream.requestMore {
      if (firstRequest) {
        firstRequest = false
        elements + stillToBeDropped
      } else elements
    }

  override def onNext(element: Any) =
    if (stillToBeDropped > 0) stillToBeDropped -= 1
    else downstream.onNext(element)
}