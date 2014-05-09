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

class Concat(next: () ⇒ Producer[Any])(implicit us: Upstream, ds: Downstream, ctx: OperationProcessor.Context)
    extends StatefulWithUpstream2Draining {

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
      override def onComplete(): Unit = {
        become(new DrainingSecondaryUpstream(requested))
        try requestSecondaryUpstream(next())
        catch {
          case NonFatal(e) ⇒ downstream.onError(e)
        }
      }
    }
}