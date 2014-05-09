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

package waves
package impl
package ops

import org.reactivestreams.api.Producer

class Merge(_secondary: Producer[Any])(implicit _upstream: Upstream, _downstream: Downstream,
                                       _ctx: OperationProcessor.Context) extends StaticFanIn(_secondary) {

  def running(upstream2: Upstream) = new MergeRunningBehavior(upstream2)

  class MergeRunningBehavior(_upstream2: Upstream) extends RunningBehavior(_upstream2) {
    var bufferedElement: Any = NoValue
    var primaryCompleted = false
    var secondaryCompleted = false

    override def requestMore(elements: Int) = {
      requested += elements
      if (requested == elements) {
        if (NoValue != bufferedElement) {
          val next = bufferedElement
          bufferedElement = NoValue
          deliver(next)
          if (requested > 0) requestOneFromBothUpstreams()
        } else requestOneFromBothUpstreams()
      }
    }

    override def onNext(element: Any): Unit =
      if (requested > 0) {
        deliver(element)
        if (requested > 0) upstream.requestMore(1)
      } else bufferedElement = element

    override def secondaryOnNext(element: Any): Unit =
      if (requested > 0) {
        deliver(element)
        if (requested > 0) upstream2.requestMore(1)
      } else bufferedElement = element

    override def onComplete(): Unit =
      if (secondaryCompleted) downstream.onComplete()
      else primaryCompleted = true

    override def secondaryOnComplete(): Unit =
      if (primaryCompleted) downstream.onComplete()
      else secondaryCompleted = true

    def deliver(element: Any): Unit = {
      downstream.onNext(element)
      requested -= 1
    }
  }
}

