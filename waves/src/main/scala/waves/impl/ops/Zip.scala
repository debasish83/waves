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

class Zip(_secondary: Producer[Any])(implicit _upstream: Upstream, _downstream: Downstream,
                                     _ctx: OperationProcessor.Context) extends StaticFanIn(_secondary) {

  def running(upstream2: Upstream) = new RunningBehavior(upstream2) {
    var primaryElement: Any = NoValue
    var secondaryElement: Any = NoValue

    override def requestMore(elements: Int) = {
      requested += elements
      if (requested == elements) requestOneFromBothUpstreams()
    }

    override def onNext(element: Any): Unit =
      if (NoValue != secondaryElement) {
        val tuple = (element, secondaryElement)
        secondaryElement = NoValue
        deliver(tuple)
      } else primaryElement = element

    override def secondaryOnNext(element: Any): Unit =
      if (NoValue != primaryElement) {
        val tuple = (primaryElement, element)
        primaryElement = NoValue
        deliver(tuple)
      } else secondaryElement = element

    override def onComplete(): Unit =
      if (NoValue != primaryElement) {
        become {
          new Behavior {
            override def secondaryOnNext(element: Any) = {
              upstream2.cancel()
              downstream.onNext(primaryElement -> element)
              downstream.onComplete()
            }
            override def secondaryOnComplete() = downstream.onComplete()
            override def secondaryOnError(cause: Throwable) = downstream.onError(cause)
          }
        }
      } else {
        downstream.onComplete()
        upstream2.cancel()
      }

    override def secondaryOnComplete(): Unit =
      if (NoValue != secondaryElement) {
        become {
          new Behavior {
            override def onNext(element: Any) = {
              upstream.cancel()
              downstream.onNext(element -> secondaryElement)
              downstream.onComplete()
            }
          }
        }
      } else {
        downstream.onComplete()
        upstream.cancel()
      }

    def deliver(element: Any): Unit = {
      downstream.onNext(element)
      requested -= 1
      if (requested > 0) requestOneFromBothUpstreams()
    }
  }
}