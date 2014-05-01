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

class Multiply(factor: Int)(implicit val upstream: Upstream, val downstream: Downstream)
    extends OperationImpl.Stateful {

  require(factor > 0)

  var requested = 0
  val startBehavior = behavior

  def initialBehavior: Behavior =
    new Behavior {
      override def requestMore(elements: Int): Unit = {
        requested = elements
        become(waitingForNextElement)
        upstream.requestMore(1)
      }
    }

  val waitingForNextElement =
    new Behavior {
      override def requestMore(elements: Int) = requested += elements
      override def onNext(element: Any) = {
        produce(factor)
        @tailrec def produce(remaining: Int): Unit =
          if (requested > 0) {
            if (remaining > 0) {
              requested -= 1
              downstream.onNext(element)
              produce(remaining - 1)
            } else upstream.requestMore(1)
          } else if (remaining > 0) become(new WaitingForRequestMore(element, remaining))
          else become(startBehavior)
      }
    }

  // when we enter this state `requested` is zero
  class WaitingForRequestMore(element: Any, var remaining: Int) extends Behavior {
    var upstreamCompleted = false
    override def requestMore(elements: Int) = {
      requested += elements
      if (requested == elements) produce()
      @tailrec def produce(): Unit =
        if (remaining > 0) {
          if (requested > 0) {
            downstream.onNext(element)
            requested -= 1
            remaining -= 1
            produce()
          } // else break loop and wait for next requestMore
        } else if (upstreamCompleted) {
          downstream.onComplete()
        } else {
          become(waitingForNextElement)
          upstream.requestMore(1)
        }
    }
    override def onComplete() = upstreamCompleted = true
  }
}