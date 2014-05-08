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

class Zip(secondary: Producer[Any])(implicit val upstream: Upstream, val downstream: Downstream,
                                    ctx: OperationProcessor.Context) extends OperationImpl.StatefulWithSecondaryUpstream {
  import Zip.Empty

  requestSecondaryUpstream(secondary)

  var requested = 0

  def initialBehavior: Behavior =
    new Behavior {
      override def requestMore(elements: Int) = requested += elements
      override def cancel() = {
        upstream.cancel()
        cancelSecondaryUpstreamUponSubscription()
      }
      override def onComplete() = {
        downstream.onComplete()
        cancelSecondaryUpstreamUponSubscription()
      }
      override def onError(cause: Throwable) = {
        downstream.onError(cause)
        cancelSecondaryUpstreamUponSubscription()
      }
      override def secondaryOnSubscribe(upstream2: Upstream) = {
        become(running(upstream2))
        if (requested > 0) {
          upstream.requestMore(1)
          upstream2.requestMore(1)
        }
      }
      def cancelSecondaryUpstreamUponSubscription(): Unit = become {
        new Behavior {
          override def secondaryOnSubscribe(upstream2: Upstream) = upstream2.cancel()
          override def secondaryOnComplete() = ()
          override def secondaryOnError(cause: Throwable) = ()
        }
      }
    }

  def running(upstream2: Upstream) = new Behavior {
    var primaryElement: Any = Empty
    var secondaryElement: Any = Empty

    override def requestMore(elements: Int) = {
      requested += elements
      if (requested == elements) requestOne()
    }
    override def cancel() = {
      upstream.cancel()
      upstream2.cancel()
    }
    override def onNext(element: Any): Unit =
      if (secondaryElement != Empty) {
        val tuple = (element, secondaryElement)
        secondaryElement = Empty
        deliver(tuple)
      } else primaryElement = element

    override def secondaryOnNext(element: Any): Unit =
      if (primaryElement != Empty) {
        val tuple = (primaryElement, element)
        primaryElement = Empty
        deliver(tuple)
      } else secondaryElement = element

    override def onComplete(): Unit =
      if (primaryElement != Empty) {
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
      if (secondaryElement != Empty) {
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

    override def onError(cause: Throwable): Unit = {
      downstream.onError(cause)
      upstream2.cancel()
    }
    override def secondaryOnError(cause: Throwable): Unit = {
      downstream.onError(cause)
      upstream.cancel()
    }

    def deliver(tuple: (Any, Any)): Unit = {
      downstream.onNext(tuple)
      requested -= 1
      if (requested > 0) requestOne()
    }

    def requestOne(): Unit = {
      upstream.requestMore(1)
      upstream2.requestMore(1)
    }
  }
}

object Zip {
  private val Empty = new AnyRef
}