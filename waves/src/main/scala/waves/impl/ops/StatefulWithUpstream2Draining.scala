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

abstract class StatefulWithUpstream2Draining(implicit val upstream: Upstream, val downstream: Downstream, ctx: OperationProcessor.Context)
    extends OperationImpl.StatefulWithSecondaryUpstream {

  class DrainingSecondaryUpstream(var requested: Int) extends Behavior {
    override def requestMore(elements: Int) = requested += elements
    override def cancel() = become {
      new Behavior {
        override def secondaryOnSubscribe(upstream2: Upstream) = upstream2.cancel()
        override def secondaryOnComplete() = ()
        override def secondaryOnError(cause: Throwable) = ()
      }
    }
    override def secondaryOnSubscribe(upstream2: Upstream) = {
      become {
        new Behavior {
          override def requestMore(elements: Int) = upstream2.requestMore(elements)
          override def cancel() = upstream2.cancel()
          override def secondaryOnNext(element: Any) = downstream.onNext(element)
          override def secondaryOnComplete() = downstream.onComplete()
          override def secondaryOnError(cause: Throwable) = downstream.onError(cause)
        }
      }
      upstream2.requestMore(requested)
    }
    override def secondaryOnComplete() = downstream.onComplete()
    override def secondaryOnError(cause: Throwable) = downstream.onError(cause)
  }
}