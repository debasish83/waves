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

import org.reactivestreams.spi.Subscription

package object impl {
  private[waves]type ==>[-I, +O] = Operation[I, O]

  // shorten entry point into operations DSL
  def operation[T]: Operation.Identity[T] = Operation.Identity()

  type Upstream = Subscription // the interface is identical, should we nevertheless use a separate type?
}

package impl {
  // same as `Subscriber[T]`, but untyped and without `onSubscribe` and the "must be async" semantics
  trait Downstream {
    def onNext(element: Any): Unit
    def onComplete(): Unit
    def onError(cause: Throwable): Unit
  }
}
