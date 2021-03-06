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

import org.reactivestreams.api.{ Consumer, Producer }
import org.reactivestreams.spi.Publisher

private[waves] trait AbstractProducer[T] extends Producer[T] with Publisher[T] {
  def getPublisher: Publisher[T] = this
  def produceTo(consumer: Consumer[T]) = subscribe(consumer.getSubscriber)
}
