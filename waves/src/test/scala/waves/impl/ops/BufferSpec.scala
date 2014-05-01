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

package waves.impl.ops

import waves.Operation

class BufferSpec extends OperationImplSpec {

  val op = Operation.Buffer[Symbol](4)

  "`Buffer`" should {

    "request and store`size - 1` elements immediately after start" in test(op) { fixture ⇒
      import fixture._
      expectRequestMore(3)
      onNext('a, 'b', 'c)
    }

    "deliver the stored elements to downstream without requesting more from upstream" in test(op) { fixture ⇒
      import fixture._
      expectRequestMore(3)
      onNext('a, 'b, 'c)
      requestMore(1)
      expectNext('a)
      expectNoRequestMore()
      requestMore(4)
      expectNext('b, 'c)
      expectRequestMore(3)
    }

    // TODO: complete
  }
}
