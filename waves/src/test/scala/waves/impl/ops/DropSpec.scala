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

class DropSpec extends OperationImplSpec {

  val op = Operation.Drop[Symbol](3)

  "`Drop`" should {
    "add drop-count to very first requestMore from downstream" in test(op) { fixture ⇒
      import fixture._
      requestMore(5)
      expectRequestMore(8)
      requestMore(3)
      expectRequestMore(3)
    }

    "drop the first drop-count elements from upstream" in test(op) { fixture ⇒
      import fixture._
      requestMore(2)
      expectRequestMore(5)
      onNext('A, 'B, 'C, 'D, 'E)
      expectNext('D, 'E)
      requestMore(1)
      expectRequestMore(1)
      onNext('F)
      expectNext('F)
    }

    "propagate cancel" in test(op) { fixture ⇒
      import fixture._
      fixture.cancel()
      expectCancel()
    }

    "propagate complete" in test(op) { fixture ⇒
      import fixture._
      onComplete()
      expectComplete()
    }

    "propagate error" in test(op) { fixture ⇒
      import fixture._
      onError(TestException)
      expectError(TestException)
    }
  }
}
