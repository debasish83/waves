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

class TakeSpec extends OperationImplSpec {

  val op = Operation.Take[Char](3)

  "`Take`" should {
    "propagate requestMore directly if requested < n" in test(op) { fixture ⇒
      import fixture._
      requestMore(2)
      expectRequestMore(2)
    }

    "propagate requestMore directly if requested == n" in test(op) { fixture ⇒
      import fixture._
      requestMore(3)
      expectRequestMore(3)
    }

    "requestMore(n) from upstream the requested count from downstream is > n" in test(op) { fixture ⇒
      import fixture._
      requestMore(4)
      expectRequestMore(3)
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

    "propagate the first n elements from upstream, then complete downstream and cancel upstream" in test(op) { fixture ⇒
      import fixture._
      requestMore(2)
      expectRequestMore(2)
      onNext('A', 'B')
      expectNext('A', 'B')
      expectNoRequestMore()
      requestMore(3)
      expectRequestMore(1)
      onNext('C')
      expectNext('C')
      expectComplete()
      expectCancel()
    }
  }
}