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

class FoldSpec extends OperationImplSpec {

  val op = Operation.Fold[Char, String]("", _ + _)

  "`Fold`" should {

    "propagate requestMore as requestMore(1)" in test(op) { fixture ⇒
      import fixture._
      requestMore(5)
      expectRequestMore(1)
      requestMore(2)
      expectRequestMore(1)
    }

    "propagate cancel" in test(op) { fixture ⇒
      import fixture._
      fixture.cancel()
      expectCancel()
    }

    "propagate complete" in test(op) { fixture ⇒
      import fixture._
      onComplete()
      expectNext("")
      expectComplete()
    }

    "propagate error" in test(op) { fixture ⇒
      import fixture._
      onError(TestException)
      expectError(TestException)
    }

    "fold upstream elements with the user function" in test(op) { fixture ⇒
      import fixture._
      requestMore(1)
      expectRequestMore(1)
      onNext('A')
      expectRequestMore(1)
      onNext('B')
      expectRequestMore(1)
      onNext('C')
      expectRequestMore(1)
      onComplete()
      expectNext("ABC")
      expectComplete()
    }

    "when the user function throws an error: propagate as onError and cancel upstream" in
      test(Operation.Fold[Char, String]("", (_, _) ⇒ throw TestException)) { fixture ⇒
        import fixture._
        requestMore(1)
        expectRequestMore(1)
        onNext('A')
        expectError(TestException)
        expectCancel()
      }
  }
}
