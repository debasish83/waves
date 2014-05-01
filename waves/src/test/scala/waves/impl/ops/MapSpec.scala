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

class MapSpec extends OperationImplSpec {

  val op = Operation.Map[Int, Int](_ * 2)

  "`Map`" should {
    "propagate requestMore" in test(op) { fixture ⇒
      import fixture._
      requestMore(5)
      expectRequestMore(5)
      requestMore(2)
      expectRequestMore(2)
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

    "map the `onNext` element with the user function" in test(op) { fixture ⇒
      import fixture._
      onNext(3)
      expectNext(6)
      onNext(-42)
      expectNext(-84)
    }

    "when user function throws an error: propagate as onError and cancel upstream" in
      test(Operation.Map[Int, Int](_ ⇒ throw TestException)) { fixture ⇒
        import fixture._
        onNext(3)
        expectError(TestException)
        expectCancel()
      }
  }
}
