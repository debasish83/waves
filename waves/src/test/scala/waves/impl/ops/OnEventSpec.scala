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

import waves.Operation
import Operation.StreamEvent
import StreamEvent._

class OnEventSpec extends OperationImplSpec {

  "`OnEvent`" should {

    "propagate requestMore and notify the callback" in onEventTest(RequestMore(42)) { fixture ⇒
      import fixture._
      requestMore(42)
      expectRequestMore(42)
    }

    "propagate cancel and notify the callback" in onEventTest(Cancel) { fixture ⇒
      import fixture._
      fixture.cancel()
      expectCancel()
    }

    "propagate onNext and notify the callback" in onEventTest(OnNext('x')) { fixture ⇒
      import fixture._
      onNext('x')
      expectNext('x')
    }

    "propagate complete and notify the callback" in onEventTest(OnComplete) { fixture ⇒
      import fixture._
      onComplete()
      expectComplete()
    }

    "propagate onError and notify the callback" in onEventTest(OnError(TestException)) { fixture ⇒
      import fixture._
      onError(TestException)
      expectError(TestException)
    }
  }

  def onEventTest(expected: StreamEvent[Char])(body: Fixture[Char, Char] ⇒ Unit) = {
    var callbackCall: StreamEvent[Any] = null
    test(Operation.OnEvent[Char](callbackCall = _)) { fixture ⇒
      body(fixture)
      callbackCall shouldEqual expected
    }
  }
}
