package akka.stream2.impl
package ops

import akka.stream2.Operation
import Operation.StreamEvent
import StreamEvent._

class OnEventSpec extends OperationImplSpec {

  "`OnEvent` should" - {

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
