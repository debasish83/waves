package akka.stream2.impl.ops

import akka.stream2.Operation

class DropSpec extends OperationImplSpec {

  val op = Operation.Drop[Symbol](3)

  "`Drop` should" - {
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
