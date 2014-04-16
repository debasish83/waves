package akka.stream2.impl.ops

import akka.stream2.Operation

class DropSpec extends OperationImplSpec {

  val op = Operation.Drop[Symbol](3)

  "`Drop` should" - {
    "add drop-count to very first requestMore from downstream" in new Test(op) {
      requestMore(5)
      expectRequestMore(8)
      requestMore(3)
      expectRequestMore(3)
    }

    "drop the first drop-count elements from upstream" in new Test(op) {
      requestMore(2)
      expectRequestMore(5)
      onNext('A, 'B, 'C, 'D, 'E)
      expectNext('D, 'E)
      requestMore(1)
      expectRequestMore(1)
      onNext('F)
      expectNext('F)
    }

    "propagate cancel" in new Test(op) {
      cancel()
      expectCancel()
    }

    "propagate complete" in new Test(op) {
      onComplete()
      expectComplete()
    }

    "propagate error" in new Test(op) {
      onError(TestException)
      expectError(TestException)
    }
  }
}
