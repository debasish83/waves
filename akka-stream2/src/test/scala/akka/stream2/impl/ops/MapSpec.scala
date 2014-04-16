package akka.stream2.impl.ops

import akka.stream2.Operation

class MapSpec extends OperationImplSpec {

  val op = Operation.Map[Int, Int](_ * 2)

  "`Map` should" - {
    "propagate requestMore" in new Test(op) {
      requestMore(5)
      expectRequestMore(5)
      requestMore(2)
      expectRequestMore(2)
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

    "map the `onNext` element with the user function" in new Test(op) {
      onNext(3)
      expectNext(6)
      onNext(-42)
      expectNext(-84)
    }

    "when user function throws an error: propagate as onError and cancel upstream" in
      new Test(Operation.Map[Int, Int](_ ⇒ throw TestException)) {
        onNext(3)
        expectError(TestException)
        expectCancel()
      }
  }
}
