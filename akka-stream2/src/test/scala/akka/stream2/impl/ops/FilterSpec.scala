package akka.stream2.impl.ops

import akka.stream2.Operation

class FilterSpec extends OperationImplSpec {

  val op = Operation.Filter[Char](Character.isLowerCase)

  "`Filter` should" - {

    "propagate requestMore" in new Test(op) {
      requestMore(5)
      expectRequestMore(5)
      requestMore(2)
      expectRequestMore(2)
    }

    "propagate elements that satisfy the filter condition" in new Test(op) {
      requestMore(3)
      expectRequestMore(3)
      onNext('a', 'b')
      expectNext('a', 'b')
      onNext('c')
      expectNext('c')
    }

    "drop elements that don't satisfy the filter condition and requestMore(1) for them" in new Test(op) {
      requestMore(3)
      expectRequestMore(3)
      onNext('a', 'B')
      expectNext('a')
      expectRequestMore(1)
      onNext('c', 'd')
      expectNext('c', 'd')
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

    "when the user function throws an error: propagate as onError and cancel upstream" in
      new Test(Operation.Filter[Char](_ ⇒ throw TestException)) {
        requestMore(1)
        expectRequestMore(1)
        onNext('A')
        expectError(TestException)
        expectCancel()
      }
  }
}
