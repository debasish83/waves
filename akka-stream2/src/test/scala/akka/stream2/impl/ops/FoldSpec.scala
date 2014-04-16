package akka.stream2.impl.ops

import akka.stream2.Operation

class FoldSpec extends OperationImplSpec {

  val op = Operation.Fold[Char, String]("", _ + _)

  "`Fold` should" - {

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
      expectNext("")
      expectComplete()
    }

    "propagate error" in new Test(op) {
      onError(TestException)
      expectError(TestException)
    }

    "fold upstream elements with the user function" in new Test(op) {
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
      new Test(Operation.Fold[Char, String]("", (_, _) ⇒ throw TestException)) {
        requestMore(1)
        expectRequestMore(1)
        onNext('A')
        expectError(TestException)
        expectCancel()
      }
  }
}
