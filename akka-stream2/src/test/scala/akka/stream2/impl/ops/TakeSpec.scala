package akka.stream2.impl.ops

import akka.stream2.Operation

class TakeSpec extends OperationImplSpec {

  val op = Operation.Take[Char](3)

  "`Take` should" - {
    "propagate requestMore directly if requested < n" in new Test(op) {
      requestMore(2)
      expectRequestMore(2)
    }

    "propagate requestMore directly if requested == n" in new Test(op) {
      requestMore(3)
      expectRequestMore(3)
    }

    "requestMore(n) from upstream the requested count from downstream is > n" in new Test(op) {
      requestMore(4)
      expectRequestMore(3)
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

    "propagate the first n elements from upstream, then complete downstream and cancel upstream" in new Test(op) {
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