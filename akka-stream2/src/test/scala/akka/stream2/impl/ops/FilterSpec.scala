package akka.stream2.impl.ops

import akka.stream2.Operation

class FilterSpec extends OperationImplSpec {

  val op = Operation.Filter[Char](Character.isLowerCase)

  "`Filter` should" - {

    "propagate requestMore" in test(op) { fixture ⇒
      import fixture._
      requestMore(5)
      expectRequestMore(5)
      requestMore(2)
      expectRequestMore(2)
    }

    "propagate elements that satisfy the filter condition" in test(op) { fixture ⇒
      import fixture._
      requestMore(3)
      expectRequestMore(3)
      onNext('a', 'b')
      expectNext('a', 'b')
      onNext('c')
      expectNext('c')
    }

    "drop elements that don't satisfy the filter condition and requestMore(1) for them" in test(op) { fixture ⇒
      import fixture._
      requestMore(3)
      expectRequestMore(3)
      onNext('a', 'B')
      expectNext('a')
      expectRequestMore(1)
      onNext('c', 'd')
      expectNext('c', 'd')
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

    "when the user function throws an error: propagate as onError and cancel upstream" in
      test(Operation.Filter[Char](_ ⇒ throw TestException)) { fixture ⇒
        import fixture._
        requestMore(1)
        expectRequestMore(1)
        onNext('A')
        expectError(TestException)
        expectCancel()
      }
  }
}
