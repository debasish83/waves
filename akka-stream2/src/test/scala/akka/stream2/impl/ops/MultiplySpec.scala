package akka.stream2.impl.ops

import akka.stream2.Operation

class MultiplySpec extends OperationImplSpec with MultiplyTests {

  "`Multiply` should" - {
    multiply5Tests(Operation.Multiply[Char](5))
  }
}

// modelled via a trait so we can reuse the tests for the ProcessSpec
trait MultiplyTests { this: OperationImplSpec ⇒

  def multiply5Tests(op: Operation[Char, Char]) = {

    "reduce a requestMore(5) to requestMore(1) and gather up subsequent requestMore counts" in test(op) { fixture ⇒
      import fixture._
      requestMore(5)
      expectRequestMore(1)
      requestMore(2)
      requestMore(3)
      expectNoRequestMore()
    }

    "correctly multiply an upstream element if the request counts are smaller than the multiply factor" in test(op) { fixture ⇒
      import fixture._
      requestMore(2)
      expectRequestMore(1)
      onNext('A')
      expectNext('A', 'A')
      expectNoNext()
      requestMore(1)
      expectNext('A')
      expectNoNext()
      requestMore(3)
      expectNext('A', 'A')
      expectNoNext()
      expectRequestMore(1)
    }

    "correctly multiply an upstream element if the request counts are greater than the multiply factor" in test(op) { fixture ⇒
      import fixture._
      requestMore(8)
      expectRequestMore(1)
      onNext('A')
      expectNext('A', 'A', 'A', 'A', 'A')
      expectNoNext()
      expectRequestMore(1)
      onNext('B')
      expectNext('B', 'B', 'B')
      expectNoNext()
      requestMore(6)
      expectNext('B', 'B')
      expectRequestMore(1)
      onNext('C')
      expectNext('C', 'C', 'C', 'C')
    }

    "continue to produce until the end of the current series if the upstream is completed" in test(op) { fixture ⇒
      import fixture._
      requestMore(3)
      expectRequestMore(1)
      onNext('A')
      expectNext('A', 'A', 'A')
      onComplete()
      requestMore(1)
      expectNext('A')
      requestMore(3)
      expectNext('A')
      expectComplete()
    }

    "immediately propagate upstream error" in test(op) { fixture ⇒
      import fixture._
      requestMore(3)
      expectRequestMore(1)
      onNext('A')
      expectNext('A', 'A', 'A')
      onError(TestException)
      expectError(TestException)
    }

    "propagate cancel" in test(op) { fixture ⇒
      import fixture._
      fixture.cancel()
      expectCancel()
    }

    "propagate an empty stream" in test(op) { fixture ⇒
      import fixture._
      onComplete()
      expectComplete()
    }
  }
}
