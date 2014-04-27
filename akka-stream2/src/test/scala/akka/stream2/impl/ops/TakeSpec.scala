package akka.stream2.impl.ops

import akka.stream2.Operation

class TakeSpec extends OperationImplSpec {

  val op = Operation.Take[Char](3)

  "`Take` should" - {
    "propagate requestMore directly if requested < n" in test(op) { fixture ⇒
      import fixture._
      requestMore(2)
      expectRequestMore(2)
    }

    "propagate requestMore directly if requested == n" in test(op) { fixture ⇒
      import fixture._
      requestMore(3)
      expectRequestMore(3)
    }

    "requestMore(n) from upstream the requested count from downstream is > n" in test(op) { fixture ⇒
      import fixture._
      requestMore(4)
      expectRequestMore(3)
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

    "propagate the first n elements from upstream, then complete downstream and cancel upstream" in test(op) { fixture ⇒
      import fixture._
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